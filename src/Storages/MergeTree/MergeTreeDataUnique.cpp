#include <Storages/MergeTree/MergeTreeDataUnique.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

const NameAndTypePair UniqueKeyIdDescription::FILTER_COLUMN {"_unique_key_id", std::make_shared<DataTypeUInt32>()};


void MergeTreeUniquePartition::initRocksDB()
{
    String rocksdb_path;
    for (const auto & disk : data.getDisks())
    {
        auto path = fs::path(data.getRelativeDataPath()) / "rocksdb" / partition_id;
        if (disk->exists(path))
        {
            rocksdb_path = path;
            rocksdb_disk = disk;
            break;
        }
    }

    if (rocksdb_path.empty())
    {
        rocksdb_path = fs::path(data.getRelativeDataPath()) / "rocksdb" / partition_id;
        rocksdb_disk = data.getStoragePolicy()->getAnyDisk();
        rocksdb_disk->createDirectories(rocksdb_path);
    }

    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.compression = rocksdb::CompressionType::kNoCompression;

    rocksdb::Status status = rocksdb::DB::Open(options, fs::path(rocksdb_disk->getPath()) / rocksdb_path, &db);
    if (status != rocksdb::Status::OK())
        throw Exception(ErrorCodes::ROCKSDB_ERROR,
                        "Fail to open rocksdb path at: {} status:{}. .", rocksdb_path, status.ToString());

    rocksdb = std::unique_ptr<rocksdb::DB>(db);

    std::vector<Slice> keys {"_last_unique_seq_id", "_last_bitmap_seq_id"};
    std::vector<String> values;
    auto res = rocksdb->MultiGet(rocksdb::ReadOptions(), keys, &values);
    if (res[0].IsNotFound() || res[1].IsNotFound())
    {
        recoverRocksDB();
    }
    else if (res[0].ok() && res[1].ok())
    {
        last_unique_seq_id = *(reinterpret_cast<const UInt32 *>(values[0].c_str()));
        last_bitmap_seq_id = *(reinterpret_cast<const UInt32 *>(values[1].c_str()));
    }
    else
        throw Exception(ErrorCodes::ROCKSDB_ERROR,
                        "Failed to get seq id from rocksdb {} status: {}.", rocksdb_path, res[0].ToString());
}

void MergeTreeUniquePartition::recoverRocksDB()
{
    auto data_parts = data.getVisibleDataPartsVectorInPartition(data.getContext(), partition_id);
    if (data_parts.empty())
        return;

    auto metadata_snapshot = data.getInMemoryMetadataPtr();

    Names column_names;
    Names unique_names = metadata_snapshot->getUniqueKeyColumns();
    column_names.insert(column_names.end(), unique_names.begin(), unique_names.end());
    String version_name = data.merging_params.version_column;
    if (!version_name.empty())
        column_names.push_back(version_name);

    column_names.push_back(UniqueKeyIdDescription::FILTER_COLUMN.name);

    std::shared_ptr<std::atomic<size_t>> input_rows_filtered{std::make_shared<std::atomic<size_t>>(0)};
    Pipes pipes;
    ReadSettings read_settings;
    for (auto & data_part : data_parts)
    {
        PartBitmap::Ptr unique_bitmap = getPartBitmap(data_part);
        last_bitmap_seq_id = std::max(last_bitmap_seq_id, unique_bitmap->seq_id);

        Pipe pipe = createMergeTreeSequentialSource(
            data,
            std::make_shared<StorageSnapshot>(data, metadata_snapshot),
            data_part,
            column_names,
            false,
            true,
            false,
            {},
            unique_bitmap);

        pipes.emplace_back(std::move(pipe));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    QueryPipeline query_pipeline(std::move(pipe));
    PullingPipelineExecutor executor(query_pipeline);

    struct UniqueVersion
    {
        UInt32 unique_id;
        Int64 version;
    };

    Block block;
    Batch batch;
    while(executor.pull(block))
    {
        size_t rows = block.rows();
        const auto & unique_id_column = typeid_cast<const ColumnUInt32 &>(
                                            *block.getByName(UniqueKeyIdDescription::FILTER_COLUMN.name).column)
                                            .getData();
        for (size_t idx = 0; idx < rows; ++idx)
        {
            String key;
            for (const auto & unique_name : unique_names)
            {
                auto & column = block.getByName(unique_name).column;
                key.append(column->getDataAt(idx).data, column->getDataAt(idx).size);
            }

            UniqueVersion uv;
            uv.unique_id = unique_id_column[idx];
            if (!version_name.empty())
                uv.version = block.getByName(version_name).column->getInt(idx);
            else
                uv.version = time(nullptr);

            batch.Put(key,
                      {reinterpret_cast<char *>(&(uv.unique_id)), sizeof(uv.unique_id) + sizeof(uv.version)});

            last_unique_seq_id = std::max(last_unique_seq_id, uv.unique_id);
        }

        rocksdb->Write(rocksdb::WriteOptions(), &batch);
        batch.Clear();
    }

    batch.Put("_last_unique_seq_id",
              {reinterpret_cast<char *>(&last_unique_seq_id), sizeof(last_unique_seq_id)});
    batch.Put("_last_bitmap_seq_id",
              {reinterpret_cast<char *>(&last_bitmap_seq_id), sizeof(last_bitmap_seq_id)});
    rocksdb->Write(rocksdb::WriteOptions(), &batch);
}

void MergeTreeUniquePartition::writeUniqueColumn(MergeTreeMutableDataPartPtr data_part,
                                                 ColumnRawPtrs unique_columns,
                                                 ColumnPtr version_column,
                                                 MutableColumnPtr & unique_id_column)
{
    size_t rows = version_column->size();
    size_t unique_columns_size = unique_columns.size();
    auto & unique_id_data = typeid_cast<ColumnUInt32 &>(*unique_id_column).getData();
    unique_id_data.resize(rows);

    std::vector<Slice> keys;
    keys.reserve(rows);
    std::vector<String> keys_string;
    keys_string.resize(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        std::vector<Slice> key;
        for (size_t j = 0; j < unique_columns_size; ++j)
            key.emplace_back(unique_columns[j]->getDataAt(i).data, unique_columns[j]->getDataAt(i).size);

        keys.emplace_back(rocksdb::SliceParts(key.data(), static_cast<int>(unique_columns_size)),
                          &keys_string[i]);
    }

    std::vector<String> values;
    std::vector<Status> status = rocksdb->MultiGet(rocksdb::ReadOptions(), keys, &values);

    struct UpdateKey
    {
        UInt32 unique_id;
        Int64 version;
        size_t index;
    };
    std::unordered_map<String, UpdateKey> update_keys;

    for (size_t i = 0; i < rows; ++i)
    {
        Int64 version = version_column->getInt(i);
        unique_id_data[i] = 0;
        auto it = update_keys.find(keys_string[i]);
        if (it == update_keys.end())
        {
            if (status[i].IsNotFound())
            {
                UInt32 unique_id = getAndAddUniqueId();
                update_keys[keys_string[i]] = {unique_id, version, i};
                unique_id_data[i] = unique_id;
            }
            else if (status[i].ok())
            {
                char * c = values[i].data();
                Int64 ver = *(reinterpret_cast<Int64 *>(c + sizeof(UInt64)));
                if (version >= ver)
                {
                    UInt32 unique_id = *(reinterpret_cast<UInt32 *>(c));
                    update_keys[keys_string[i]] = {unique_id, version, i};
                    unique_id_data[i] = unique_id;
                }
            }
            else
                throw Exception(ErrorCodes::ROCKSDB_ERROR,
                                "Failed to get value from rocksdb status: {}.", status[i].ToString());
        }
        else
        {
            auto & update_key = it->second;
            if (version >= update_key.version)
            {
                unique_id_data[update_key.index] = 0;
                unique_id_data[i] = update_key.unique_id;
                update_key.version = version;
                update_key.index = i;
            }
        }
    }

    PartBitmap::MutablePtr part_bitmap = PartBitmap::create(data_part, getAndAddBitmapId());
    Batch batch;
    batch.Put("_last_unique_seq_id",
              {reinterpret_cast<char *>(&last_unique_seq_id), sizeof(last_unique_seq_id)});
    batch.Put("_last_bitmap_seq_id",
              {reinterpret_cast<char *>(&last_bitmap_seq_id), sizeof(last_bitmap_seq_id)});

    for (auto it : update_keys)
    {
        auto & v = it.second;
        batch.Put(it.first,
                  {reinterpret_cast<char *>(&(v.unique_id)), sizeof(v.unique_id) + sizeof(v.version)});
        part_bitmap->add(v.unique_id);
    }

    rocksdb->Write(rocksdb::WriteOptions(), &batch);
    part_bitmap->write();
}


void MergeTreeUniquePartition::update(MergeTreeMutableDataPartPtr data_part,
                                      Block & block,
                                      const StorageMetadataPtr & metadata_snapshot)
{
    std::lock_guard lock (write_mutex);
    if (!rocksdb)
    {
        initRocksDB();
    }

    Names unique_keys = metadata_snapshot->getUniqueKeyColumns();
    ColumnRawPtrs unique_columns;
    for(const auto & name : unique_keys)
        unique_columns.emplace_back(block.getByName(name).column.get());

    ColumnPtr version_column;
    if (!data.merging_params.version_column.empty())
        version_column = block.getByName(data.merging_params.version_column).column;
    else
    {
        auto col = ColumnInt64::create();
        ColumnInt64::Container & version_data = typeid_cast<ColumnInt64 &>(*col).getData();
        version_data.resize_fill(block.rows(), time(nullptr));
        version_column = std::move(col);
    }

    /// add unique_key_id
    ColumnWithTypeAndName & unique_id_column = block.getByName(UniqueKeyIdDescription::FILTER_COLUMN.name);
    auto column = unique_id_column.type->createColumn();
    writeUniqueColumn(data_part, unique_columns, version_column, column);
    unique_id_column.column = std::move(column);
    block.insert(unique_id_column);
}

PartBitmap::Ptr & MergeTreeUniquePartition::getPartBitmap(MergeTreeData::DataPartPtr data_part)
{
    auto iter = part_bitmaps_cache.find(data_part->name);
    if (iter == part_bitmaps_cache.end())
    {
        PartBitmap::MutablePtr part_bitmap = PartBitmap::create(data_part);
        part_bitmap->read();
        return part_bitmaps_cache.insert(std::make_pair(data_part->name, std::move(part_bitmap))).first->second;
    }

    return iter->second;
}

void MergeTreeUniquePartition::mergePartBitmaps(MergeTreeData::DataPartsVector & parts)
{
    if (parts.empty())
        return;

    size_t base_update_seq_id  = getPartBitmap(parts[0])->update_seq_id;
    bool merged = true;
    for (const auto & part : parts)
    {
        if (base_update_seq_id != getPartBitmap(part)->update_seq_id)
        {
            merged = false;
            break;
        }
    }

    if (merged)
        return;

    std::sort(parts.begin(), parts.end(), [this](MergeTreeData::DataPartPtr & l_part, MergeTreeData::DataPartPtr r_part)
              { return getPartBitmap(l_part)->seq_id < getPartBitmap(r_part)->seq_id; });

    RBitmapPtr merged_bitmap = std::make_shared<RBitmap>();
    bool update_all = false;
    for (size_t i = parts.size() - 1; i != 0; --i)
    {
        PartBitmap::Ptr current_part_bitmap = getPartBitmap(parts[i]);
        PartBitmap::Ptr & next_part_bitmap = getPartBitmap(parts[i - 1]);
        merged_bitmap->rb_or(*current_part_bitmap->bitmap);

        if (update_all || current_part_bitmap->update_seq_id > next_part_bitmap->update_seq_id)
        {
            LOG_DEBUG(&Poco::Logger::root(),
                      "update delayed part bitmap part: {} seq: {} update_seq: {}, new_part :{} seq: {}, update_seq: {}",
                      next_part_bitmap->data_part->name, next_part_bitmap->seq_id,
                      next_part_bitmap->update_seq_id, next_part_bitmap->data_part->name, current_part_bitmap->seq_id,
                      current_part_bitmap->update_seq_id);

            PartBitmap::MutablePtr new_part_bitmap = PartBitmap::mutate(std::move(next_part_bitmap));
            new_part_bitmap->bitmap->rb_andnot(*merged_bitmap);
            new_part_bitmap->update_seq_id = current_part_bitmap->update_seq_id;
            new_part_bitmap->write();
            next_part_bitmap = std::move(new_part_bitmap);
            update_all = true;
        }
        else {
            LOG_DEBUG(&Poco::Logger::root(), "updated part bitmap part: {} seq: {} update_seq: {}",
                      current_part_bitmap->data_part->name, current_part_bitmap->seq_id, current_part_bitmap->update_seq_id);
        }
    }
}

void MergeTreeDataUnique::update(MergeTreeMutableDataPartPtr data_part,
                                 Block & block,
                                 const StorageMetadataPtr & metadata_snapshot)
{
    if (block.rows() == 0)
        return;

    auto unique_partition = getUniquePartition(data_part);

    unique_partition->update(data_part, block, metadata_snapshot);
}

PartBitmap::Ptr MergeTreeDataUnique::getPartBitmap(MergeTreeData::DataPartPtr data_part)
{
    auto unique_partition = getUniquePartition(data_part);
    return unique_partition->getPartBitmap(data_part);
}

MergeTreeUniquePartitionPtr MergeTreeDataUnique::getUniquePartition(MergeTreeData::DataPartPtr data_part)
{
    MergeTreeUniquePartitionPtr unique_partition;
    {
        std::lock_guard lock(write_mutex);

        String partition_id = data_part->partition.getID(data);
        auto it = unique_partitions.find(partition_id);
        if (it == unique_partitions.end())
            unique_partition = unique_partitions.emplace(
                                                    partition_id,
                                                    std::make_shared<MergeTreeUniquePartition>(data, partition_id)).first->second;
        else
            unique_partition = it->second;
    }
    return unique_partition;
}

void MergeTreeDataUnique::mergePartitionPartBitmaps(MergeTreeData::DataPartsVector & partition_parts,
                                                    MergeTreeData::DataPartsVector & select_parts,
                                                    PartBitmapsVector & bitmaps)
{
    if (partition_parts.empty() || select_parts.empty())
        return;

    auto unique_partition = getUniquePartition(partition_parts[0]);
    unique_partition->mergePartBitmaps(partition_parts);

    for (const auto & part : select_parts)
        bitmaps.emplace_back(unique_partition->getPartBitmap(part));
}

}
