#include <Storages/MergeTree/MergeTreeDataUnique.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

void PartBitmap::write()
{
    String path = data_part->getDataPartStoragePtr()->getFullPath();
    String tmp_path = path + "bitmap.bin.tmp";
    {
        WriteBufferFromFile out (tmp_path);
        bitmap->write(out);
        writeVarUInt(seq_id, out);
        writeVarUInt(update_seq_id, out);
    }
    fs::rename(tmp_path, path + "bitmap.bin");
}

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

    struct UniqueVersion
    {
        UInt32 unique_id;
        Int64 version;
    };

    std::vector<Slice> keys {"_last_unique_seq_id", "_last_bitmap_seq_id"};
    std::vector<String> values;
    auto res = rocksdb->MultiGet(rocksdb::ReadOptions(), keys, &values);
    if (res[0].IsNotFound() || res[1].IsNotFound())
    {
        /// log recover
        auto data_parts = data.getVisibleDataPartsVectorInPartition(data.getContext(), partition_id);
        if (data_parts.empty())
        {
            last_unique_seq_id = 1;
            last_bitmap_seq_id = 1;
        }
        else
        {
            Batch batch;
            for (const auto & data_part : data_parts)
            {
                auto in = data_part->getDataPartStoragePtr()->readFile("unique_key.log", {}, {}, {});
                readVarUInt(last_unique_seq_id, *in);
                readVarUInt(last_bitmap_seq_id, *in);
                UniqueVersion uv;
                while(!in->eof())
                {
                    String key;
                    readString(key, *in);
                    readVarUInt(uv.unique_id, *in);
                    readVarInt(uv.version, *in);
                    batch.Put(key, {reinterpret_cast<char *>(&(uv.unique_id)), sizeof(uv.unique_id) + sizeof(uv.version)});
                }
                rocksdb->Write(rocksdb::WriteOptions(), &batch);
            }
        }
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

void MergeTreeUniquePartition::writeUniqueColumn(MergeTreeMutableDataPartPtr data_part, ColumnRawPtrs unique_columns, ColumnPtr version_column, MutableColumnPtr & unique_id_column)
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

    PartBitmap bitmap {data_part, getAndAddBitmapId()};
    Batch batch;
    batch.Put("_last_unique_seq_id",
              {reinterpret_cast<char *>(&last_unique_seq_id), sizeof(last_unique_seq_id)});
    batch.Put("_last_bitmap_seq_id",
              {reinterpret_cast<char *>(&last_bitmap_seq_id), sizeof(last_bitmap_seq_id)});

    auto out = data_part->getDataPartStorage().writeFile("unique_key.log", DBMS_DEFAULT_BUFFER_SIZE, {});
    writeVarUInt(last_unique_seq_id, *out);
    writeVarUInt(last_bitmap_seq_id, *out);
    for (auto it : update_keys)
    {
        auto & v = it.second;
        batch.Put(it.first,
                  {reinterpret_cast<char *>(&(v.unique_id)), sizeof(v.unique_id) + sizeof(v.version)});
        bitmap.add(v.unique_id);

        /// write key
        writeString(it.first, *out);
        /// write value
        out->write(reinterpret_cast<char *>(&(v.unique_id)), sizeof(v.unique_id) + sizeof(v.version));
        writeVarUInt(v.unique_id, *out);
        writeVarInt(v.version, *out);
    }

    rocksdb->Write(rocksdb::WriteOptions(), &batch);
    bitmap.write();
}


void MergeTreeUniquePartition::update(MergeTreeMutableDataPartPtr data_part, Block & block, const StorageMetadataPtr & metadata_snapshot)
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
    ColumnWithTypeAndName & unique_id_column = block.getByName("_unique_key_id");
    auto column = unique_id_column.type->createColumn();
    writeUniqueColumn(data_part, unique_columns, version_column, column);
    unique_id_column.column = std::move(column);
    block.insert(unique_id_column);
}

void MergeTreeDataUnique::update(MergeTreeMutableDataPartPtr data_part, Block & block, const StorageMetadataPtr & metadata_snapshot)
{
    if (block.rows() == 0)
        return;

    MergeTreeUniquePartitionPtr unique_partition;
    {
        std::lock_guard lock(write_mutex);

        String partition_id = data_part->partition.getID(metadata_snapshot->getPartitionKey().sample_block);
        auto it = unique_partitions.find(partition_id);
        if (it == unique_partitions.end())
            unique_partition
                = unique_partitions.emplace(partition_id, std::make_shared<MergeTreeUniquePartition>(data, partition_id)).first->second;
        else
            unique_partition = it->second;
    }

    unique_partition->update(data_part, block, metadata_snapshot);
}


}
