#include "config.h"

#include <Storages/MergeTree/MergeTreePartitionUniquer.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Compression/CompressedReadBuffer.h>

namespace ProfileEvents
{
extern const Event PartitionUniquerWriteLockMicroseconds;
extern const Event PartitionUniquerMergeLockMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
    extern const int FILE_DOESNT_EXIST;
}

const NameAndTypePair UniqueKeyIdDescription::FILTER_COLUMN{"_unique_key_id", std::make_shared<DataTypeUInt32>()};

#if USE_ROCKSDB

MergeTreePartitionUniquer::~MergeTreePartitionUniquer()
{
    closeRocksDB();
}

void MergeTreePartitionUniquer::closeRocksDB()
{
    auto lock = lockPartition();
    try
    {
        if (rocksdb)
        {
            LOG_INFO(log, "Close partition {} rocksdb.", partition_id);
            rocksdb->Close();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    rocksdb = nullptr;
    rocksdb_disk = nullptr;
}

void MergeTreePartitionUniquer::removeRocksDB()
{
    auto lock = lockPartition();
    try
    {
        if (rocksdb)
            rocksdb->Close();

        String rocksdb_dir = data.getRelativeDataPath() + partition_id + "_unique_key";
        if (rocksdb_disk && rocksdb_disk->exists(rocksdb_dir))
        {
            rocksdb_disk->removeRecursive(rocksdb_dir);
            LOG_INFO(log, "Remove partition rocksdb from file system {}{}.", rocksdb_disk->getPath(), rocksdb_dir);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    rocksdb = nullptr;
    rocksdb_disk = nullptr;
}

void MergeTreePartitionUniquer::initializedRocksdb(const PartitionUniquerGuard::Lock &)
{
    if (removed)
        throw Exception(ErrorCodes::ROCKSDB_ERROR,
                        "Unable to initialize rocksdb, {} partition uniquer has been removed.", partition_id);

    bool force = false;
    if (rocksdb == nullptr)
    {
        String rocksdb_path = data.getRelativeDataPath() + partition_id + "_unique_key";
        for (const auto & disk : data.getDisks())
        {
            if (disk->isBroken())
                continue;

            if (disk->exists(rocksdb_path))
            {
                rocksdb_disk = disk;
                break;
            }
        }

        if (!rocksdb_disk)
        {
            rocksdb_disk = data.getStoragePolicy()->getAnyDisk();
            rocksdb_disk->createDirectories(rocksdb_path);
            LOG_INFO(log, "Create the rocksdb directory of partition {}.", partition_id);
        }

        LOG_INFO(log, "Initialized partition rocksdb from file system {}{}.", rocksdb_disk->getPath(), rocksdb_path);

        rocksdb::Options options;
        rocksdb::DB * db;
        options.create_if_missing = true;
        options.compression = rocksdb::CompressionType::kNoCompression;

        rocksdb::Status status = rocksdb::DB::Open(options, fs::path(rocksdb_disk->getPath()) / rocksdb_path, &db);
        if (status != rocksdb::Status::OK())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to open rocksdb path at: {} status:{}. .", rocksdb_path, status.ToString());

        rocksdb = std::unique_ptr<rocksdb::DB>(db);

        /// Try to load unique seq id and bitmap seq id from rocksdb.
        std::vector<rocksdb::Slice> keys{LAST_UNIQUE_SEQ_ID, LAST_BITMAP_SEQ_ID};
        std::vector<String> values;

        auto res = rocksdb->MultiGet(rocksdb::ReadOptions(), keys, &values);
        if (res[0].IsNotFound() || res[1].IsNotFound())
        {
            setUniqueId(0);
            setBitmapId(0);
            force = true;
            LOG_INFO(log, "Forced recovery of rocksdb in partition {}.", partition_id);
        }
        else if (res[0].ok() && res[1].ok())
        {
            int64_t unique_id = *(reinterpret_cast<const int64_t *>(values[0].c_str()));
            setUniqueId(static_cast<int>(unique_id));
            setBitmapId(*(reinterpret_cast<const int64_t *>(values[1].c_str())));
            LOG_INFO(log, "The unique seq id {} and bitmap seq id {} of partition {} were initialized successfully.",
                     getUniqueId(), getBitmapId(), partition_id);
        }
        else
            throw Exception(ErrorCodes::ROCKSDB_ERROR,
                            "Failed to get seq id from rocksdb {} status: {}.", rocksdb_path, res[0].ToString());
    }

    checkAndRecoverRocksDB(force);
}

void MergeTreePartitionUniquer::checkAndRecoverRocksDB(bool force)
{
    auto data_parts = data.getVisibleDataPartsVectorInPartition(data.getContext(), partition_id);
    if (data_parts.empty())
        return;

    MergeTreeDataPartsVector missing_parts;
    if (!force)
    {
        /// Only the missing parts in rocksdb.
        int64_t max_bitmap_id = getBitmapId();
        for (const auto & part : data_parts)
        {
            if (part->isEmpty())
                continue;

            PartBitmap::Ptr part_bitmap = getPartBitmap(part);
            if (max_bitmap_id <= part_bitmap->getSeqId())
                missing_parts.push_back(part);
        }

        if (missing_parts.empty())
            return;
    }
    else
        missing_parts = data_parts;

    /// Restoring rocksdb requires reading unique columns,
    /// and the data that has been marked for deletion needs to be filtered out in advance.
    {
        auto lock = lockMergeBitmap();
        mergeBitmapsImpl(data_parts);
    }

    if (!data.getSettings()->read_unique_id_log)
        recoverRocksDB(missing_parts);
    else
        recoverRocksDBFromLogFile(missing_parts);
}

void MergeTreePartitionUniquer::recoverRocksDB(MergeTreeDataPartsVector & parts)
{
    /// Collect the columns that need to be loaded.
    /// unique columns、version column、_unique_key_id.
    Names columns_to_read;
    Names unique_names = data.merging_params.unique_columns;
    size_t unique_columns_size = unique_names.size();

    columns_to_read.insert(columns_to_read.end(), unique_names.begin(), unique_names.end());
    String version_name = data.merging_params.version_column;
    if (!version_name.empty())
        columns_to_read.push_back(version_name);

    columns_to_read.push_back(UniqueKeyIdDescription::FILTER_COLUMN.name);

    UInt32 max_unique_seq_id = static_cast<UInt32>(getUniqueId());
    int64_t max_bitmap_seq_id = getBitmapId();

    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    /// Create Pipeline to read Part's data.
    Pipes pipes;
    for (auto & part : parts)
    {
        if (part->isEmpty())
            continue;

        PartBitmap::Ptr unique_bitmap = getPartBitmap(part);
        max_bitmap_seq_id = std::max(max_bitmap_seq_id, unique_bitmap->getSeqId());

        Pipe pipe = createMergeTreeSequentialSource(
            data, std::make_shared<StorageSnapshot>(data, metadata_snapshot), part, columns_to_read, false, true, false, {}, unique_bitmap);

        pipes.emplace_back(std::move(pipe));
        LOG_INFO(log, "Read part {} to restore rocksdb of partition {}, part bitmap seq id {} and update seq id {}.",
                 part->name, partition_id, unique_bitmap->getSeqId(), unique_bitmap->getUpdateSeqId());
    }

    if (pipes.empty())
        return;

    auto pipe = Pipe::unitePipes(std::move(pipes));
    QueryPipeline query_pipeline(std::move(pipe));
    PullingPipelineExecutor executor(query_pipeline);

    UInt32 current_time = static_cast<UInt32>(time(nullptr));
    Block block;
    while (executor.pull(block))
    {
        size_t rows = block.rows();
        if (!rows)
            continue;

        rocksdb::WriteBatch batch;

        ColumnRawPtrs unique_columns;
        for (auto & name : unique_names)
            unique_columns.emplace_back(block.getByName(name).column.get());

        ColumnPtr version_column = nullptr;
        if (!version_name.empty())
            version_column = block.getByName(version_name).column;

        auto & unique_id_column = block.getByName(UniqueKeyIdDescription::FILTER_COLUMN.name).column;
        const auto & unique_id_data = typeid_cast<const ColumnUInt32 &>(*unique_id_column).getData();

        for (size_t idx = 0; idx < rows; ++idx)
        {
            std::vector<rocksdb::Slice> keys;
            for (size_t j = 0; j < unique_columns_size; ++j)
                keys.emplace_back(unique_columns[j]->getDataAt(idx).data, unique_columns[j]->getDataAt(idx).size);

            String key;
            rocksdb::Slice slice(rocksdb::SliceParts(keys.data(), static_cast<int>(unique_columns_size)), &key);

            UInt32 vv[2];
            vv[0] = unique_id_data[idx];
            if (version_column)
                vv[1] = static_cast<UInt32>(version_column->getUInt(idx));
            else
                vv[1] = current_time;

            batch.Put(key, {reinterpret_cast<char *>(vv), sizeof(vv)});

            max_unique_seq_id = std::max(max_unique_seq_id, vv[0]);
        }

        /// Rocksdb stores unassigned IDs, so you need to add 1 here.
        UInt32 unique_seq_id = max_unique_seq_id + 1;
        UInt64 bitmap_seq_id = max_bitmap_seq_id + 1;
        batch.Put("_last_unique_seq_id", {reinterpret_cast<char *>(&unique_seq_id), sizeof(unique_seq_id)});
        batch.Put("_last_bitmap_seq_id", {reinterpret_cast<char *>(&bitmap_seq_id), sizeof(bitmap_seq_id)});

        rocksdb::Status write_status = rocksdb->Write(rocksdb::WriteOptions(), &batch);
        if (!write_status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to write a batch from rocksdb status: {}.", write_status.ToString());
    }

    setUniqueId(max_unique_seq_id + 1);
    setBitmapId(max_bitmap_seq_id + 1);

    LOG_INFO(log, "Reading parts recovery partition {} rocksdb successfully, unique seq id {} and bitmap seq id {}.",
             partition_id, getUniqueId(), getBitmapId());
}

void MergeTreePartitionUniquer::recoverRocksDBFromLogFile(MergeTreeDataPartsVector & parts)
{
    if (parts.empty())
        return;

    size_t max_batch_bytes = DEFAULT_INSERT_BLOCK_SIZE * 256;
    for (const auto & part : parts)
    {
        DataPartStoragePtr data_part_storage = part->getDataPartStoragePtr();
        std::unique_ptr<ReadBuffer> reader;
        if (data_part_storage->exists(IMergeTreeDataPart::KEY_ID_LOG_BIN_FILE_NAME))
        {
            reader = data_part_storage->readFile(IMergeTreeDataPart::KEY_ID_LOG_BIN_FILE_NAME, {}, std::nullopt, std::nullopt);
        }
        else if (data_part_storage->exists(IMergeTreeDataPart::KEY_ID_LOG_COMPRESS_BIN_FILE_NAME))
        {
            std::unique_ptr<ReadBufferFromFileBase> buf = data_part_storage->readFile(IMergeTreeDataPart::KEY_ID_LOG_COMPRESS_BIN_FILE_NAME, {}, std::nullopt, std::nullopt);
            reader = std::make_unique<CompressedReadBufferFromFile>(std::move(buf));
        }
        else
        {
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                            "Part {} does not exist file `key_id_log.bin` and file `key_id_log.bin.tar`.", partition_id);
        }
        LOG_INFO(log, "Read part {} to restore rocksdb of partition {}.", part->name, partition_id);

        rocksdb::WriteBatch batch;
        size_t current_batch_bytes = 0;
        while (!reader->eof())
        {
            int total_entries = -1;
            reader->readStrict(reinterpret_cast<char *>(&total_entries), sizeof(int));
            for (int i = 0; i < total_entries; i++)
            {
                String key;
                int32_t len = -1;
                reader->readStrict(reinterpret_cast<char *>(&len), sizeof(int32_t));
                key.resize(len);
                reader->readStrict(key.data(), len);

                String value;
                len = sizeof(UInt32) * 2;
                value.resize(len);
                reader->readStrict(value.data(), len);

                current_batch_bytes += key.length();
                current_batch_bytes += value.length();
                batch.Put(rocksdb::Slice(key.c_str(), key.length()), rocksdb::Slice(value.c_str(), value.length()));
                if (current_batch_bytes >= max_batch_bytes)
                {
                    rocksdb::Status status = rocksdb->Write(rocksdb::WriteOptions(), &batch);
                    if (!status.ok())
                        throw Exception(
                            ErrorCodes::ROCKSDB_ERROR,
                            "Fail to write rocksdb of partition: {}, rocksdb: {}",
                            partition_id,
                            status.ToString());

                    LOG_DEBUG(log, "Write a batch of data to partition rocksdb {}, count: {}.", partition_id, batch.Count());
                    batch.Clear();
                    current_batch_bytes = 0;
                }
            }
        }

        if (current_batch_bytes == 0)
            return;

        rocksdb::Status status = rocksdb->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok())
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR, "Fail to write rocksdb of partition {}, rocksdb: {}", partition_id, status.ToString());
    }

    /// Try to load unique seq id and bitmap seq id from rocksdb.
    std::vector<rocksdb::Slice> keys{LAST_UNIQUE_SEQ_ID, LAST_BITMAP_SEQ_ID};
    std::vector<String> values;

    auto res = rocksdb->MultiGet(rocksdb::ReadOptions(), keys, &values);
    if (res[0].ok() && res[1].ok())
    {
        int64_t unique_id = *(reinterpret_cast<const int64_t *>(values[0].c_str()));
        setUniqueId(static_cast<int>(unique_id));
        setBitmapId(*(reinterpret_cast<const int64_t *>(values[1].c_str())));
        LOG_INFO(log, "Recovering unique seq id {} and bitmap seq id {} from rocksdb of partition {} successfully.",
                 getUniqueId(), getBitmapId(), partition_id);
    }
    else
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to get seq id from partition {} rocksdb status: {}.", partition_id, res[0].ToString());
}

PartBitmap::Ptr MergeTreePartitionUniquer::getPartBitmap(const MergeTreeDataPartPtr & data_part)
{
    return part_bitmap_cache.getPartBitmap(data_part);
}

void MergeTreePartitionUniquer::removePartBitmap(const MergeTreeDataPartPtr & data_part)
{
    part_bitmap_cache.removePartBitmap(data_part);
}

void MergeTreePartitionUniquer::mergeBitmapsImpl(const MergeTreeDataPartsVector & parts)
{
    std::vector<std::tuple<MergeTreeDataPartPtr, PartBitmap::Ptr>> part_bitmaps;
    /// The bitmaps that have been merged have the same update seq id.
    int64_t prev_update_seq_id = INT64_MAX;
    bool merged = true;
    for (const auto & part : parts)
    {
        if (part->isEmpty())
            continue;

        auto bitmap = getPartBitmap(part);
        if (prev_update_seq_id != INT64_MAX && prev_update_seq_id != bitmap->getUpdateSeqId())
            merged = false;
        else
            prev_update_seq_id = bitmap->getUpdateSeqId();

        part_bitmaps.emplace_back(part, bitmap);
    }

    if (part_bitmaps.size() < 2 || merged)
        return;

    /// Sort by bitmap seq id.
    std::sort(part_bitmaps.begin(), part_bitmaps.end(),
              [](std::tuple<MergeTreeDataPartPtr, PartBitmap::Ptr> & lhs, std::tuple<MergeTreeDataPartPtr, PartBitmap::Ptr> & rhs)
              {
                  return std::get<1>(lhs)->getSeqId() < std::get<1>(rhs)->getSeqId();
              });

    auto merged_bitmap = PartBitmap::create(0, 0);

    bool update_all = false;
    /// Merge bitmap from back to front.
    for (auto it = part_bitmaps.rbegin(); it != part_bitmaps.rend(); ++it)
    {
        auto [part, bitmap] = *it;

        if (it == part_bitmaps.rbegin())
        {
            merged_bitmap->bitmapOR(bitmap->bitmap);
            prev_update_seq_id = bitmap->getUpdateSeqId();
            LOG_TRACE(log, "The latest part bitmap {} (seq={} update_seq={})", part->name, bitmap->getSeqId(), bitmap->getUpdateSeqId());
            continue;
        }

        if (update_all || prev_update_seq_id > bitmap->getUpdateSeqId())
        {
            LOG_TRACE(log, "Part bitmap {} (seq={} update_seq={}) merges with part bitmap update seq id {})",
                      part->name, bitmap->getSeqId(), bitmap->getUpdateSeqId(), prev_update_seq_id);

            auto new_bitmap = PartBitmap::mutate(bitmap);
            new_bitmap->bitmapAndNot(merged_bitmap->bitmap);
            new_bitmap->update_seq_id = prev_update_seq_id;

            merged_bitmap->bitmapOR(new_bitmap->bitmap);
            /// Update bitmap in cache.
            part_bitmap_cache.setPartBitmap(part, std::move(new_bitmap));
            update_all = true;
        }
        else
        {
            merged_bitmap->bitmapOR(bitmap->bitmap);
            LOG_TRACE(log, "Skip merge part bitmap {} (seq={} update_seq={})", part->name, bitmap->getSeqId(), bitmap->getUpdateSeqId());
        }
    }
}

void MergeTreePartitionUniquer::mergeBitmaps()
{
    auto lock = lockMergeBitmap();
    auto parts = data.getVisibleDataPartsVectorInPartition(data.getContext(), partition_id);
    mergeBitmapsImpl(parts);
}

PartBitmaps MergeTreePartitionUniquer::mergeBitmaps(MergeTreeDataPartsVector & select_parts)
{
    auto lock = lockMergeBitmap();

    auto parts = data.getVisibleDataPartsVectorInPartition(data.getContext(), partition_id);
    mergeBitmapsImpl(parts);

    PartBitmaps part_bitmaps;
    for (const auto & part : select_parts)
        part_bitmaps.push_back(getPartBitmap(part));

    return part_bitmaps;
}

PartitionUniquerGuard::Lock MergeTreePartitionUniquer::lockPartition()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionUniquerWriteLockMicroseconds);
    return write_guard.lock();
}

PartitionUniquerGuard::Lock MergeTreePartitionUniquer::lockMergeBitmap()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PartitionUniquerMergeLockMicroseconds);
    return merge_guard.lock();
}

std::unique_ptr<rocksdb::DB> & MergeTreePartitionUniquer::getRocksDB(const PartitionUniquerGuard::Lock &)
{
    if (removed)
        throw Exception(ErrorCodes::ROCKSDB_ERROR,
                        "Unable to get rocksdb, {} partition uniquer has been removed.", partition_id);

    last_access_rocksdb_time = time(nullptr);
    return rocksdb;
}
#endif

}
