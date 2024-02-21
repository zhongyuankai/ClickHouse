#include "config.h"

#if USE_ROCKSDB
#include <Storages/MergeTree/MergeTreeDataUniquer.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

static const auto UNIQUER_CLEAR_CHECK_INTERVALS = 14400; /// 4 hour.
static const auto PARTITION_UNIQUER_ROCKSDB_DESTRUCTION_TIME = 259200; /// 3 day.


void MergeTreeDataUniquer::uniqueBlock(Block & block, MergeTreeMutableDataPartPtr & data_part)
{
    size_t rows = block.rows();
    if (!rows)
        return;

    ColumnRawPtrs unique_columns;
    for (const auto & name : data.merging_params.unique_columns)
        unique_columns.emplace_back(block.getByName(name).column.get());

    size_t unique_columns_size = unique_columns.size();
    std::vector<rocksdb::Slice> keys;
    std::vector<String> unique_keys;
    keys.reserve(rows);
    unique_keys.resize(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        std::vector<rocksdb::Slice> key;
        for (size_t j = 0; j < unique_columns_size; ++j)
            key.emplace_back(unique_columns[j]->getDataAt(i).data, unique_columns[j]->getDataAt(i).size);

        keys.emplace_back(rocksdb::SliceParts(key.data(), static_cast<int>(unique_columns_size)), &unique_keys[i]);
    }

    const String & partition_id = data_part->info.partition_id;
    MergeTreePartitionUniquerPtr partition_uniquer = getPartitionUniquer(partition_id);

    /// Need to hold a write lock, only one insert is allowed in a partition
    auto lock = partition_uniquer->lockPartition();
    partition_uniquer->initializedRocksdb(lock);

    ColumnPtr version_column;
    if (!data.merging_params.version_column.empty())
        version_column = block.getByName(data.merging_params.version_column).column;
    else
    {
        time_t now = time(nullptr);
        auto col = ColumnUInt32::create();
        ColumnUInt32::Container & version_data = typeid_cast<ColumnUInt32 &>(*col).getData();
        version_data.resize_fill(rows, static_cast<UInt32>(now));
        version_column = std::move(col);
    }

    ColumnWithTypeAndName & unique_id_column = block.getByName(UniqueKeyIdDescription::FILTER_COLUMN.name);
    auto column = unique_id_column.type->createColumn();
    auto & unique_id_data = typeid_cast<ColumnUInt32 &>(*column).getData();
    unique_id_data.resize_fill(block.rows(), -1);
    unique_id_column.column = std::move(column);

    LOG_INFO(log, "Start process part {}, the current unique id {} and bitmap id {}.",
             data_part->name, partition_uniquer->getUniqueId(), partition_uniquer->getBitmapId());

    std::vector<String> values;
    std::vector<rocksdb::Status> status = partition_uniquer->getRocksDB(lock)->MultiGet(rocksdb::ReadOptions(), keys, &values);

    /// Deduplicate the current batch of data based on Unique key and version.
    std::unordered_map<std::string_view, UniqueKeyInfo> unique_key_infos;
    for (size_t i = 0; i < rows; ++i)
    {
        int current_version = static_cast<int>(version_column->getUInt(i));
        std::string_view unique_key(unique_keys[i]);
        auto it = unique_key_infos.find(unique_key);
        if (it == unique_key_infos.end())
        {
            if (status[i].IsNotFound())
            {
                int unique_id = partition_uniquer->getAndAddUniqueId(lock);
                unique_key_infos[unique_key] = {unique_id, current_version, i};
                unique_id_data[i] = unique_id;
            }
            else if (status[i].ok())
            {
                char * v = values[i].data();
                int version = *(reinterpret_cast<int *>(v + sizeof(int)));
                if (current_version >= version)
                {
                    int unique_id = *(reinterpret_cast<int *>(v));
                    unique_key_infos[unique_key] = {unique_id, current_version, i};
                    unique_id_data[i] = unique_id;
                }
            }
            else
                throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to get value from rocksdb status: {}.", status[i].ToString());
        }
        else
        {
            auto & unique_key_info = it->second;
            if (current_version >= unique_key_info.version)
            {
                unique_id_data[unique_key_info.row_index] = -1;
                unique_id_data[i] = unique_key_info.unique_id;
                unique_key_info.version = current_version;
                unique_key_info.row_index = i;
            }
        }
    }

    rocksdb::WriteBatch batch;

    int64_t last_unique_seq_id = partition_uniquer->getUniqueId();
    int64_t last_bitmap_seq_id = partition_uniquer->getBitmapId();
    batch.Put(LAST_UNIQUE_SEQ_ID,
              {reinterpret_cast<char *>(&last_unique_seq_id), sizeof(last_unique_seq_id)});
    batch.Put(LAST_BITMAP_SEQ_ID,
              {reinterpret_cast<char *>(&last_bitmap_seq_id), sizeof(last_bitmap_seq_id)});

    LOG_INFO(log, "After process part {}, the current unique id {} and bitmap id {}.",
             data_part->name, last_unique_seq_id, last_bitmap_seq_id);

    {
        PartBitmap::MutablePtr part_bitmap = PartBitmap::create(partition_uniquer->getAndAddBitmapId(lock));

        /// Compatible with older versions
        MutableDataPartStoragePtr data_part_storage = data_part->getDataPartStoragePtr();
        std::unique_ptr<WriteBufferFromFileBase> unique_log_buf;
        if (data.getSettings()->write_unique_id_log)
        {
            unique_log_buf = data_part_storage->writeFile(IMergeTreeDataPart::KEY_ID_LOG_BIN_FILE_NAME, DBMS_DEFAULT_BUFFER_SIZE, {});

            int32_t total_rows = static_cast<uint32_t>(unique_key_infos.size() + 2);
            writeBinary(total_rows, *unique_log_buf);

            String key = LAST_UNIQUE_SEQ_ID;
            int32_t len = static_cast<int32_t>(key.size());
            unique_log_buf->write(reinterpret_cast<char *>(&len), sizeof(int32_t));
            unique_log_buf->write(key.data(), len);
            unique_log_buf->write(reinterpret_cast<char *>(&last_unique_seq_id), sizeof(int64_t));

            key = LAST_BITMAP_SEQ_ID;
            len = static_cast<int32_t>(key.size());
            unique_log_buf->write(reinterpret_cast<char *>(&len), sizeof(int32_t));
            unique_log_buf->write(key.data(), len);
            unique_log_buf->write(reinterpret_cast<char *>(&last_bitmap_seq_id), sizeof(int64_t));
        }

        for (auto it : unique_key_infos)
        {
            const auto & k = it.first;
            auto & v = it.second;

            batch.Put(k, {reinterpret_cast<char *>(&(v.unique_id)), sizeof(v.unique_id) + sizeof(v.version)});
            part_bitmap->insert(v.unique_id);

            if (unique_log_buf)
            {
                int32_t len = static_cast<int32_t>(k.size());
                unique_log_buf->write(reinterpret_cast<char *>(&len), sizeof(int32_t));
                unique_log_buf->write(k.data(), len);
                unique_log_buf->write(reinterpret_cast<char *>(&(v.unique_id)), sizeof(v.unique_id) + sizeof(v.version));
            }
        }

        part_bitmap->serialize(data_part->getDataPartStoragePtr());
    }

    rocksdb::Status write_status = partition_uniquer->getRocksDB(lock)->Write(rocksdb::WriteOptions(), &batch);
    if (!write_status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to write a batch from rocksdb status: {}.", write_status.ToString());
}

void MergeTreeDataUniquer::mergePartitionBitmaps(const String & partition_id)
{
    auto partition_uniquer = getPartitionUniquer(partition_id);
    partition_uniquer->mergeBitmaps();
}

PartBitmaps MergeTreeDataUniquer::mergePartitionBitmaps(const String & partition_id, MergeTreeDataPartsVector & select_parts)
{
    auto partition_uniquer = getPartitionUniquer(partition_id);
    return partition_uniquer->mergeBitmaps(select_parts);
}

MergeTreePartitionUniquerPtr MergeTreeDataUniquer::getPartitionUniquer(const String & partition_id, bool not_exist_create)
{
    std::lock_guard lock(uniquer_mutex);
    MergeTreePartitionUniquerPtr partition_uniquer;
    auto it = partition_uniquers.find(partition_id);
    if (it == partition_uniquers.end())
    {
        if (!not_exist_create)
            return nullptr;

        partition_uniquer =
            partition_uniquers.emplace(partition_id, std::make_shared<MergeTreePartitionUniquer>(data, log, partition_id)).first->second;
    }
    else
        partition_uniquer = it->second;

    if (!not_exist_create)
        partition_uniquer->updateLastAccessTime();

    return partition_uniquer;
}

PartBitmap::Ptr MergeTreeDataUniquer::getPartBitmap(const MergeTreeDataPartPtr & data_part)
{
    const String & partition_id = data_part->info.partition_id;
    auto partition_uniquer = getPartitionUniquer(partition_id);
    return partition_uniquer->getPartBitmap(data_part);
}

PartBitmapGuard::Lock MergeTreeDataUniquer::lockPartBitmap(const MergeTreeDataPartPtr & data_part)
{
    const String & partition_id = data_part->info.partition_id;
    auto partition_uniquer = getPartitionUniquer(partition_id);
    auto entry = partition_uniquer->getPartBitmapEntry(data_part);
    return entry->lockPartBitmap();
}

void MergeTreeDataUniquer::removePartBitmaps(const MergeTreeDataPartsVector & parts)
{
    for (const auto & part : parts)
    {
        const String & partition_id = part->info.partition_id;
        auto partition_uniquer = getPartitionUniquer(partition_id, false);
        if (partition_uniquer)
            partition_uniquer->removePartBitmap(part);
    }

    clearPartitionUniquer();
}

void MergeTreeDataUniquer::clearPartitionUniquer()
{
    try
    {
        time_t current_time = time(nullptr);
        if (current_time - last_clear_time > UNIQUER_CLEAR_CHECK_INTERVALS)
        {
            std::lock_guard lock(uniquer_mutex);

            /// Handle unloaded partition uniquer.
            if (current_time - last_clear_time > PARTITION_UNIQUER_ROCKSDB_DESTRUCTION_TIME)
            {
                /// Note that getAllPartitionIds will lock all parts.
                auto all_partition_ids = data.getAllPartitionIds();
                for (const auto & partition_id : all_partition_ids)
                {
                    if (!partition_uniquers.contains(partition_id))
                        clearRocksDBFromFileSystem(partition_id);
                }
            }

            for (auto it = partition_uniquers.begin(); it != partition_uniquers.end();)
            {
                auto & partition_uniquer = it->second;
                ///  By default, if it has not been accessed for more than 3 days, rocksdb will be deleted locally.
                if (current_time - partition_uniquer->getLastAccessRocksDBTime() > PARTITION_UNIQUER_ROCKSDB_DESTRUCTION_TIME)
                    partition_uniquer->removeRocksDB();

                if (!partition_uniquer->isInitializedRocksDB() &&
                    current_time - partition_uniquer->getLastAccessTime() > PARTITION_UNIQUER_ROCKSDB_DESTRUCTION_TIME)
                {
                    LOG_INFO(log, "Drop partition uniquer {}.", partition_uniquer->partition_id);
                    partition_uniquer->markRemoveStatus();
                    it = partition_uniquers.erase(it);
                }
                else
                    ++it;
            }
            last_clear_time = current_time;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void MergeTreeDataUniquer::dropPartitionUniquer(const String & partition_id)
{
    std::lock_guard lock(uniquer_mutex);
    auto it = partition_uniquers.find(partition_id);
    if (it != partition_uniquers.end())
    {
        LOG_INFO(log, "Drop partition uniquer {}.", partition_id);
        auto & partition_uniquer = it->second;
        partition_uniquer->removeRocksDB();
        partition_uniquer->markRemoveStatus();
        partition_uniquers.erase(it);
    }
    else
        clearRocksDBFromFileSystem(partition_id);
}

void MergeTreeDataUniquer::dropAllPartitionUniquer()
{
    LOG_INFO(log, "Drop all partition uniquer.");
    std::lock_guard lock(uniquer_mutex);

    for (auto it = partition_uniquers.begin(); it != partition_uniquers.end();)
    {
        auto & partition_uniquer = it->second;
        partition_uniquer->removeRocksDB();
        partition_uniquer->markRemoveStatus();
        it = partition_uniquers.erase(it);
    }
}

void MergeTreeDataUniquer::clearRocksDBFromFileSystem(const String & partition_id)
{
    String rocksdb_path = data.getRelativeDataPath() + partition_id + "_unique_key";
    for (const auto & disk : data.getDisks())
    {
        try
        {
            if (disk->exists(rocksdb_path))
            {
                LOG_INFO(log, "Clean up rocksdb data directory {}{} from the file system.", disk->getPath(), rocksdb_path);
                disk->removeRecursive(rocksdb_path);
                break;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

}
#endif
