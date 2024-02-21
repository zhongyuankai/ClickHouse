#pragma once

#include <Storages/MergeTree/MergeTreePartitionUniquer.h>

namespace DB
{

using MergeTreePartitionUniquers = std::unordered_map<String, MergeTreePartitionUniquerPtr>;

/**
 * When writing new parts to merge tree, it is marked and deduplicated by bitmap.
 * Only supports intra-partition deduplication.
 */
class MergeTreeDataUniquer
{
#if USE_ROCKSDB
public:
    /// Record the unique key information of valid rows.
    struct UniqueKeyInfo
    {
        int unique_id;
        int version;
        /// Row index in the current block
        size_t row_index;
    };

    MergeTreeDataUniquer(MergeTreeData & data_)
        : data(data_)
        , log(&Poco::Logger::get(data.getLogName() + " (Uniquer)"))
        , last_clear_time(time(nullptr))
    {}

    /// Make blocks unique when writing.
    void uniqueBlock(Block & block, MergeTreeMutableDataPartPtr & data_part);

    /// /// Merge the bitmaps of the specified partitions and return the bitmaps of the given parts.
    void mergePartitionBitmaps(const String & partition_id);
    PartBitmaps mergePartitionBitmaps(const String & partition_id, MergeTreeDataPartsVector & select_parts);

    /// Get Bitmap index from file or cache.
    PartBitmap::Ptr getPartBitmap(const MergeTreeDataPartPtr & data_part);

    /// Only removes the Part bitmap from cache.
    void removePartBitmaps(const MergeTreeDataPartsVector & parts);

    /// Reading the bitmap.bin file requires locking because bitmap is always being merged.
    PartBitmapGuard::Lock lockPartBitmap(const MergeTreeDataPartPtr & data_part);

    /// Clean up rocksdb that has not been used for a long time.
    void clearPartitionUniquer();
    void dropPartitionUniquer(const String & partition_id);
    void dropAllPartitionUniquer();

    /// Clean up the unloaded rocksdb data directory from the file system.
    void clearRocksDBFromFileSystem(const String & partition_id);

private:
    /// It will be created if it does not exist by default.
    /// If `not_exist_create` is false and does not exist, return nullptr.
    MergeTreePartitionUniquerPtr getPartitionUniquer(const String & partition_id, bool not_exist_create = true);

    MergeTreeData & data;
    Poco::Logger * log;

    MergeTreePartitionUniquers partition_uniquers;
    std::mutex uniquer_mutex;

    time_t last_clear_time;
#endif
};

using MergeTreeDataUniquerPtr = std::shared_ptr<MergeTreeDataUniquer>;

}
