#pragma once

#include <Storages/MergeTree/PartBitmap.h>

#if USE_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#endif

namespace DB
{

class MergeTreeDataUniquer;

static const auto LAST_UNIQUE_SEQ_ID = "_last_unique_key_id";
static const auto LAST_BITMAP_SEQ_ID = "_last_update_seq_id";

struct UniqueKeyIdDescription
{
    static const NameAndTypePair FILTER_COLUMN;
};

struct PartitionUniquerGuard : private boost::noncopyable
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/**
 * Holds rocksdb and bitmap required for partition deduplication.
 */
class MergeTreePartitionUniquer
{
#if USE_ROCKSDB
    friend class MergeTreeDataUniquer;
public:
    MergeTreePartitionUniquer(MergeTreeData & data_, Poco::Logger * log_, const String & partition_id_)
        : data(data_)
        , log(log_)
        , partition_id(partition_id_)
        , last_access_time(time(nullptr))
    {}

    ~MergeTreePartitionUniquer();

    int getAndAddUniqueId(const PartitionUniquerGuard::Lock &) { return unique_key_seq_id++; }
    int64_t getAndAddBitmapId(const PartitionUniquerGuard::Lock &) { return part_bitmap_seq_id++; }

    int getUniqueId() const { return unique_key_seq_id; }
    int64_t getBitmapId() const { return part_bitmap_seq_id; }

    /// Get the bitmap from cache when querying, and load it from the disk if it does not exist.
    PartBitmap::Ptr getPartBitmap(const MergeTreeDataPartPtr & data_part);
    PartBitmapEntryPtr getPartBitmapEntry(const MergeTreeDataPartPtr & data_part) { return part_bitmap_cache.getPartBitmapEntry(data_part); }

    /// Only remove bitmap from cache.
    void removePartBitmap(const MergeTreeDataPartPtr & data_part);

    /// Merge the bitmaps of the specified parts.
    /// It should be noted that the passed parts must be in the same partition. This will not be checked here.
    void mergeBitmaps();
    PartBitmaps mergeBitmaps(MergeTreeDataPartsVector & select_parts);

    /// Initialize rocksdb, load the latest unique id and bitmap id, and only load once after multiple calls.
    /// Initialize after acquiring the write lock when writing data.
    void initializedRocksdb(const PartitionUniquerGuard::Lock &);
    bool isInitializedRocksDB() { return rocksdb != nullptr; }

    std::unique_ptr<rocksdb::DB> & getRocksDB(const PartitionUniquerGuard::Lock &);
    void removeRocksDB();

    PartitionUniquerGuard::Lock lockPartition();

    void updateLastAccessTime() { last_access_time = time(nullptr); }
    time_t getLastAccessTime() const { return last_access_time; }
    time_t getLastAccessRocksDBTime() const { return last_access_rocksdb_time; }

private:
    /// Check whether rocksdb saves the latest key by bitmap seq id.
    void checkAndRecoverRocksDB(bool force);

    /// Read unique key column from part to generate keys to restore rocksdb
    void recoverRocksDB(MergeTreeDataPartsVector & parts);

    /// Recovering rocksdb from log files `key_id_log.bin` is compatible with rolling upgrades of old versions.
    /// New versions will gradually phase out this method.
    void recoverRocksDBFromLogFile(MergeTreeDataPartsVector & parts);

    void closeRocksDB();

    void mergeBitmapsImpl(const MergeTreeDataPartsVector & parts);

    void setUniqueId(int unique_key_seq_id_) { unique_key_seq_id = unique_key_seq_id_; }
    void setBitmapId(int64_t part_bitmap_seq_id_) { part_bitmap_seq_id = part_bitmap_seq_id_; }

    PartitionUniquerGuard::Lock lockMergeBitmap();

    //// Marked as removed, operating rocksdb will throw an exception.
    void markRemoveStatus() { removed = true; }

    MergeTreeData & data;
    Poco::Logger * log;

    String partition_id;

    DiskPtr rocksdb_disk;
    std::unique_ptr<rocksdb::DB> rocksdb;

    /// Assign monotonically increasing ids
    std::atomic<int> unique_key_seq_id{0};
    std::atomic<int64_t> part_bitmap_seq_id{0};

    /// Part bitmap cache
    PartBitmapCache part_bitmap_cache;

    /// Only one thread can complete bitmap merging at the same time.
    PartitionUniquerGuard merge_guard;

    /// Write lock, only one thread is allowed to write data to the partition at the same time
    PartitionUniquerGuard write_guard;

    /// Whether the tag has been removed.
    std::atomic<bool> removed{false};

    time_t last_access_time;
    time_t last_access_rocksdb_time;
#endif
};

using MergeTreePartitionUniquerPtr = std::shared_ptr<MergeTreePartitionUniquer>;

}
