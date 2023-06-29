#pragma once

#include <Storages/MergeTree/PartBitmap.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>

namespace DB
{

struct UniqueKeyIdDescription
{
    static const NameAndTypePair FILTER_COLUMN;
};


class MergeTreeUniquePartition
{
public:
    MergeTreeUniquePartition(MergeTreeData & data_, String partition_id_)
        : data(data_)
        , partition_id(partition_id_)
    {}

    void update(MergeTreeMutableDataPartPtr data_part,
                Block & block,
                const StorageMetadataPtr & metadata_snapshot);

    PartBitmap::Ptr & getPartBitmap(MergeTreeData::DataPartPtr data_part);

    void mergePartBitmaps(MergeTreeData::DataPartsVector & parts);

private:
    using Slice = rocksdb::Slice;
    using Batch = rocksdb::WriteBatch;
    using Status = rocksdb::Status;

    void initRocksDB();
    void recoverRocksDB();

    void writeUniqueColumn(MergeTreeMutableDataPartPtr data_part,
                           ColumnRawPtrs unique_columns,
                           ColumnPtr version_column,
                           MutableColumnPtr & unique_id_column);

    UInt32 getAndAddUniqueId()
    {
        return ++last_unique_seq_id;
    }

    UInt32 getAndAddBitmapId()
    {
        return ++last_bitmap_seq_id;
    }


    MergeTreeData & data;
    String partition_id;

    DiskPtr rocksdb_disk;
    std::unique_ptr<rocksdb::DB> rocksdb;

    UInt32 last_unique_seq_id = 0;
    UInt32 last_bitmap_seq_id = 0;
    /// part bitmap cache
    PartBitmaps part_bitmaps_cache;
    std::mutex part_bitmaps_mutex;

    std::mutex write_mutex;
};

using MergeTreeUniquePartitionPtr = std::shared_ptr<MergeTreeUniquePartition>;
using MergeTreeUniquePartitions = std::unordered_map<String, MergeTreeUniquePartitionPtr>;


class MergeTreeDataUnique
{
public:
    MergeTreeDataUnique(MergeTreeData & data_)
        : data(data_)
        , log(&Poco::Logger::get(data.getLogName() + " (Unique)"))
    {}

    void update(MergeTreeMutableDataPartPtr data_part,
                Block & block,
                const StorageMetadataPtr & metadata_snapshot);

    PartBitmap::Ptr getPartBitmap(MergeTreeData::DataPartPtr data_part);

    MergeTreeUniquePartitionPtr getUniquePartition(MergeTreeData::DataPartPtr data_part);

    void mergePartitionPartBitmaps(MergeTreeData::DataPartsVector & partition_parts,
                                   MergeTreeData::DataPartsVector & select_parts,
                                   PartBitmapsVector & bitmaps);

private:
    MergeTreeData & data;
    Poco::Logger * log;

    MergeTreeUniquePartitions unique_partitions;
    std::mutex write_mutex;
};

using MergeTreeDataUniquePtr = std::shared_ptr<MergeTreeDataUnique>;


}
