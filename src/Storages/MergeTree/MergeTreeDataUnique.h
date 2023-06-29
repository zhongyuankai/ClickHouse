#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmap.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>

namespace DB
{

//class MergeTreeData;
//using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;



class PartBitmap
{
public:
    explicit PartBitmap(MergeTreeData::DataPartPtr data_part_, UInt32 seq_id_)
        : data_part(data_part_)
        , seq_id(seq_id_)
    {}

    void add(UInt32 unique_id)
    {
        std::cout << "add " << std::endl;
        if (!bitmap)
        {
            std::cout << "init " << std::endl;
            bitmap = std::make_shared<RBitmap>();
        }

        std::cout << "end " << std::endl;
        bitmap->add(unique_id);
    }

    void write();

private:
    using RBitmap = RoaringBitmapWithSmallSet<UInt32, 255>;
    using RBitmapPtr = std::shared_ptr<RBitmap>;

    MergeTreeData::DataPartPtr data_part;

    UInt32 seq_id;
    UInt32 update_seq_id;
    RBitmapPtr bitmap;
};



using PartBitmapPtr = std::shared_ptr<PartBitmap>;
using PartBitmaps = std::unordered_map<String, PartBitmapPtr>;

class MergeTreeUniquePartition
{
public:
    MergeTreeUniquePartition(MergeTreeData & data_, String partition_id_)
        : data(data_)
        , partition_id(partition_id_)
    {}

    void update(MergeTreeMutableDataPartPtr data_part, Block & block, const StorageMetadataPtr & metadata_snapshot);

private:
    using Slice = rocksdb::Slice;
    using Batch = rocksdb::WriteBatch;
    using Status = rocksdb::Status;

    void initRocksDB();

    void writeUniqueColumn(MergeTreeMutableDataPartPtr data_part, ColumnRawPtrs unique_columns, ColumnPtr version_column, MutableColumnPtr & unique_id_column);

    UInt32 getAndAddUniqueId()
    {
        return last_unique_seq_id++;
    }

    UInt32 getAndAddBitmapId()
    {
        return last_bitmap_seq_id++;
    }

    MergeTreeData & data;
    String partition_id;

    DiskPtr rocksdb_disk;
    std::unique_ptr<rocksdb::DB> rocksdb;

    UInt32 last_unique_seq_id = 1;
    UInt32 last_bitmap_seq_id = 1;
    /// part bitmap cache
    PartBitmaps part_bitmaps;
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

    void update(MergeTreeMutableDataPartPtr data_part, Block & block, const StorageMetadataPtr & metadata_snapshot);

private:
    MergeTreeData & data;
    Poco::Logger * log;

    MergeTreeUniquePartitions unique_partitions;
    std::mutex write_mutex;
};

using MergeTreeDataUniquePtr = std::shared_ptr<MergeTreeDataUnique>;


}
