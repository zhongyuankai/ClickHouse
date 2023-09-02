#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmap.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>

namespace DB
{

//class MergeTreeData;
//using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

struct UniqueKeyIdDescription
{
    static const NameAndTypePair FILTER_COLUMN;
};

using RBitmap = RoaringBitmapWithSmallSet<UInt32, 255>;
using RBitmapPtr = std::shared_ptr<RBitmap>;

class PartBitmap : public COW<PartBitmap>
{
private:
    friend class COW<PartBitmap>;

    PartBitmap() = default;

    explicit PartBitmap(MergeTreeData::DataPartPtr data_part_)
        : PartBitmap(data_part_, 0)
    {}

    explicit PartBitmap(MergeTreeData::DataPartPtr data_part_, UInt32 seq_id_)
        : PartBitmap(data_part_, std::make_shared<RBitmap>(), seq_id_, seq_id_)
    {}

    explicit PartBitmap(MergeTreeData::DataPartPtr data_part_, RBitmapPtr bitmap_, UInt32 seq_id_, UInt32 update_seq_id_)
        : data_part(data_part_)
        , bitmap(bitmap_)
        , seq_id(seq_id_)
        , update_seq_id(update_seq_id_)
    {}


    virtual PartBitmap::MutablePtr clone() const
    {
        RBitmapPtr new_bitmap = std::make_shared<RBitmap>();
        new_bitmap->rb_or(*bitmap);
        return create(data_part, new_bitmap, seq_id, update_seq_id);
    }

public:
    virtual ~PartBitmap() = default;

    void add(UInt32 unique_id)
    {
        bitmap->add(unique_id);
    }

    void write();

    void read();

    String toString() const
    {
        PaddedPODArray<UInt32> res_data;
        bitmap->rb_to_array(res_data);
        std::stringstream ss;
        ss << "seq_id: " << seq_id << ", update_seq_id: " << update_seq_id;
        ss << ", part_name: " << data_part->name << "[";
        for (UInt32 v : res_data) ss << v << ", ";
        ss.peek();
        ss << "]";
        return ss.str();
    }

    MergeTreeData::DataPartPtr data_part;

    RBitmapPtr bitmap;
    UInt32 seq_id;
    UInt32 update_seq_id;
};



using PartBitmaps = std::unordered_map<String, PartBitmap::Ptr>;
using PartBitmapsVector = std::vector<PartBitmap::Ptr>;

class MergeTreeUniquePartition
{
public:
    MergeTreeUniquePartition(MergeTreeData & data_, String partition_id_)
        : data(data_)
        , partition_id(partition_id_)
    {}

    void update(MergeTreeMutableDataPartPtr data_part, Block & block, const StorageMetadataPtr & metadata_snapshot);

    PartBitmap::Ptr & getPartBitmap(MergeTreeData::DataPartPtr data_part);

    void mergePartBitmaps(MergeTreeData::DataPartsVector & parts);

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

    void update(MergeTreeMutableDataPartPtr data_part, Block & block, const StorageMetadataPtr & metadata_snapshot);

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
