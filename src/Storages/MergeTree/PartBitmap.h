#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>

namespace DB
{

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

    explicit PartBitmap(MergeTreeData::DataPartPtr data_part_,
                        UInt32 seq_id_,
                        UInt32 update_seq_id_)
        : PartBitmap(data_part_, std::make_shared<RBitmap>(), seq_id_, update_seq_id_)
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


}
