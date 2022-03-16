#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/UniqueMergeTreeRangeReader.h>
#include <Storages/StorageReplicatedUniqueMergeTree.h>
#include <ext/range.h>

namespace DB
{
UniqueMergeTreeRangeReader::UniqueMergeTreeRangeReader(
    IMergeTreeReader * merge_tree_reader_,
    MergeTreeRangeReader * prev_reader_,
    const PrewhereInfoPtr & prewhere_,
    bool last_reader_in_chain_,
    const PartBitmap::Ptr &part_bitmap_)
    : MergeTreeRangeReader(merge_tree_reader_, prev_reader_, prewhere_, last_reader_in_chain_),part_bitmap(part_bitmap_)
{
    assert(part_bitmap);
}

void UniqueMergeTreeRangeReader::executePrewhereActionsAndFilterColumns(ReadResult & result)
{
    if (!prewhere)
    {
        result.clearFilter();
        MutableColumnPtr filter_column = DataTypeUInt8().createColumn();
        ColumnUInt8::Container & filter_column_data = typeid_cast<ColumnUInt8 &>(*filter_column).getData();
        filter_column_data.resize_fill(result.columns[0]->size(), 0);

        {
            int position = sample_block.getPositionByName("_unique_key_id");
            const ColumnUInt32::Container & ukey_id_column = typeid_cast<const ColumnUInt32 &>(*result.columns[position]).getData();
            for (size_t i = 0; i < result.columns[0]->size(); i++)
            {
                uint32_t key_id = ukey_id_column[i];
                if (key_id != 0xffffffff && part_bitmap->bitmap->rb_contains(key_id))
                    filter_column_data[i] = 1;
                /*
                //for testing
                {
                    filter_column_data[i] = 1;
                    if (!bitmap_holder.bitmap->rb_contains(ukey_id_column[i]))
                    {
                        const unsigned int * v = &ukey_id_column[i];
                        unsigned int * vv = const_cast<unsigned int *>(v);
                        *vv = 0xffffffff;
                    }
                }
                */
            }
        }

        result.setFilter(ColumnPtr(std::move(filter_column)));
        const auto * result_filter = result.getFilter();
        filterColumns(result.columns, result_filter->getData());
        result.num_rows = result.columns.size() == 0 ? 0 : result.columns[0]->size();
        return;
    }


}

}
