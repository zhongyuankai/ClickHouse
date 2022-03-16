#include <Storages/MergeTree/UniqueMergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/UniqueMergeTreeRangeReader.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

UniqueMergeTreeSelectProcessor::UniqueMergeTreeSelectProcessor(
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeData::DataPartPtr & owned_data_part_,
    UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    Names required_columns_,
    MarkRanges mark_ranges_,
    bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    bool check_columns_,
    const MergeTreeReaderSettings & reader_settings_,
    const PartBitmap::Ptr &part_bitmap_,
    const Names & virt_column_names_,
    size_t part_index_in_query_,
    bool quiet):
        MergeTreeSelectProcessor(storage_, metadata_snapshot_, owned_data_part_, max_block_size_rows_, preferred_block_size_bytes_,
            preferred_max_column_in_block_size_bytes_, required_columns_, mark_ranges_, use_uncompressed_cache_, prewhere_info_,
            check_columns_, reader_settings_, virt_column_names_, part_index_in_query_, quiet),
        part_bitmap(part_bitmap_)
{
    assert(part_bitmap);
}

void UniqueMergeTreeSelectProcessor::initializeRangeReaders(MergeTreeReadTask & current_task)
{
    if (prewhere_info)
    {
        if (reader->getColumns().empty())
        {
            current_task.range_reader = std::make_unique<UniqueMergeTreeRangeReader>(pre_reader.get(),
               nullptr, prewhere_info, true, part_bitmap);
        }
        else
        {
            MergeTreeRangeReader * pre_reader_ptr = nullptr;
            if (pre_reader != nullptr)
            {
                current_task.pre_range_reader = std::make_unique<MergeTreeRangeReader>(pre_reader.get(),
                   nullptr, prewhere_info, false);
                pre_reader_ptr = current_task.pre_range_reader.get();
            }

            current_task.range_reader = std::make_unique<UniqueMergeTreeRangeReader>(reader.get(),
                pre_reader_ptr, nullptr, true, part_bitmap);
        }
    }
    else
    {
        current_task.range_reader = std::make_unique<UniqueMergeTreeRangeReader>(reader.get(), nullptr, nullptr, true, part_bitmap);
    }
}


}

