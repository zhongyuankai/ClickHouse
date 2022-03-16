#include <Storages/MergeTree/UniqueMergeTreeThreadSelectBlockInputProcessor.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/UniqueMergeTreeReadPool.h>
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

UniqueMergeTreeThreadSelectBlockInputProcessor::UniqueMergeTreeThreadSelectBlockInputProcessor(
    const size_t thread_,
    const UniqueMergeTreeReadPoolPtr & pool_,
    const size_t min_marks_to_read_,
    const UInt64 max_block_size_rows_,
    size_t preferred_block_size_bytes_,
    size_t preferred_max_column_in_block_size_bytes_,
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const bool use_uncompressed_cache_,
    const PrewhereInfoPtr & prewhere_info_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & virt_column_names_):
        MergeTreeThreadSelectBlockInputProcessor(thread_, pool_, min_marks_to_read_, max_block_size_rows_,
            preferred_block_size_bytes_, preferred_max_column_in_block_size_bytes_, storage_, metadata_snapshot_, use_uncompressed_cache_, prewhere_info_,
            reader_settings_, virt_column_names_),
        reader_pool(std::move(pool_))
{
    assert(reader_pool->part_bitmaps.size() > 0);
}

void UniqueMergeTreeThreadSelectBlockInputProcessor::initializeRangeReaders(MergeTreeReadTask & current_task)
{
    const PartBitmap::Ptr &part_bitmap = reader_pool->part_bitmaps.at(current_task.data_part->name);
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
        current_task.range_reader = std::make_unique<UniqueMergeTreeRangeReader>(reader.get(), nullptr, nullptr, true,
            part_bitmap);
    }
}


}

