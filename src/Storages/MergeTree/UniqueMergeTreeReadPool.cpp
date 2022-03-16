#include <Storages/MergeTree/UniqueMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Common/formatReadable.h>
#include <ext/range.h>


namespace DB
{
UniqueMergeTreeReadPool::UniqueMergeTreeReadPool(
    const std::map<String, PartBitmap::Ptr> &part_bitmaps_,
    const size_t threads_,
    const size_t sum_marks_,
    const size_t min_marks_for_concurrent_read_,
    RangesInDataParts parts_,
    const MergeTreeData & data_,
    const StorageMetadataPtr & metadata_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const bool check_columns_,
    const Names & column_names_,
    const BackoffSettings & backoff_settings_,
    size_t preferred_block_size_bytes_,
    const bool do_not_steal_tasks_) :
        MergeTreeReadPool(threads_, sum_marks_, min_marks_for_concurrent_read_, parts_, data_, metadata_snapshot_,
            prewhere_info_, check_columns_, column_names_, backoff_settings_, preferred_block_size_bytes_,
            do_not_steal_tasks_),
        part_bitmaps(part_bitmaps_)
{
    assert(part_bitmaps.size()>0);
}

}
