#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/PartBitmap.h>
#include <Storages/SelectQueryInfo.h>
#include <mutex>


namespace DB
{

class UniqueMergeTreeReadPool : public MergeTreeReadPool
{
public:
    UniqueMergeTreeReadPool(
        const std::map<String, PartBitmap::Ptr> &part_bitmaps,
        const size_t threads_, const size_t sum_marks_, const size_t min_marks_for_concurrent_read_,
        RangesInDataParts parts_, const MergeTreeData & data_, const StorageMetadataPtr & metadata_snapshot_, const PrewhereInfoPtr & prewhere_info_,
        const bool check_columns_, const Names & column_names_,
        const BackoffSettings & backoff_settings_, size_t preferred_block_size_bytes_,
        const bool do_not_steal_tasks_ = false);

    ~UniqueMergeTreeReadPool() override {}

    const std::map<String, PartBitmap::Ptr> part_bitmaps;
};
using UniqueMergeTreeReadPoolPtr = std::shared_ptr<UniqueMergeTreeReadPool>;

}
