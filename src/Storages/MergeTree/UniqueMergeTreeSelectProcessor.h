#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/StorageReplicatedUniqueMergeTree.h>

#include <Processors/Sources/SourceWithProgress.h>

namespace DB
{

class IMergeTreeReader;
class UncompressedCache;
class MarkCache;


class UniqueMergeTreeSelectProcessor : public MergeTreeSelectProcessor
{
public:
    UniqueMergeTreeSelectProcessor(
        const MergeTreeData & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData::DataPartPtr & owned_data_part,
        UInt64 max_block_size_rows,
        size_t preferred_block_size_bytes,
        size_t preferred_max_column_in_block_size_bytes,
        Names required_columns_,
        MarkRanges mark_ranges,
        bool use_uncompressed_cache,
        const PrewhereInfoPtr & prewhere_info,
        bool check_columns,
        const MergeTreeReaderSettings & reader_settings,
        const PartBitmap::Ptr &part_bitmap,
        const Names & virt_column_names = {},
        size_t part_index_in_query = 0,
        bool quiet = false);


    ~UniqueMergeTreeSelectProcessor() override = default;

    String getName() const override { return "UniqueMergeTree"; }

protected:
    void initializeRangeReaders(MergeTreeReadTask & current_task) override;

    PartBitmap::Ptr part_bitmap;

};

}
