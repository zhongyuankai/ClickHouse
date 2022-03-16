#pragma once

#include <Core/QueryProcessingStage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageReplicatedUniqueMergeTree.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>


namespace DB
{

/** Executes SELECT queries on data from the merge tree.
  */
class UniqueMergeTreeDataSelectExecutor : public MergeTreeDataSelectExecutor
{
public:
    explicit UniqueMergeTreeDataSelectExecutor(StorageReplicatedUniqueMergeTree &data_);

private:
    StorageReplicatedUniqueMergeTree &storage_data;

    Pipe readFromParts(
        MergeTreeData::DataPartsVector parts,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        UInt64 max_block_size,
        unsigned num_streams,
        const PartitionIdToMaxBlock * max_block_numbers_to_read = nullptr) const override;

    Pipe spreadMarkRangesAmongStreams(
        RangesInDataParts && parts,
        size_t num_streams,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const SelectQueryInfo & query_info,
        const Names & virt_columns,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        const std::map<String, PartBitmap::Ptr> &part_bitmaps) const;

    Pipe spreadMarkRangesAmongStreamsWithOrder(
        RangesInDataParts && parts,
        size_t num_streams,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        UInt64 max_block_size,
        bool use_uncompressed_cache,
        const SelectQueryInfo & query_info,
        const ExpressionActionsPtr & sorting_key_prefix_expr,
        const Names & virt_columns,
        const Settings & settings,
        const MergeTreeReaderSettings & reader_settings,
        ExpressionActionsPtr & out_projection,
        const std::map<String, PartBitmap::Ptr> &part_bitmaps) const;

};

}
