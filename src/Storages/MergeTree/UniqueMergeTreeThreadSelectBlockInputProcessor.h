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
class UniqueMergeTreeReadPool;


class UniqueMergeTreeThreadSelectBlockInputProcessor : public MergeTreeThreadSelectBlockInputProcessor
{
public:
    UniqueMergeTreeThreadSelectBlockInputProcessor(
        const size_t thread_,
        const std::shared_ptr<UniqueMergeTreeReadPool> & pool_,
        const size_t min_marks_to_read_,
        const UInt64 max_block_size_,
        size_t preferred_block_size_bytes_,
        size_t preferred_max_column_in_block_size_bytes_,
        const MergeTreeData & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const bool use_uncompressed_cache_,
        const PrewhereInfoPtr & prewhere_info_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & virt_column_names_);


    ~UniqueMergeTreeThreadSelectBlockInputProcessor() override = default;

    String getName() const override { return "UniqueMergeTreeThread"; }

private:
    const std::shared_ptr<UniqueMergeTreeReadPool> reader_pool;

    void initializeRangeReaders(MergeTreeReadTask & current_task) override;



};

}
