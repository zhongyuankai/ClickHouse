#pragma once
#include <Core/Block.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/StorageReplicatedUniqueMergeTree.h>

namespace DB
{

class UniqueMergeTreeRangeReader : public MergeTreeRangeReader
{
public:
    UniqueMergeTreeRangeReader(
        IMergeTreeReader * merge_tree_reader_,
        MergeTreeRangeReader * prev_reader_,
        const PrewhereInfoPtr & prewhere_,
        bool last_reader_in_chain_,
        const PartBitmap::Ptr &part_bitmap
        );

    UniqueMergeTreeRangeReader() = default;

    void executePrewhereActionsAndFilterColumns(ReadResult & result) override;

private:
    PartBitmap::Ptr part_bitmap;

};

}
