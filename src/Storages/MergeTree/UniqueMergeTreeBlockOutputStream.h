#pragma once

#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Columns/ColumnArray.h>


namespace DB
{

/** To write one part.
  * The data refers to one partition, and is written in one part.
  */
class UniqueMergeTreeBlockOutputStream final : public MergedBlockOutputStream
{
public:
    UniqueMergeTreeBlockOutputStream(
        const MergeTreeDataPartPtr & data_part,
        const StorageMetadataPtr & metadata_snapshot_,
        const NamesAndTypesList & columns_list_,
        const MergeTreeIndices & skip_indices,
        CompressionCodecPtr default_codec,
        bool blocks_are_granules_size = false);

    /// If the data is pre-sorted.
    void write(const Block & block) override;

private:
    void writeImpl(const Block & block, const IColumn::Permutation * permutation);

};

}
