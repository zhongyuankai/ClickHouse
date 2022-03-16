#pragma once

#include <Core/Block.h>
#include <Core/Row.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Columns/ColumnsNumber.h>

#include <Interpreters/sortBlock.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/StorageReplicatedUniqueMergeTree.h>

namespace DB
{

/** Writes new parts of data to the merge tree.
  */
class UniqueMergeTreeDataWriter : public MergeTreeDataWriter
{
public:
    UniqueMergeTreeDataWriter(StorageReplicatedUniqueMergeTree & data_) : MergeTreeDataWriter(data_), storage_data(data_) {}

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      */
    MergeTreeData::MutableDataPartPtr writeTempPart(BlockWithPartition & block,
        const StorageMetadataPtr & metadata_snapshot) override;

private:
    StorageReplicatedUniqueMergeTree &storage_data;
};

}
