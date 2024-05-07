#pragma once
#include <Core/Types.h>
#include <map>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Save snapshot metadata of MergeTree
struct MergeTreeSnapshotMetadata
{
    Int64 snapshot_block_num;
    std::map<String, Int64> partition_snapshots;

    MergeTreeSnapshotMetadata() = default;
    explicit MergeTreeSnapshotMetadata(Int64 snapshot_block_num_)
        : snapshot_block_num(snapshot_block_num_)
    {}

    static std::unique_ptr<MergeTreeSnapshotMetadata> parse(const String & s);

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

    Int64 getSnapshotBlockNumByPartitionId(const String & partition_id) const;

    String toString() const;
};

using MergeTreeSnapshotMetadataPtr = std::shared_ptr<const MergeTreeSnapshotMetadata>;

}
