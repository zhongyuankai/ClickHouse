#include <Storages/MergeTree/MergeTreeSnapshotMetadata.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

void MergeTreeSnapshotMetadata::write(WriteBuffer & out) const
{
    out << "snapshotBlockNum: " << snapshot_block_num << "\n";

    for (const auto & partition_snapshot : partition_snapshots)
    {
        out << "key: " << partition_snapshot.first << "\n";
        out << "value: " << partition_snapshot.second << "\n";
    }
}

void MergeTreeSnapshotMetadata::read(ReadBuffer & in)
{
    in >> "snapshotBlockNum: " >> snapshot_block_num >> "\n";
    while (!in.eof())
    {
        String key;
        Int64 value;
        in >> "key: " >> key >> "\n";
        in >> "value: " >> value >> "\n";
        partition_snapshots[key] = value;
    }
}

std::unique_ptr<MergeTreeSnapshotMetadata> MergeTreeSnapshotMetadata::parse(const String & s)
{
    auto new_snapshot_metadata = std::make_unique<MergeTreeSnapshotMetadata>(-1);
    ReadBufferFromString buf(s);
    new_snapshot_metadata->read(buf);
    return new_snapshot_metadata;
}

Int64 MergeTreeSnapshotMetadata::getSnapshotBlockNumByPartitionId(const DB::String & partition_id) const
{
    if (partition_id.empty() || partition_snapshots.empty())
        return snapshot_block_num;
    else
    {
        if (auto it = partition_snapshots.find(partition_id); it != partition_snapshots.end())
            return it->second;
        else
            return -1;
    }
}

String MergeTreeSnapshotMetadata::toString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

}
