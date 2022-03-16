#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/StorageReplicatedUniqueMergeTree.h>
#include <Storages/MutationCommands.h>
#include <atomic>
#include <functional>
#include <Common/ActionBlocker.h>
#include <Storages/MergeTree/TTLMergeSelector.h>


namespace DB
{

class UniqueMergeTreeDataMergerMutator : public MergeTreeDataMergerMutator
{
public:
    UniqueMergeTreeDataMergerMutator(StorageReplicatedUniqueMergeTree & data_, size_t background_pool_size);

    MergeTreeData::MutableDataPartPtr mergePartsToTemporaryPart(
        const FutureMergedMutatedPart & future_part,
        const StorageMetadataPtr & metadata_snapshot,
        MergeListEntry & merge_entry,
        TableLockHolder & table_lock_holder,
        time_t time_of_merge,
        const ReservationPtr & space_reservation,
        bool deduplicate) override;

    MergeTreeData::MutableDataPartPtr mutatePartToTemporaryPart(
        const FutureMergedMutatedPart & future_part,
        const StorageMetadataPtr & metadata_snapshot,
        const MutationCommands & commands,
        MergeListEntry & merge_entry,
        time_t time_of_mutation,
        const Context & context,
        const ReservationPtr & space_reservation,
        TableLockHolder &) override;

private:
    StorageReplicatedUniqueMergeTree &storage_data;
};

}
