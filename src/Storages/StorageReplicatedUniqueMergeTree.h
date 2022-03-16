#pragma once

#include <ext/shared_ptr_helper.h>
#include <rocksdb/db.h>

#include "StorageReplicatedMergeTree.h"
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/PartBitmap.h>


namespace rocksdb
{
    class DB;
}

namespace DB
{

class StorageReplicatedUniqueMergeTree : public StorageReplicatedMergeTree
{
    using RocksDBPtr = std::shared_ptr<rocksdb::DB>;

    enum {
        UnReady,
        Ready,
        Waiting,
        Ok
    };

    class UniqueMergeTreeTransaction : public Transaction {
    public:
        UniqueMergeTreeTransaction(MergeTreeData& data_) : Transaction(data_), storage_data(static_cast<StorageReplicatedUniqueMergeTree &>(data_)) {}
        MergeTreeData::DataPartsVector commit(MergeTreeData::DataPartsLock * acquired_parts_lock = nullptr) override;

        StorageReplicatedUniqueMergeTree &storage_data;
    };

public:
    std::string getName() const override { return "ReplicatedUniqueMergeTree"; }
    void shutdown() override;
    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &) override;
    Pipe alterPartition(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        const Context & query_context) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;
    Pipe read(const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BackgroundProcessingPoolTaskResult queueTask() override;
    bool executeLogEntry(LogEntry & entry) override;

    void enterLeaderElection() override;
    void exitLeaderElection() override;
    void mergeSelectingTask() override;

    NamesAndTypesList getVirtuals() const override
    {
        NamesAndTypesList virtuals = MergeTreeData::getVirtuals();
        virtuals.insert(virtuals.end(), NameAndTypePair("_unique_key_id", std::make_shared<DataTypeUInt32>()));
        return virtuals;
    }

    bool supportTrivialCount() const override{ return false; }
    bool allowOnlyWriteLeaderReplica() override{ return true; }

    void setInMemoryMetadata(const StorageInMemoryMetadata & metadata_) override{
        StorageInMemoryMetadata new_metadata = metadata_;
        new_metadata.internal_columns = {NameAndTypePair("_unique_key_id", std::make_shared<DataTypeUInt32>())};
        new_metadata.setMergeTreeSettings(getSettings());
        IStorage::setInMemoryMetadata(new_metadata);
    }
    void checkPartChecksumsAndAddCommitOps(const zkutil::ZooKeeperPtr & zookeeper, const DataPartPtr & part,
        Coordination::Requests & ops, String part_name = "", NameSet * absent_replicas_paths = nullptr) override;

    std::mutex& getWritePartMutex();
    void readRocksDb(std::vector<rocksdb::Status> &status, std::vector<rocksdb::Slice> &keys, std::vector<std::string> &values);
    rocksdb::Status writeRocksDb(rocksdb::WriteBatch &batch);

    int getAndIncreamentUniqueKeyId() {
        return unique_key_id++;
    }
    int getUniqueKeyId() {
        return unique_key_id;
    }
    int64_t getAndIncreamentPartBitmapSeqId() {
        return part_bitmap_sequnce++;
    }
    String createRocksDbCheckpoint(int64_t &max_seq_id);
    void removeRocksDbCheckpoint(const String &rocksdb_dir_name);
    void mergePartsBitmaps(
        DataPartsVector &merge_parts,
        const DataPartsVector &select_parts,
        std::vector<PartBitmap::Ptr> &part_bitmaps);
    void clearOldPartsAndRemoveFromZK() override;
    void removeRocksdbKeys(const String &partition_id);

private:
    int64_t initRocksDb(bool attach);
    int64_t initRocksdb(DiskPtr disk, String rocksdb_dir);
    void initIdAllocator();
    void recover(int64_t last_seq);
    bool restoreRocksDb();
    void recoverRocksDb(const DataPartsVector &level0_parts, int64_t last_seq);
    void updateRocksDb(String &rocksdb_update_filename);
    bool truncateRocksDb();
    void cleanRocksDbCheckpoint();
    std::shared_ptr<Transaction> createTransaction(MergeTreeData & data_) override{
        return std::make_shared<UniqueMergeTreeTransaction>(data_);
    }
    MergeTreeData::DataPartsVector getLevel0DataPartsVector();
    void mergePartitionPartsBitmaps(DataPartsVector &parts);

private:
    std::atomic<int> unique_key_id{0};
    std::atomic<int64_t> part_bitmap_sequnce{0};
    std::map<String, PartBitmap::Ptr> part_unique_bitmaps;
    RocksDBPtr rocksdb_ptr;
    DiskPtr rocksdb_disk;
    std::mutex queue_task_mutex;
    std::mutex unique_merge_tree_mutex;
    std::mutex write_part_mutex;
    std::atomic<int32_t> rocksdb_status = UnReady;
public:
    StorageReplicatedUniqueMergeTree(
        const String & zookeeper_path_,
        const String & replica_name_,
        bool attach,
        bool final,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        Context & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);

    ~StorageReplicatedUniqueMergeTree() override{
        rocksdb_ptr->Close();
        rocksdb_ptr = nullptr;
    }

    static std::shared_ptr<StorageReplicatedUniqueMergeTree> create(
        const String & zookeeper_path_,
        const String & replica_name_,
        bool attach,
        bool final,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        Context & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);

    PartBitmap::Ptr& getPartBitmap(const DataPartPtr &part, bool locked = false);
    void removePartBitmap(const DataPartPtr &part);
};

}

