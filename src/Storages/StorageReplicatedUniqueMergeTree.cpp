#include <Core/Defines.h>

#include <Common/FieldVisitors.h>
#include <Common/Macros.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>

#include <Storages/AlterCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageReplicatedUniqueMergeTree.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/LeaderElectionSingle.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumAddedParts.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/MergeTree/UniqueMergeTreeDataWriter.h>
#include <Storages/MergeTree/UniqueMergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/UniqueMergeTreeDataSelectExecutor.h>
#include <Storages/VirtualColumnUtils.h>

#include <Disks/StoragePolicy.h>

#include <Databases/IDatabase.h>

#include <Parsers/formatAST.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ConnectionTimeouts.h>

#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>

#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <Poco/DirectoryIterator.h>

#include <ext/range.h>
#include <ext/scope_guard.h>

#include <ctime>
#include <thread>
#include <future>

#include <boost/algorithm/string/join.hpp>
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>


namespace ProfileEvents
{
    extern const Event ReplicatedPartMerges;
    extern const Event ReplicatedPartMutations;
    extern const Event ReplicatedPartFailedFetches;
    extern const Event ReplicatedPartFetchesOfMerged;
    extern const Event ObsoleteReplicatedParts;
    extern const Event ReplicatedPartFetches;
    extern const Event DataAfterMergeDiffersFromReplica;
    extern const Event DataAfterMutationDiffersFromReplica;
    extern const Event CreatedLogEntryForMerge;
    extern const Event NotCreatedLogEntryForMerge;
    extern const Event CreatedLogEntryForMutation;
    extern const Event NotCreatedLogEntryForMutation;
}


namespace DB {

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_ZOOKEEPER;
    extern const int INCORRECT_DATA;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int REPLICA_IS_ALREADY_EXIST;
    extern const int NO_REPLICA_HAS_PART;
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
    extern const int ABORTED;
    extern const int REPLICA_IS_NOT_IN_QUORUM;
    extern const int TABLE_IS_READ_ONLY;
    extern const int NOT_FOUND_NODE;
    extern const int NO_ACTIVE_REPLICAS;
    extern const int NOT_A_LEADER;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int PARTITION_ALREADY_EXISTS;
    extern const int TOO_MANY_RETRIES_TO_FETCH_PARTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int PARTITION_DOESNT_EXIST;
    extern const int UNFINISHED;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int TOO_MANY_FETCHES;
    extern const int BAD_DATA_PART_NAME;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int KEEPER_EXCEPTION;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_STATUS_CHANGED;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CORRUPTED_DATA;
    extern const int DUPLICATE_DATA_PART;
    extern const int ROCKSDB_ERROR;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsFetch;
    extern const StorageActionBlockType PartsSend;
    extern const StorageActionBlockType ReplicationQueue;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
}


/*
static const auto QUEUE_UPDATE_ERROR_SLEEP_MS        = 1 * 1000;
static const auto MERGE_SELECTING_SLEEP_MS           = 5 * 1000;
static const auto MUTATIONS_FINALIZING_SLEEP_MS      = 1 * 1000;
static const auto MUTATIONS_FINALIZING_IDLE_SLEEP_MS = 5 * 1000;
 */

StorageReplicatedUniqueMergeTree::StorageReplicatedUniqueMergeTree(
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
    bool has_force_restore_data_flag) :
    StorageReplicatedMergeTree(
        zookeeper_path_,
        replica_name_,
        attach,
        final,
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name,
        merging_params_,
        std::move(settings_),
        has_force_restore_data_flag,
        true)
{
    reader = std::make_shared<UniqueMergeTreeDataSelectExecutor>(*this);
    writer = std::make_shared<UniqueMergeTreeDataWriter>(*this);
    merger_mutator = std::make_shared<UniqueMergeTreeDataMergerMutator>(*this, context_.getBackgroundPool().getNumberOfThreads());
    setInMemoryMetadata(metadata_);
    std::lock_guard lock(write_part_mutex);
    initRocksDb(attach);
    initIdAllocator();
    cleanRocksDbCheckpoint();
}

void StorageReplicatedUniqueMergeTree::recover(int64_t last_seq)
{
    DataPartsVector level0_parts = getLevel0DataPartsVector();
    if (level0_parts.empty()) return;

    int64_t min_seq = getPartBitmap(level0_parts[0])->seq_id;
    int64_t max_seq = getPartBitmap(level0_parts[level0_parts.size()-1])->seq_id;
    if (last_seq + 1 < min_seq)
        LOG_WARNING(log, "Rocksdb does not match data parts, last_seq is {}, min_seq is {}", last_seq, min_seq);
    else if(last_seq > max_seq) {
        LOG_WARNING(log, "Rocksdb's seq id is greater than last part seq id. it means the last insert part has not been committed");
    }

    LOG_INFO(log, "Recover bitmap and rocksdb from level 0 part. last_seq is {}, part seq from {} to {}.", last_seq, min_seq, max_seq);
    recoverRocksDb(level0_parts, last_seq);
}


MergeTreeData::DataPartsVector StorageReplicatedUniqueMergeTree::getLevel0DataPartsVector() {
    auto parts = getDataPartsVector();
    DataPartsVector level0_parts(parts.size());
    auto it = std::copy_if(parts.begin(), parts.end(), level0_parts.begin(), [](DataPartPtr &part){ return part->info.level == 0;});
    level0_parts.resize(std::distance(level0_parts.begin(), it));
    std::sort(level0_parts.begin(), level0_parts.end(), [this](
        DataPartPtr &l_part, DataPartPtr &r_part) {return getPartBitmap(l_part)->seq_id < getPartBitmap(r_part)->seq_id;});
    return level0_parts;
}

void StorageReplicatedUniqueMergeTree::recoverRocksDb(const DataPartsVector &level0_parts, int64_t last_seq)
{
    for (const auto &level0_part : level0_parts) {
        PartBitmap::Ptr level0_part_bitmap = getPartBitmap(level0_part);
        if (level0_part_bitmap->seq_id > last_seq) {
            String rocksdb_update_filename = level0_part->getFullPath() + "key_id_log.bin";
            updateRocksDb(rocksdb_update_filename);
        }
    }
}

int64_t StorageReplicatedUniqueMergeTree::initRocksDb(bool)
{
    bool db_exist = false;
    PathsWithDisks paths_with_disks = getRelativeDataPathsWithDisks();
    String rocksdb_dir;
    DiskPtr disk;
    for (const PathWithDisk& path_with_disk : paths_with_disks) {
        String path = path_with_disk.first;
        disk = path_with_disk.second;
        String db_dir = path + "unique_key";
        if (disk->exists(db_dir)) {
            rocksdb_dir = db_dir;
            db_exist = true;
            break;
        }
    }

    if (!db_exist) {
        disk = reserveSpace(10*1024*1024*1024L)->getDisk();
        rocksdb_dir = relative_data_path + "unique_key";
        disk->createDirectory(rocksdb_dir);
    }
    LOG_INFO(log, "Initial rocksdb from path {}.", disk->getPath());
    rocksdb_disk = disk;

    return initRocksdb(disk, rocksdb_dir);
};

int64_t StorageReplicatedUniqueMergeTree::initRocksdb(DiskPtr disk, String rocksdb_dir)
{
    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.compression = rocksdb::CompressionType::kNoCompression;

    rocksdb_dir = disk->getPath() + rocksdb_dir;
    rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_dir, &db);
    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " +  rocksdb_dir + ": " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
    rocksdb_ptr = std::shared_ptr<rocksdb::DB>(db);

    rocksdb::Slice key("_last_update_seq_id");
    String value;
    status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value);
    if (status.IsNotFound()) {
        LOG_INFO(log, "Initial rocksdb from new file.");
        return  -1;
    }  else if (status.ok()) {
        int seq = *(reinterpret_cast<const int64_t*>(value.c_str()));
        LOG_INFO(log, "Initial rocksdb from file success, last update seq id is {}.", seq);
        return seq;
    }
    throw Exception("Get value from rocksdb error: " +  rocksdb_dir + ": " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
}

bool StorageReplicatedUniqueMergeTree::restoreRocksDb()
{
    String rocksdb_dir = relative_data_path + "unique_key";
    String source_replica_path = zookeeper_path + "/replicas/" + leader_election->leaderReplicaId();
    ReplicatedMergeTreeAddress address(getZooKeeper()->get(source_replica_path + "/host"));
    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(global_context);
    auto [user, password] = global_context.getInterserverCredentials();
    String interserver_scheme = global_context.getInterserverScheme();

    if (interserver_scheme != address.scheme)
        throw Exception("Interserver schemas are different '" + interserver_scheme + "' != '" + address.scheme + "', can't fetch part from " + address.host, ErrorCodes::LOGICAL_ERROR);

    String temp_rocksdb_dir = rocksdb_dir + "_tmp";
    int max_seq_id = fetcher.fetchUnqiueKeyBb(temp_rocksdb_dir, rocksdb_disk, source_replica_path, address.host, address.replication_port, timeouts, user,
        password, interserver_scheme);
    LOG_INFO(log, "Fetch rocks db from leader. current seq: {} fetch_max_seq: {}", part_bitmap_sequnce, max_seq_id);
    if (max_seq_id > part_bitmap_sequnce) {
        lockForAlter(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
        std::lock_guard lock(write_part_mutex);
        truncateRocksDb();
        rocksdb_disk->remove(rocksdb_dir);
        rocksdb_disk->moveDirectory(temp_rocksdb_dir, rocksdb_dir);

        int64_t last_seq = initRocksdb(rocksdb_disk, rocksdb_dir);
        recover(last_seq - 1);
        initIdAllocator();
    }
    return true;
}

bool StorageReplicatedUniqueMergeTree::truncateRocksDb()
{
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;
    String rocksdb_dir = relative_data_path + "unique_key";
    rocksdb_disk->clearDirectory(rocksdb_dir);
    return true;
}

std::mutex& StorageReplicatedUniqueMergeTree::getWritePartMutex() {
    return write_part_mutex;
}

void StorageReplicatedUniqueMergeTree::readRocksDb(
    std::vector<rocksdb::Status> &status, std::vector<rocksdb::Slice> &keys, std::vector<std::string> &values)
{
    rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), keys, &values).swap(status);
}

rocksdb::Status StorageReplicatedUniqueMergeTree::writeRocksDb(rocksdb::WriteBatch &batch)
{
    return rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
}

void StorageReplicatedUniqueMergeTree::updateRocksDb(String &rocksdb_update_filename)
{
    ReadBufferFromFile part_in(rocksdb_update_filename);
    int total_entries = -1;
    rocksdb::WriteBatch batch;
    part_in.read(reinterpret_cast<char *>(&total_entries), sizeof(int));
    for (int i = 0; i < total_entries; i++)
    {
        String key;
        int len = -1;
        part_in.read(reinterpret_cast<char *>(&len), sizeof(int));
        key.resize(len);
        part_in.read(key.data(), len);

        String value;
        len = sizeof(int) * 2;
        value.resize(len);
        part_in.read(value.data(), len);

        batch.Put(rocksdb::Slice(key.c_str(), key.length()), rocksdb::Slice(value.c_str(), value.length()));
    }
    rocksdb::Status status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception("Fail to update log to rocksdb :" + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
}

void StorageReplicatedUniqueMergeTree::initIdAllocator()
{
    std::vector<rocksdb::Slice> keys = {"_last_update_seq_id", "_last_unique_key_id"};
    std::vector<String> values;
    std::vector<rocksdb::Status> get_status = rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), keys, &values);
    for (size_t i =0; i<keys.size(); i++) {
        if (get_status[i].ok()) {
            int64_t value = *(reinterpret_cast<const int64_t *>(values[i].c_str()));
            i==0 ? part_bitmap_sequnce = value + 1 : unique_key_id = value;
        } else {
            part_bitmap_sequnce = 0;
            unique_key_id = 0;
        }
    }
}

void StorageReplicatedUniqueMergeTree::shutdown()
{
    StorageReplicatedMergeTree::shutdown();
    std::lock_guard lock(write_part_mutex);
    rocksdb_ptr->Flush(rocksdb::FlushOptions());
    LOG_INFO(log, "StorageReplicatedUniqueMergeTree shutdown.");
}

void StorageReplicatedUniqueMergeTree::truncate(
    const ASTPtr &ast, const StorageMetadataPtr & metadata_,
    const Context &context, TableExclusiveLockHolder &holder)
{
    StorageReplicatedMergeTree::truncate(ast, metadata_, context, holder);
    std::lock_guard lock(write_part_mutex);
    truncateRocksDb();
    initRocksDb(true);
    initIdAllocator();
}

Pipe StorageReplicatedUniqueMergeTree::alterPartition(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    const Context & query_context)
{
    PartitionCommandsResultInfo result;
    for (const PartitionCommand & command : commands)
    {
        PartitionCommandsResultInfo current_command_results;
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION: {
                if (is_leader)
                    StorageReplicatedMergeTree::alterPartition(query, metadata_snapshot, commands, query_context);
                else
                    LOG_INFO(log, "Ignore Drop partition operation on this replica, because it's not leader.");
                break;
            }

            case PartitionCommand::DROP_DETACHED_PARTITION:
            case PartitionCommand::MOVE_PARTITION:
            case PartitionCommand::FETCH_PARTITION:
            case PartitionCommand::FREEZE_PARTITION:
            case PartitionCommand::FREEZE_ALL_PARTITIONS:
            case PartitionCommand::ATTACH_PARTITION:
            case PartitionCommand::REPLACE_PARTITION:
                throw Exception("UniqueMergeTree does not support Attach/Replace partition.", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    return {};
}

void StorageReplicatedUniqueMergeTree::removeRocksdbKeys(const String &partition_id) {
    std::lock_guard lock(write_part_mutex);
    rocksdb::Slice begin(partition_id);
    String partition_id_end = partition_id;
    partition_id_end[partition_id.size()-1]++;
    rocksdb::Slice end(partition_id_end);
    rocksdb_ptr->DeleteRange(rocksdb::WriteOptions(), nullptr, begin, end);
}

BlockOutputStreamPtr StorageReplicatedUniqueMergeTree::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    const Context & context)
{
    if (!isLeader()) {
        throw Exception("Write cannot be done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);
    }
    const auto storage_settings_ptr = getSettings();
    assertNotReadonly();

    const Settings & query_settings = context.getSettingsRef();
    bool deduplicate = storage_settings_ptr->replicated_deduplication_window != 0 && query_settings.insert_deduplicate;

    return std::make_shared<ReplicatedMergeTreeBlockOutputStream>(
            *this, metadata_snapshot, query_settings.insert_quorum,
            query_settings.insert_quorum_timeout.totalMilliseconds(),
            query_settings.max_partitions_per_insert_block,
            deduplicate);
}

Pipe StorageReplicatedUniqueMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned num_streams)
{
    /** The `select_sequential_consistency` setting has two meanings:
    * 1. To throw an exception if on a replica there are not all parts which have been written down on quorum of remaining replicas.
    * 2. Do not read parts that have not yet been written to the quorum of the replicas.
    * For this you have to synchronously go to ZooKeeper.
    */
    if (context.getSettingsRef().select_sequential_consistency)
    {
        auto max_added_blocks = getMaxAddedBlocks();
        return reader->read(column_names, metadata_snapshot, query_info, context, max_block_size, num_streams, &max_added_blocks);
    }
    return reader->read(column_names, metadata_snapshot, query_info, context, max_block_size, num_streams);
}

bool StorageReplicatedUniqueMergeTree::executeLogEntry(LogEntry & entry)
{
    if (entry.type == LogEntry::DROP_RANGE)
    {
        executeDropRange(entry);
        auto drop_range_info = MergeTreePartInfo::fromPartName(entry.new_part_name, format_version);
        String dropped_partition = drop_range_info.partition_id;
        DataPartsVector partsVector = getDataPartsVectorInPartition(MergeTreeData::DataPartState::Committed, dropped_partition);
        if (partsVector.empty()) {
            removeRocksdbKeys(dropped_partition);
        }
        return true;
    }

    if (entry.type == LogEntry::REPLACE_RANGE)
    {
        executeReplaceRange(entry);
        return true;
    }

    if (entry.type == LogEntry::GET_PART ||
        entry.type == LogEntry::MERGE_PARTS ||
        entry.type == LogEntry::MUTATE_PART)
    {
        /// If we already have this part or a part covering it, we do not need to do anything.
        /// The part may be still in the PreCommitted -> Committed transition so we first search
        /// among PreCommitted parts to definitely find the desired part if it exists.
        DataPartPtr existing_part = getPartIfExists(entry.new_part_name, {MergeTreeDataPartState::PreCommitted});
        if (!existing_part)
            existing_part = getActiveContainingPart(entry.new_part_name);

        /// Even if the part is locally, it (in exceptional cases) may not be in ZooKeeper. Let's check that it is there.
        if (existing_part && getZooKeeper()->exists(replica_path + "/parts/" + existing_part->name))
        {
            if (!(entry.type == LogEntry::GET_PART && entry.source_replica == replica_name))
            {
                LOG_DEBUG(log, "Skipping action for part {} because part {} already exists.", entry.new_part_name, existing_part->name);
            }
            return true;
        }
    }

    if (entry.type == LogEntry::GET_PART && entry.source_replica == replica_name)
        LOG_WARNING(log, "Part {} from own log doesn't exist.", entry.new_part_name);

    /// Perhaps we don't need this part, because during write with quorum, the quorum has failed (see below about `/quorum/failed_parts`).
    if (entry.quorum && getZooKeeper()->exists(zookeeper_path + "/quorum/failed_parts/" + entry.new_part_name))
    {
        LOG_DEBUG(log, "Skipping action for part {} because quorum for that part was failed.", entry.new_part_name);
        return true;    /// NOTE Deletion from `virtual_parts` is not done, but it is only necessary for merge.
    }

    bool do_fetch = false;
    if (entry.type == LogEntry::GET_PART)
    {
        do_fetch = true;
    }
    else if (entry.type == LogEntry::MERGE_PARTS)
    {
        /// Sometimes it's better to fetch merged part instead of merge
        /// For example when we don't have all source parts for merge
        do_fetch = !tryExecuteMerge(entry);
    }
    else if (entry.type == LogEntry::MUTATE_PART)
    {
        /// Sometimes it's better to fetch mutated part instead of merge
        do_fetch = !tryExecutePartMutation(entry);
    }
    else if (entry.type == LogEntry::ALTER_METADATA)
    {
        return executeMetadataAlter(entry);
    }
    else
    {
        throw Exception("Unexpected log entry type: " + toString(static_cast<int>(entry.type)), ErrorCodes::LOGICAL_ERROR);
    }

    if (do_fetch)
        return executeFetch(entry);

    return true;
}

void StorageReplicatedUniqueMergeTree::checkPartChecksumsAndAddCommitOps(const zkutil::ZooKeeperPtr & zookeeper,
    const DataPartPtr & part, Coordination::Requests & ops, String part_name, NameSet * absent_replicas_paths)
{
    if (part_name.empty())
        part_name = part->name;

    auto local_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
        part->getColumns(), part->checksums);

    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
    std::shuffle(replicas.begin(), replicas.end(), thread_local_rng);
    bool has_been_already_added = false;

    for (const String & replica : replicas)
    {
        String current_part_path = zookeeper_path + "/replicas/" + replica + "/parts/" + part_name;

        String part_zk_str;
        if (!zookeeper->tryGet(current_part_path, part_zk_str))
        {
            if (absent_replicas_paths)
                absent_replicas_paths->emplace(current_part_path);

            continue;
        }

        /*
        ReplicatedMergeTreePartHeader replica_part_header;
        if (!part_zk_str.empty())
            replica_part_header = ReplicatedMergeTreePartHeader::fromString(part_zk_str);
        else
        {
            Coordination::Stat columns_stat_before, columns_stat_after;
            String columns_str;
            String checksums_str;
            /// Let's check that the node's version with the columns did not change while we were reading the checksums.
            /// This ensures that the columns and the checksum refer to the same
            if (!zookeeper->tryGet(current_part_path + "/columns", columns_str, &columns_stat_before) ||
                !zookeeper->tryGet(current_part_path + "/checksums", checksums_str) ||
                !zookeeper->exists(current_part_path + "/columns", &columns_stat_after) ||
                columns_stat_before.version != columns_stat_after.version)
            {
                LOG_INFO(log, "Not checking checksums of part {} with replica {} because part changed while we were reading its checksums", part_name, replica);
                continue;
            }

            replica_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksumsZNodes(
                columns_str, checksums_str);
        }

        if (replica_part_header.getColumnsHash() != local_part_header.getColumnsHash())
        {
            LOG_INFO(log, "Not checking checksums of part {} with replica {} because columns are different", part_name, replica);
            continue;
        }

        replica_part_header.getChecksums().checkEqual(local_part_header.getChecksums(), true);
        */

        if (replica == replica_name)
            has_been_already_added = true;

        /// If we verify checksums in "sequential manner" (i.e. recheck absence of checksums on other replicas when commit)
        /// then it is enough to verify checksums on at least one replica since checksums on other replicas must be the same.
        if (absent_replicas_paths)
        {
            absent_replicas_paths->clear();
            break;
        }
    }

    if (!has_been_already_added)
    {
        const auto storage_settings_ptr = getSettings();
        String part_path = replica_path + "/parts/" + part_name;

        //ops.emplace_back(zkutil::makeCheckRequest(
        //    zookeeper_path + "/columns", expected_columns_version));

        if (storage_settings_ptr->use_minimalistic_part_header_in_zookeeper)
        {
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path, local_part_header.toString(), zkutil::CreateMode::Persistent));
        }
        else
        {
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path, "", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path + "/columns", part->getColumns().toString(), zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCreateRequest(
                part_path + "/checksums", getChecksumsForZooKeeper(part->checksums), zkutil::CreateMode::Persistent));
        }
    }
    else
    {
        LOG_WARNING(log, "checkPartAndAddToZooKeeper: node {} already exists. Will not commit any nodes.", replica_path + "/parts/" + part_name);
    }
}

BackgroundProcessingPoolTaskResult StorageReplicatedUniqueMergeTree::queueTask()
{
    static time_t tt = 0;
    time_t t = time(NULL);
    if (tt != t) {
        LOG_TRACE(log, "Queue status is {}", rocksdb_status);
        tt = t;
    }

    if (rocksdb_status == Ok)
        return StorageReplicatedMergeTree::queueTask();

    if (rocksdb_status == UnReady)
        return BackgroundProcessingPoolTaskResult::ERROR;

    {
        std::unique_lock lock(queue_task_mutex, std::try_to_lock);
        if (!lock.owns_lock()) return BackgroundProcessingPoolTaskResult::ERROR;

        if (rocksdb_status == Ready) {
            if (!leader_election) return BackgroundProcessingPoolTaskResult::ERROR;
            String leader_id = leader_election->leaderReplicaId();
            if (leader_id.empty()) return BackgroundProcessingPoolTaskResult::ERROR;

            rocksdb_status = Waiting;
            if (!leader_election->isLeader()) {
                return BackgroundProcessingPoolTaskResult::SUCCESS;
            }
        }

        if (rocksdb_status == Waiting) {
            if (leader_election && leader_election->isLeader()) {
                auto queue_status = queue.getStatus();
                if (queue_status.queue_size == 0) {
                    recover(part_bitmap_sequnce - 1);
                    initIdAllocator();
                    rocksdb_status = Ok;
                    is_leader = true;
                    LOG_INFO(log, "Became leader");
                    merge_selecting_task->activateAndSchedule();
                    LOG_INFO(log, "Recover rocksdb from local success. part_bitmap_sequnce is {}, unique_key_id is {}",
                        part_bitmap_sequnce, unique_key_id);
                }
            } else {
                bool success = restoreRocksDb();
                if (success) {
                    rocksdb_status = Ok;
                    LOG_INFO( log, "Recover rocksdb from leader {} success. part_bitmap_sequnce is {}, unique_key_id is {}",
                        leader_election->leaderReplicaId(), part_bitmap_sequnce, unique_key_id);
                    return BackgroundProcessingPoolTaskResult::SUCCESS;
                } else {
                    return BackgroundProcessingPoolTaskResult::ERROR;
                }
            }
        }
    }
    return StorageReplicatedMergeTree::queueTask();
}

std::shared_ptr<StorageReplicatedUniqueMergeTree> StorageReplicatedUniqueMergeTree::create(
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
    bool has_force_restore_data_flag)
{
    return std::make_shared<StorageReplicatedUniqueMergeTree>(zookeeper_path_, replica_name_, attach, final, table_id_,
        relative_data_path_, metadata_, context_, date_column_name, merging_params_,
        std::move(settings_), has_force_restore_data_flag);
}


String StorageReplicatedUniqueMergeTree::createRocksDbCheckpoint(int64_t &max_seq_id)
{
    if (!is_leader) throw Exception("Cannot send UniqueKeyDb done on this replica because it is not a leader", ErrorCodes::NOT_A_LEADER);

    rocksdb::Checkpoint *checkpoint;
    rocksdb::Status status = rocksdb::Checkpoint::Create(rocksdb_ptr.get(), &checkpoint);
    std::unique_ptr<rocksdb::Checkpoint> checkpoint_ptr = std::unique_ptr<rocksdb::Checkpoint>(checkpoint);

    if (!status.ok()) throw Exception("Fail to create checkpoint for rocksdb :" + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

    String checkpoint_dir;
    while(true) {
        time_t t = time(nullptr);
        checkpoint_dir = relative_data_path + "unique_key" + "_" + std::to_string(t);

        if (!rocksdb_disk->exists(checkpoint_dir)) break;
        t++;
    }

    std::lock_guard lock(write_part_mutex);
    checkpoint_dir = rocksdb_disk->getPath() + checkpoint_dir;
    status = checkpoint->CreateCheckpoint(checkpoint_dir);
    if (!status.ok()) throw Exception("Fail to create checkpoint for rocksdb :" + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

    max_seq_id = part_bitmap_sequnce;

    LOG_INFO(log, "create rocksdb checkpoint path is {}.", checkpoint_dir);
    return checkpoint_dir;
}

void StorageReplicatedUniqueMergeTree::removeRocksDbCheckpoint(const String &rocksdb_checkpoint_dir_name)
{
    Poco::File(rocksdb_checkpoint_dir_name).remove(true);
    LOG_INFO(log, "remove rocksdb checkpoint, path is {}.", rocksdb_checkpoint_dir_name);
}

void StorageReplicatedUniqueMergeTree::cleanRocksDbCheckpoint() {
    std::vector<String> files;
    Poco::File(rocksdb_disk->getPath() + relative_data_path).list(files);

    for (const String& file : files) {
        if (file.starts_with("unique_key_")) {
            rocksdb_disk->removeRecursive(relative_data_path + file);
        }
    }
}

PartBitmap::Ptr& StorageReplicatedUniqueMergeTree::getPartBitmap(const DataPartPtr &part, bool locked)
{
    if (!locked) unique_merge_tree_mutex.lock();
    auto itr = part_unique_bitmaps.find(part->name);
    if (itr == part_unique_bitmaps.end()) {
        String part_path = part->getFullPath();
        BitmapPtr bitmap = std::make_shared<Bitmap>();
        ReadBufferFromFile part_in(part_path + "bitmap.bin");
        bitmap->read(part_in);
        int64_t seq_id = 0;
        part_in.read(reinterpret_cast<char*>(&seq_id), sizeof(seq_id));
        int64_t update_seq_id = 0;
        part_in.read(reinterpret_cast<char*>(&update_seq_id), sizeof(update_seq_id));

        itr = part_unique_bitmaps.insert(std::make_pair(
            part->name, PartBitmap::create(bitmap, part, seq_id, update_seq_id))).first;
    }
    //auto &part_bitmap = itr->second;
    //part_bitmap->part = part;
    if (!locked) unique_merge_tree_mutex.unlock();
    return itr->second;
}

void StorageReplicatedUniqueMergeTree::removePartBitmap(const DataPartPtr &part) {
    std::lock_guard lock(unique_merge_tree_mutex);
    part_unique_bitmaps.erase(part->name);
}

void StorageReplicatedUniqueMergeTree::enterLeaderElection()
{
    LOG_INFO(log, "Enter leader election");
    auto callback = [this]()
    {
        LOG_INFO(log, "Prepare became leader");
        {
            std::lock_guard lock(queue_task_mutex);
            rocksdb_status = Ready;
        }
        if (queue_task_handle)
            queue_task_handle->signalReadyToRun();
    };

    try
    {
        leader_election = std::make_shared<zkutil::LeaderElectionSingle>(
            global_context.getSchedulePool(),
            zookeeper_path + "/leader_election",
            *current_zookeeper,    /// current_zookeeper lives for the lifetime of leader_election,
                                   ///  since before changing `current_zookeeper`, `leader_election` object is destroyed in `partialShutdown` method.
            callback,
            replica_name);
        {
            std::lock_guard lock(queue_task_mutex);
            rocksdb_status = Ready;
        }
    }
    catch (...)
    {
        leader_election = nullptr;
        throw;
    }
}

void StorageReplicatedUniqueMergeTree::exitLeaderElection()
{
    LOG_INFO(log, "Exit leader election");
    {
        std::lock_guard lock(queue_task_mutex);
        rocksdb_status = UnReady;
    }
    StorageReplicatedMergeTree::exitLeaderElection();

}

void StorageReplicatedUniqueMergeTree::mergeSelectingTask()
{
    auto zookeeper = getZooKeeper();
    Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

    bool can_merge = true;
    String reason = "";
    try {
        for (const auto & replica : replicas)
        {
            if (replica == replica_name)
                continue;

            // if replica death. delay merge selection
            if (!zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active")) {
                can_merge = false;
                reason = "replica " + replica + " is no active.";
                break;
            }

            String value;
            if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/min_unprocessed_insert_time", value)) {
                can_merge = false;
                reason = "replica " + replica + " min_unprocessed_insert_time is not set.";
                break;
            }

            time_t replica_time = value.empty() ? 0 : parse<time_t>(value);
            time_t delay = time(NULL) - replica_time;
            if (delay > 5 && replica_time != 0) {
                can_merge = false;
                reason = "replica " + replica + " is delay after " + std::to_string(delay) + " seconds";
                break;
            }
        }
    } catch (...) {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
    if (!can_merge) {
        LOG_DEBUG(log, "Some replica is delayed, delay merge selection. because " + reason);
        merge_selecting_task->scheduleAfter(5 * 1000);
        return;
    }

    return StorageReplicatedMergeTree::mergeSelectingTask();
}

void StorageReplicatedUniqueMergeTree::mergePartsBitmaps(
    DataPartsVector &merge_parts,
    const DataPartsVector &select_parts,
    std::vector<PartBitmap::Ptr> &part_bitmaps)
{
    part_bitmaps.reserve(select_parts.size());
    std::lock_guard lock(unique_merge_tree_mutex);

    mergePartitionPartsBitmaps(merge_parts);
    for (const DataPartPtr &part : select_parts) {
        part_bitmaps.emplace_back(getPartBitmap(part, true));
    }
    assert(select_parts.size() == part_bitmaps.size());
}

void StorageReplicatedUniqueMergeTree::mergePartitionPartsBitmaps(DataPartsVector &parts)
{
    if (parts.empty()) return;

    bool is_same_seq = true;
    int part_bitmap_update_seq = getPartBitmap(parts[0], true)->update_seq_id;
    for (const DataPartPtr &part : parts) {
        if (part_bitmap_update_seq != getPartBitmap(part, true)->update_seq_id) {
            is_same_seq = false;
            LOG_TRACE(log, "check part bitmap seq failed. part: {} seq: {} last_seq: {}",
                part->name, getPartBitmap(part, true)->update_seq_id, part_bitmap_update_seq);
            break;
        }
        LOG_TRACE(log, "check part bitmap part: {} seq: {}", part->name, getPartBitmap(part, true)->update_seq_id);
    }

    if (is_same_seq) {
        LOG_TRACE(log, "check part bitmap seq success, all part bitmap seq are same. seq is {}", part_bitmap_update_seq);
        return;
    }


    std::sort(parts.begin(), parts.end(), [this](DataPartPtr &l_part, DataPartPtr &r_part)
              {return getPartBitmap(l_part, true)->seq_id < getPartBitmap(r_part, true)->seq_id;});

    BitmapPtr merged_bitmap = std::make_shared<Bitmap>();
    bool update_all = false;
    for (size_t i=parts.size()-1; i!=0; i--) {
        PartBitmap::Ptr current_part_bitmap = getPartBitmap(parts[i], true);
        PartBitmap::Ptr &next_part_bitmap = getPartBitmap(parts[i-1], true);
        merged_bitmap->rb_or(*current_part_bitmap->bitmap);

        if (update_all || current_part_bitmap->update_seq_id != next_part_bitmap->update_seq_id) {
            LOG_TRACE(log, "update delayed part bitmap part: {} seq: {} update_seq: {}, new_part :{} seq: {}, update_seq: {}",
                next_part_bitmap->part->name, next_part_bitmap->seq_id,
                next_part_bitmap->update_seq_id, current_part_bitmap->part->name, current_part_bitmap->seq_id,
                current_part_bitmap->update_seq_id);

            PartBitmap::MutablePtr next_part_bitmap_writeable = PartBitmap::mutate(std::move(next_part_bitmap));
            next_part_bitmap_writeable->bitmap->rb_andnot(*merged_bitmap);
            next_part_bitmap_writeable->write(current_part_bitmap->update_seq_id);
            next_part_bitmap = std::move(next_part_bitmap_writeable);
            update_all = true;
        } else {
            LOG_TRACE(log, "updated part bitmap part: {} seq: {} update_seq: {}",
                current_part_bitmap->part->name, current_part_bitmap->seq_id, current_part_bitmap->update_seq_id);
        }
    }
}

void PartBitmap::write(int64_t update_seq_id_)
{
    String path = part->getFullPath();
    std::string tmp_fname = path + "bitmap.bin.tmp";
    std::string fname = path + "bitmap.bin";
    WriteBufferFromFile out(tmp_fname);
    bitmap->write(out);
    out.write(reinterpret_cast<char*>(&seq_id), sizeof(seq_id));
    update_seq_id = update_seq_id_;
    out.write(reinterpret_cast<char*>(&update_seq_id), sizeof(update_seq_id));
    out.close();
    Poco::File(tmp_fname).renameTo(fname);
}

MergeTreeData::DataPartsVector
StorageReplicatedUniqueMergeTree::UniqueMergeTreeTransaction::commit(MergeTreeData::DataPartsLock * acquired_parts_lock)
{
    DataPartsVector total_covered_parts;

    if (!isEmpty())
    {
        auto parts_lock = acquired_parts_lock ? DataPartsLock() : data.lockParts();
        auto * owing_parts_lock = acquired_parts_lock ? acquired_parts_lock : &parts_lock;

        auto current_time = time(nullptr);
        for (const DataPartPtr & part : precommitted_parts)
        {
            DataPartPtr covering_part;
            DataPartsVector covered_parts = data.getActivePartsToReplace(part->info, part->name, covering_part, *owing_parts_lock);
            if (covering_part)
            {
                LOG_WARNING(data.log, "Tried to commit obsolete part {} covered by {}", part->name, covering_part->getNameWithState());

                part->remove_time.store(0, std::memory_order_relaxed); /// The part will be removed without waiting for old_parts_lifetime seconds.
                data.modifyPartState(part, DataPartState::Outdated);
            }
            else
            {
                total_covered_parts.insert(total_covered_parts.end(), covered_parts.begin(), covered_parts.end());
                for (const DataPartPtr & covered_part : covered_parts)
                {
                    covered_part->remove_time.store(current_time, std::memory_order_relaxed);
                    data.modifyPartState(covered_part, DataPartState::Outdated);
                    data.removePartContributionToColumnSizes(covered_part);
                }

                if (!storage_data.isLeader()) {
                    if (part->info.level == 0) {
                        String rocksdb_update_filename = part->getFullPath() + "key_id_log.bin";
                        std::lock_guard lock(storage_data.write_part_mutex);
                        storage_data.updateRocksDb(rocksdb_update_filename);
                    }

                    {
                        std::lock_guard lock(storage_data.unique_merge_tree_mutex);
                        PartBitmap::Ptr part_bitmap_ = storage_data.getPartBitmap(part, true);
                        PartBitmap * part_bitmap = const_cast<PartBitmap *>(part_bitmap_.get());
                        part_bitmap->write(part_bitmap->seq_id);
                    }
                }

                data.modifyPartState(part, DataPartState::Committed);
                data.addPartContributionToColumnSizes(part);
            }
        }
    }

    clear();
    return total_covered_parts;
}

void StorageReplicatedUniqueMergeTree::clearOldPartsAndRemoveFromZK(){
    {
        //todo  need fix coredump in loop and remove on map
        std::lock_guard lock(unique_merge_tree_mutex);

        for (auto itr = part_unique_bitmaps.begin(); itr != part_unique_bitmaps.end();) {
            if (itr->second->part->state != DataPartState::Committed &&
                itr->second->part->state != DataPartState::PreCommitted) {
                auto del_itr = itr;
                ++itr;
                part_unique_bitmaps.erase(del_itr);
            } else
                ++itr;
        }
    }
    return StorageReplicatedMergeTree::clearOldPartsAndRemoveFromZK();
}

}

