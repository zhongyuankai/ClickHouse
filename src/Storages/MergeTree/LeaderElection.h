#pragma once

#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>

namespace zkutil
{

void checkNoOldLeaders(Poco::Logger * log, ZooKeeper & zookeeper, const String path);

class LeaderElection
{
public:
    using LeadershipHandler = std::function<bool()>;

    /** handler is called when this instance become leader.
      *
      * identifier - if not empty, must uniquely (within same path) identify participant of leader election.
      * It means that different participants of leader election have different identifiers
      *  and existence of more than one ephemeral node with same identifier indicates an error.
      */
    LeaderElection(
        DB::BackgroundSchedulePool & pool_,
        const std::string & path_,
        ZooKeeper & zookeeper_,
        LeadershipHandler handler_,
        const std::string & identifier_);

    String leaderReplicaId() { return leader_replica_id; }

    void shutdown();

    ~LeaderElection() { releaseNode(); }

private:
    static inline constexpr auto suffix = " (Single leaders Ok)";
    DB::BackgroundSchedulePool & pool;
    DB::BackgroundSchedulePool::TaskHolder task;
    std::string path;
    ZooKeeper & zookeeper;
    LeadershipHandler handler;
    std::string identifier;
    std::string log_name;
    Poco::Logger * log;

    EphemeralNodeHolderPtr node;
    std::string node_name;
    std::string leader_replica_id;

    std::atomic<bool> shutdown_called {false};

    void createNode();

    void releaseNode();

    void threadFunction();
};

using LeaderElectionPtr = std::shared_ptr<LeaderElection>;

}
