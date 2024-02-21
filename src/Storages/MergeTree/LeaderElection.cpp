#include <Storages/MergeTree/LeaderElection.h>

#include <filesystem>
#include <base/sort.h>
#include <Common/ZooKeeper/KeeperException.h>

namespace fs = std::filesystem;

namespace zkutil
{

/** Initially was used to implement leader election algorithm described here:
  * http://zookeeper.apache.org/doc/r3.4.5/recipes.html#sc_leaderElection
  *
  * But then we decided to get rid of leader election, so every replica can become leader.
  * For now, every replica can become leader if there is no leader among replicas with old version.
  */

void checkNoOldLeaders(Poco::Logger * log, ZooKeeper & zookeeper, const String path)
{
    /// Previous versions (before 21.12) used to create ephemeral sequential node path/leader_election-
    /// Replica with the lexicographically smallest node name becomes leader (before 20.6) or enables multi-leader mode (since 20.6)
    constexpr auto persistent_multiple_leaders = "leader_election-0";   /// Less than any sequential node
    constexpr auto suffix = " (multiple leaders Ok)";
    constexpr auto persistent_identifier = "all (multiple leaders Ok)";

    size_t num_tries = 1000;
    while (num_tries--)
    {
        Strings potential_leaders;
        Coordination::Error code = zookeeper.tryGetChildren(path, potential_leaders);
        /// NOTE zookeeper_path/leader_election node must exist now, but maybe we will remove it in future versions.
        if (code == Coordination::Error::ZNONODE)
            return;
        else if (code != Coordination::Error::ZOK)
            throw KeeperException::fromPath(code, path);

        Coordination::Requests ops;

        if (potential_leaders.empty())
        {
            /// Ensure that no leaders appeared and enable persistent multi-leader mode
            /// May fail with ZNOTEMPTY
            ops.emplace_back(makeRemoveRequest(path, 0));
            ops.emplace_back(makeCreateRequest(path, "", zkutil::CreateMode::Persistent));
            /// May fail with ZNODEEXISTS
            ops.emplace_back(makeCreateRequest(fs::path(path) / persistent_multiple_leaders, persistent_identifier, zkutil::CreateMode::Persistent));
        }
        else
        {
            ::sort(potential_leaders.begin(), potential_leaders.end());
            if (potential_leaders.front() == persistent_multiple_leaders)
                return;

            /// Ensure that current leader supports multi-leader mode and make it persistent
            auto current_leader = fs::path(path) / potential_leaders.front();
            Coordination::Stat leader_stat;
            String identifier;
            if (!zookeeper.tryGet(current_leader, identifier, &leader_stat))
            {
                LOG_INFO(log, "LeaderElection: leader suddenly changed, will retry");
                continue;
            }

            if (!identifier.ends_with(suffix))
                throw Poco::Exception(fmt::format("Found leader replica ({}) with too old version (< 20.6). Stop it before upgrading", identifier));

            /// Version does not matter, just check that it still exists.
            /// May fail with ZNONODE
            ops.emplace_back(makeCheckRequest(current_leader, leader_stat.version));
            /// May fail with ZNODEEXISTS
            ops.emplace_back(makeCreateRequest(fs::path(path) / persistent_multiple_leaders, persistent_identifier, zkutil::CreateMode::Persistent));
        }

        Coordination::Responses res;
        code = zookeeper.tryMulti(ops, res);
        if (code == Coordination::Error::ZOK)
            return;
        else if (code == Coordination::Error::ZNOTEMPTY || code == Coordination::Error::ZNODEEXISTS || code == Coordination::Error::ZNONODE)
            LOG_INFO(log, "LeaderElection: leader suddenly changed or new node appeared, will retry");
        else
            KeeperMultiException::check(code, ops, res);
    }

    throw Poco::Exception("Cannot check that no old leaders exist");
}

LeaderElection::LeaderElection(
    DB::BackgroundSchedulePool & pool_,
    const std::string & path_,
    ZooKeeper & zookeeper_,
    LeadershipHandler handler_,
    const std::string & identifier_)
    : pool(pool_)
    , path(path_)
    , zookeeper(zookeeper_)
    , handler(handler_)
    , identifier(identifier_ + suffix)
    , log_name("LeaderElection.(" + path + ")")
    , log(&Poco::Logger::get(log_name))
{
    task = pool.createTask(log_name, [this] { threadFunction(); });
    createNode();
}


void LeaderElection::shutdown()
{
    leader_replica_id = "";
    if (shutdown_called)
        return;

    shutdown_called = true;
    task->deactivate();
}

void LeaderElection::createNode()
{
    shutdown_called = false;
    node = EphemeralNodeHolder::createSequential(path + "/leader_election-", zookeeper, identifier);

    std::string node_path = node->getPath();
    node_name = node_path.substr(node_path.find_last_of('/') + 1);

    task->activateAndSchedule();
}

void LeaderElection::releaseNode()
{
    try
    {
        shutdown();
        node = nullptr;
    }
    catch (...)
    {
    }
}

void LeaderElection::threadFunction()
{
    bool success = false;

    try
    {
        Strings children = zookeeper.getChildren(path);
        ::sort(children.begin(), children.end());

        auto my_node_it = std::lower_bound(children.begin(), children.end(), node_name);
        if (my_node_it == children.end() || *my_node_it != node_name)
        {
            leader_replica_id = "";
            throw Poco::Exception("Assertion failed in LeaderElection");
        }

        String value = zookeeper.get(path + "/" + children.front());
        leader_replica_id = value.substr(0, value.find(' '));

        LOG_DEBUG(log, "Path {} leader replica id is: {}, children: {}, my node: {}", path, leader_replica_id, fmt::join(children, ", "), *my_node_it);

#if !defined(ARCADIA_BUILD) /// C++20; Replicated tables are unused in Arcadia.
        if (my_node_it == children.begin() && value.ends_with(suffix))
        {
            if (handler())
                return;

            task->scheduleAfter(10'000);
            return;
        }
#endif
        if (my_node_it == children.begin())
        {
            leader_replica_id = "";
            throw Poco::Exception("Assertion failed in LeaderElection");
        }

        /// Watch for the node in front of us.
        if (!zookeeper.existsWatch(path + "/" + children.front(), nullptr, task->getWatchCallback()))
            task->schedule();

        success = true;
    }
    catch (const KeeperException & e)
    {
        leader_replica_id = "";
        DB::tryLogCurrentException(log);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;
    }
    catch (...)
    {
        leader_replica_id = "";
        DB::tryLogCurrentException(log);
    }

    if (!success)
        task->scheduleAfter(10'000);
}

}
