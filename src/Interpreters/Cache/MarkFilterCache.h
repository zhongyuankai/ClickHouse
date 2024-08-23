#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/// table_id(uuid)->data_part->condition
class MarkFilterCache
{
public:
    // enum class ConditionMarkStatus : UInt8
    // {
    //     FALSE = 0,
    //     TRUE = 1,
    //     UNKNOW = 2,
    // };

    MarkFilterCache(size_t max_count_)
        : max_count(max_count_)
    {}

    std::vector<bool> getByCondition(const MergeTreeDataPartPtr & data_part, const String & condition);

    void update(const MergeTreeDataPartPtr & data_part, const String & condition, const MarkRanges & mark_ranges, bool exists);

    void removeTable(const UUID & table_id);

    void removePart(const UUID & table_id, const String & part_name);

private:
    struct Key
    {
        UUID table_id;
        String part_name;
        String condition;
    };
    using KeyPtr = std::shared_ptr<Key>;

    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct Entry
    {
        std::vector<bool> filter;
        LRUQueueIterator queue_iterator;
    };
    using EntryPtr = std::shared_ptr<Entry>;

    struct PartMetadata : std::unordered_map<String, EntryPtr>
    {
        EntryPtr tryGetEntry(const Key & key);

        // void setEntryAndUpdateQueue(const Key & key, const EntryPtr & entry, LRUQueue & queue);

        std::tuple<bool, EntryPtr> getOrSet(const Key & key);

        bool remove(const Key & key) { return erase(key.condition); }

    };
    using PartMetadataPtr = std::shared_ptr<PartMetadata>;

    struct TableMetadata : std::unordered_map<String, PartMetadataPtr>
    {
        PartMetadataPtr getPartMetadata(const String & part_name);
        // PartMetadataPtr getOrSetPartMetadata(const Key & key);

        EntryPtr tryGetEntry(const Key & key);

        // void setEntryAndUpdateQueue(const Key & key, const EntryPtr & entry, LRUQueue & queue);

        std::tuple<bool, EntryPtr> getOrSet(const Key & key);

        bool remove(const Key & key);

        bool removePart(const String & part_name);

    };
    using TableMetadataPtr = std::shared_ptr<TableMetadata>;


    EntryPtr get(const Key & key);
    EntryPtr getOrSet(const Key & key);

    // void set(const Key & key, const EntryPtr & entry);

    bool remove(const Key & key);

    // TableMetadataPtr getTableMetadata(const Key & key);
    // TableMetadataPtr getOrSetTableMetadata(const Key & key);

    void removeOverflow();

    size_t max_count;

    using Cache = std::unordered_map<UUID, TableMetadataPtr>;
    Cache cache;
    LRUQueue queue;
    std::mutex mutex;

};

using MarkFilterCachePtr = std::shared_ptr<MarkFilterCache>;

}

