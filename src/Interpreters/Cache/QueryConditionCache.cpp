#include <memory>
#include <mutex>
#include <shared_mutex>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>

namespace ProfileEvents
{
    extern const Event QueryConditionCacheHits;
    extern const Event QueryConditionCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric QueryConditionCacheBytes;
    extern const Metric QueryConditionCacheEntries;
}

namespace DB
{

bool QueryConditionCache::Key::operator==(const Key & other) const
{
    return table_id == other.table_id
        && part_name == other.part_name
        && condition_hash == other.condition_hash;
}

size_t QueryConditionCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.table_id);
    hash.update(key.part_name);
    hash.update(key.condition_hash);
    return hash.get64();
}

size_t QueryConditionCache::EntryWeight::operator()(const Entry & entry) const
{
    /// Estimate the memory size of `std::vector<bool>` (it uses bit-packing internally)
    size_t memory = (entry.capacity() + 7) / 8; /// round up to bytes.
    return memory + sizeof(decltype(entry));
}

QueryConditionCache::QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
    : cache(cache_policy, CurrentMetrics::QueryConditionCacheBytes, CurrentMetrics::QueryConditionCacheEntries, max_size_in_bytes, 0, size_ratio)
{
}

void QueryConditionCache::write(const Key & key, const Entry & entry)
{
    cache.set(key, std::make_shared<Entry>(entry));
}

QueryConditionCache::EntryPtr QueryConditionCache::read(const UUID & table_id, const String & part_name, UInt64 condition_hash)
{
    Key key = {table_id, part_name, condition_hash, ""};

    if (auto entry = cache.get(key))
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheHits);

        LOG_TEST(
            logger,
            "Read entry for table_uuid: {}, part: {}, condition_hash: {}",
            table_id,
            part_name,
            condition_hash);

        return {entry};
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::QueryConditionCacheMisses);

        LOG_TEST(
            logger,
            "Could not find entry for table_uuid: {}, part: {}, condition_hash: {}",
            table_id,
            part_name,
            condition_hash);

        return {};
    }

}

std::vector<QueryConditionCache::Cache::KeyMapped> QueryConditionCache::dump() const
{
    return cache.dump();
}

void QueryConditionCache::clear()
{
    cache.clear();
}

void QueryConditionCache::setMaxSizeInBytes(size_t max_size_in_bytes)
{
    cache.setMaxSizeInBytes(max_size_in_bytes);
}

size_t QueryConditionCache::maxSizeInBytes() const
{
    return cache.maxSizeInBytes();
}

QueryConditionCacheWriter::QueryConditionCacheWriter(
    QueryConditionCache & query_condition_cache_,
    size_t condition_hash_,
    const String & condition_,
    double selectivity_threshold_)
    : query_condition_cache(query_condition_cache_)
    , condition_hash(condition_hash_)
    , condition(condition_)
    , selectivity_threshold(selectivity_threshold_)
    /// Implementation note: It would be nicer to pass in the table_id as well ...
{}

QueryConditionCacheWriter::~QueryConditionCacheWriter()
{
    finalize();
}

void QueryConditionCacheWriter::addRanges(const UUID & table_id, const String & part_name, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark)
{
    /// Clickhouse scan ranges in parallel, and the addRanges method will be frequently called.
    /// Extra care is required to avoid performance problems caused by lock contention.

    QueryConditionCache::Key key = {table_id, part_name, condition_hash, condition};

    CacheEntryPtr cache_entry;
    {
        std::lock_guard lock(mutex);
        if (auto it = new_entries.find(key); it != new_entries.end())
            cache_entry = it->second;
    }

    /// In most cases, marks do not need to be updated. 
    /// To improve performance, first acquire a read lock and check whether marks need to be updated.
    /// If marks need to be updated, then acquire a write lock and perform the update.
    bool need_update_marks = false;
    if (cache_entry)
    {
        // Acquire a read lock and check, if no update is needed, exit immediately.
        std::shared_lock shared_lock(cache_entry->mutex);
        if (!needUpdateMarks(cache_entry->entry, mark_ranges, marks_count, has_final_mark))
            return;

        need_update_marks = true;
    }

    bool is_insert = false;
    if (!need_update_marks)
    {
        /// Check from the query condition cache whether marks need to be updated, 
        /// which helps avoid a large number of unnecessary updates and reduces lock contention.
        /// It also addresses the issue where marks in the cache entries are incomplete 
        /// due to the LIMIT operator terminating the ranges scan early.
        auto entry_ptr = query_condition_cache.read(table_id, part_name, condition_hash);
        if (entry_ptr)
        {
            chassert(marks_count == entry_ptr->size());

            if (!needUpdateMarks(*entry_ptr, mark_ranges, marks_count, has_final_mark))
                return;

            {
                std::lock_guard lock(mutex);
                /// Copy entry in query condition cache.
                auto [it, inserted] = new_entries.try_emplace(key, *entry_ptr);
                cache_entry = it->second;
                is_insert = true;
            }
        }
    }

    if (!cache_entry)
    {
        std::lock_guard lock(mutex);
        /// Create new entry.
        auto [it, inserted] = new_entries.try_emplace(key, marks_count);
        cache_entry = it->second;
        is_insert = true;
    }

    {
        std::lock_guard lock(cache_entry->mutex);
        /// The input mark ranges are the areas which the scan can skip later on.
        for (const auto & mark_range : mark_ranges)
            std::fill(cache_entry->entry.begin() + mark_range.begin, cache_entry->entry.begin() + mark_range.end, false);

        if (has_final_mark)
            cache_entry->entry[marks_count - 1] = false;
    }

    LOG_TEST(
        logger,
        "{} entry for table_id: {}, part_name: {}, condition_hash: {}, condition: {}, marks_count: {}, has_final_mark: {}",
        is_insert ? "Inserted" : "Updated",
        table_id,
        part_name,
        condition_hash,
        condition,
        marks_count,
        has_final_mark);
}

void QueryConditionCacheWriter::finalize()
{
    std::lock_guard<std::mutex> lock(mutex);

    for (const auto & [key, cache_entry] : new_entries)
    {
        auto & entry = cache_entry->entry;
        if (entry.empty())
            continue;

        size_t matching_marks = std::count(entry.begin(), entry.end(), true);
        double selectivity = static_cast<double>(matching_marks) / entry.size();
        if (selectivity <= selectivity_threshold)
            query_condition_cache.write(key, entry);
    }
}

bool QueryConditionCacheWriter::needUpdateMarks(const QueryConditionCache::Entry & entry, const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark) const
{
    bool need_not_update_marks = true;
    for (const auto & mark_range : mark_ranges)
    {
        /// If the bits are already in the desired state (false), we don't need to update them.
        need_not_update_marks = std::all_of(entry.begin() + mark_range.begin,
                                            entry.begin() + mark_range.end,
                                            [](auto b) { return b == false; });
        if (!need_not_update_marks)
            break;
    }

    /// Do we either have no final mark or final mark is already in the desired state?
    bool need_not_update_final_mark = !has_final_mark || entry[marks_count - 1] == false;

    if (need_not_update_marks && need_not_update_final_mark)
        return false;

    return true;
}

}
