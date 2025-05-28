#include <Storages/System/StorageSystemQueryConditionCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include "Core/NamesAndTypes.h"


namespace DB
{

NamesAndTypesList StorageSystemQueryConditionCache::getNamesAndTypes()
{
    return {
        {"table_uuid", std::make_shared<DataTypeString>()},
        {"part_name", std::make_shared<DataTypeString>()},
        {"condition", std::make_shared<DataTypeString>()},
        {"condition_hash", std::make_shared<DataTypeUInt64>()},
        {"entry_size", std::make_shared<DataTypeUInt64>()},
        {"matching_marks", std::make_shared<DataTypeString>()}
    };
}

StorageSystemQueryConditionCache::StorageSystemQueryConditionCache(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id)
{
}

void StorageSystemQueryConditionCache::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    QueryConditionCachePtr query_condition_cache = context->getQueryConditionCache();

    if (!query_condition_cache)
        return;

    std::vector<QueryConditionCache::Cache::KeyMapped> content = query_condition_cache->dump();

    auto to_string = [](const auto & values)
    {
        String str;
        for (auto val : values)
            str += std::to_string(val);
        return str;
    };

    for (const auto & [key, entry] : content)
    {
        res_columns[0]->insert(key.table_id);
        res_columns[1]->insert(key.part_name);
        res_columns[2]->insert(key.condition);
        res_columns[3]->insert(key.condition_hash);
        res_columns[4]->insert(QueryConditionCache::QueryConditionCacheEntryWeight()(*entry));

        std::shared_lock lock(entry->mutex);
        res_columns[5]->insert(toString(to_string(entry->matching_marks)));
    }
}

}
