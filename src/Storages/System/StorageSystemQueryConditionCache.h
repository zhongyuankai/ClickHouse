#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include "Core/NamesAndTypes.h"

namespace DB
{

class StorageSystemQueryConditionCache final : public IStorageSystemOneBlock<StorageSystemQueryConditionCache>
{
public:
    explicit StorageSystemQueryConditionCache(const StorageID & table_id_);

    std::string getName() const override { return "SystemQueryConditionCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
