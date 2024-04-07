#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_CLUSTER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/**
 * Return the local shard corresponding to the cluster.
 */
class FunctionShard : public IFunction, WithContext
{
public:
    static constexpr auto name = "shard";

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionShard>(context_);
    }

    explicit FunctionShard(ContextPtr context_) : WithContext(context_) {}

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument of function {}.",
                            arguments[0]->getName(),
                            getName()
                            );

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
       const ColumnPtr & column_cluster = arguments[0].column;
       const ColumnConst * col_cluster_const = typeid_cast<const ColumnConst *>(&*column_cluster);

       String cluster_name = col_cluster_const->getValue<String>();

       ClusterPtr cluster = getContext()->getCluster(cluster_name);
       if (cluster == nullptr)
           throw Exception(ErrorCodes::UNKNOWN_CLUSTER,
                           "UnKnown cluster {} of argument of function {}", cluster_name,getName());

       UInt32 shard_num = 0;

       const auto * local_shard_info = cluster->getLocalShardInfo();
       if (local_shard_info != nullptr)
           shard_num = local_shard_info->shard_num;

       ColumnPtr res =  result_type->createColumnConst(input_rows_count, shard_num);

       return res;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }
};

REGISTER_FUNCTION(Shared)
{
    factory.registerFunction<FunctionShard>();
}
}
