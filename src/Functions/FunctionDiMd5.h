#pragma once

#include "config.h"

#if USE_DIMD5
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <DiMd5.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int DIMD5_MISMATCH;
}

class FunctionDiMd5 : public IFunction
{
public:
    static constexpr auto name = "diMd5";
    static constexpr auto md5_length = 32;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDiMd5>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & type = arguments[0];
        if (!isStringOrFixedString(type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", type->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;
        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef data = column->getDataAt(i);
            /// The result length is all 32, one character is reserved to store '\0'.
            char res[md5_length + 1];

            DiMd5Hash(res, sizeof(res), data.data, static_cast<unsigned int>(data.size));
            if (strcmp(res, "") == 0 || strlen(res) != md5_length)
                throw Exception(ErrorCodes::DIMD5_MISMATCH, "Function {} execute {} failed.", getName(), data);

            col_res->insert(Field(res, md5_length));
        }
        return col_res;
    }
};

}
#endif

