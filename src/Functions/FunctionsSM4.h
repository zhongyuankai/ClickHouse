#pragma once

#include "config.h"

#if USE_KMS

#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>

#include <kms_operator.h>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ENCRYPT_DECRYPT_ERROR;
}

class FunctionSM4Encrypt : public IFunction
{
public:
    static constexpr auto name = "sm4Encrypt";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSM4Encrypt>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 5; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & type : arguments)
        {
            if (!isStringOrFixedString(type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", type->getName(), getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const ColumnPtr & data_column = arguments[0].column;
        const ColumnPtr & ak_column = arguments[1].column;
        const ColumnPtr & sk_column = arguments[2].column;
        const ColumnPtr & secret_id_column = arguments[3].column;
        const ColumnPtr & secret_version_column = arguments[4].column;

        StringRef ak_str = ak_column->getDataAt(0);
        unsigned char ak[ak_str.size + 1];
        memset(ak, 0x0, ak_str.size + 1);
        memcpy(ak, ak_str.data, ak_str.size);

        StringRef sk_str = sk_column->getDataAt(0);
        unsigned char sk[sk_str.size + 1];
        memset(sk, 0x0, sk_str.size + 1);
        memcpy(sk, sk_str.data, sk_str.size);

        StringRef secret_id_str = secret_id_column->getDataAt(0);
        unsigned char * secret_id = reinterpret_cast<unsigned char *>(const_cast<char *>(secret_id_str.data));

        StringRef secret_version_str = secret_version_column->getDataAt(0);
        unsigned char * secret_version = reinterpret_cast<unsigned char *>(const_cast<char *>(secret_version_str.data));

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef data = data_column->getDataAt(i);
            size_t res_len = static_cast<size_t>((data.size + 29) * 2);
            unsigned char res[res_len];

            int code = sm4_encrypt_v3(ak, sk,
                                      secret_id, secret_id_str.size,
                                      secret_version, secret_version_str.size,
                                      reinterpret_cast<unsigned char *>(const_cast<char *>(data.data)), data.size,
                                      res, &res_len);

            if (code)
                throw Exception(ErrorCodes::ENCRYPT_DECRYPT_ERROR, "Function {} encryption {} failed, code: {}.", getName(), data, code);

            col_res->insert(Field(res, res_len));
        }
        return col_res;
    }
};

class FunctionSM4Decrypt : public IFunction
{
public:
    static constexpr auto name = "sm4Decrypt";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSM4Decrypt>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & type : arguments)
        {
            if (!isStringOrFixedString(type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", type->getName(), getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const ColumnPtr & data_column = arguments[0].column;
        const ColumnPtr & ak_column = arguments[1].column;
        const ColumnPtr & sk_column = arguments[2].column;

        StringRef ak_str = ak_column->getDataAt(0);
        unsigned char ak[ak_str.size + 1];
        memset(ak, 0x0, ak_str.size + 1);
        memcpy(ak, ak_str.data, ak_str.size);

        StringRef sk_str = sk_column->getDataAt(0);
        unsigned char sk[sk_str.size + 1];
        memset(sk, 0x0, sk_str.size + 1);
        memcpy(sk, sk_str.data, sk_str.size);

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef data = data_column->getDataAt(i);
            size_t res_len = static_cast<size_t>((data.size + 29) * 2);
            unsigned char res[res_len];

            int code = sm4_decrypt_v3(ak, sk,
                                      reinterpret_cast<unsigned char *>(const_cast<char *>(data.data)), data.size,
                                      res, &res_len);

            if (code)
                throw Exception(ErrorCodes::ENCRYPT_DECRYPT_ERROR, "Function {} decryption {} failed, code: {}.", getName(), data, code);

            col_res->insert(Field(res, res_len));
        }
        return col_res;
    }
};

}

#endif

