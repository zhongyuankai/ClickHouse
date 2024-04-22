#include <Functions/FunctionsSM4.h>

#if USE_KMS

#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(SM4)
{
    factory.registerFunction<FunctionSM4Encrypt>();
    factory.registerFunction<FunctionSM4Decrypt>();
}

}

#endif

