#include <Functions/FunctionDiMd5.h>

#if USE_DIMD5

#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(DiMd5)
{
    factory.registerFunction<FunctionDiMd5>();
}

}

#endif
