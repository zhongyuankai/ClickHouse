#pragma once

#include <Parsers/IAST.h>

namespace DB
{

String getQueryType(ASTPtr ast);

using DatabaseAndTables = std::map<String, std::set<String>>;
void getAnyDatabaseAndTable(ASTPtr ast, DatabaseAndTables & database_and_tables);

/// return `query_type;database;table`
String parseAnyDatabaseAndTable(const String & sql);

struct QueryFeature
{
    UInt64 structs;     /// select, join, etc.
    UInt64 tables;      /// table name
    UInt64 columns;
    UInt64 order_and_group;
    UInt64 filters;

    String getID() const;
};

void getStatementFeature(ASTPtr ast, QueryFeature & feature);

String outputQueryClassId(const String & sql);

}
