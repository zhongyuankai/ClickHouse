#include <Parsers/QueryParseTools.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

const uint UNION  = 1;
const uint SELECT  = 10;
const uint SUB_QUERY  = 100;
const uint JOIN  = 1000;
const int MAX_FILTER_LENGTH  = 50;


String getQueryType(ASTPtr ast)
{
    if (ast->as<ASTInsertQuery>())
        return "INSERT";
    if (ast->as<ASTSelectWithUnionQuery>())
        return "SELECT";
    if (ast->as<ASTDescribeQuery>())
        return "DESCRIBE";
    if (ast->as<ASTShowTablesQuery>() || ast->as<ASTShowCreateTableQuery>() || ast->as<ASTShowCreateDatabaseQuery>())
        return "SHOW";
    if (ast->as<ASTExplainQuery>())
        return "EXPLAIN";
    if (ast->as<ASTAlterQuery>())
        return "ALTER";
    if (ast->as<ASTCreateQuery>())
        return "CREATE";
    return "UNKNOWN";
}

void getAnyDatabaseAndTable(ASTPtr ast, DatabaseAndTables & database_and_tables)
{
    if (ast == nullptr || !database_and_tables.empty())
        return;

    if (auto * show = ast->as<ASTShowTablesQuery>())
    {
        if (show->from != nullptr)
            if (auto * database = show->from->as<ASTIdentifier>())
                database_and_tables[database->name()].insert("");
    }

    if (auto * alter = ast->as<ASTAlterQuery>())
    {
        if (alter->table)
        {
            if (auto * table = alter->table->as<ASTIdentifier>())
            {
                String database_name;
                if (alter->database)
                    if (auto * database = alter->database->as<ASTIdentifier>())
                        database_name = database->name();

                database_and_tables[database_name].insert(table->name());
            }
        }
    }

    if (auto * create = ast->as<ASTCreateQuery>())
    {
        if (create->database || create->table)
        {
            String database_name;
            if (create->database)
                if (auto * database = create->database->as<ASTIdentifier>())
                    database_name = database->name();


            String table_name;
            if (create->table)
                if (auto * table = create->table->as<ASTIdentifier>())
                    table_name = table->name();

            database_and_tables[database_name].insert(table_name);
        }
    }

    if (auto * show_create_table = ast->as<ASTShowCreateTableQuery>())
    {
        if (show_create_table->table)
        {
            if (auto * table = show_create_table->table->as<ASTIdentifier>())
            {
                String database_name;
                if (show_create_table->database)
                    if (auto * database = show_create_table->database->as<ASTIdentifier>())
                        database_name = database->name();

                database_and_tables[database_name].insert(table->name());
            }
        }
    }

    if (auto * show_create_database = ast->as<ASTShowCreateDatabaseQuery>())
    {
        if (show_create_database->database)
            if (auto * database = show_create_database->database->as<ASTIdentifier>())
                database_and_tables[database->name()].insert("");
    }

    if (auto * insert = ast->as<ASTInsertQuery>())
    {
        if (insert->table)
        {
            if (auto * table = insert->table->as<ASTIdentifier>())
            {
                String database_name;
                if (insert->database)
                    if (auto * database = insert->database->as<ASTIdentifier>())
                        database_name = database->name();

                database_and_tables[database_name].insert(table->name());
            }
        }
    }

    if (auto * table_expression = ast->as<ASTTableExpression>())
    {
        if (table_expression->database_and_table_name)
        {
            if (auto * identifier = table_expression->database_and_table_name->as<ASTTableIdentifier>())
            {
                if (identifier->compound())
                    database_and_tables[identifier->getFirstNamePart()].insert(identifier->getSecondNamePart());
                else
                    database_and_tables[""].insert(identifier->name());
            }
        }
    }

    if (auto * desc = ast->as<ASTDescribeQuery>())
        getAnyDatabaseAndTable(desc->table_expression, database_and_tables);

    for (const auto & child : ast->children)
        getAnyDatabaseAndTable(child, database_and_tables);
}

String parseAnyDatabaseAndTable(const String & sql)
{
    String query_type;
    String database;
    String table;
    try
    {
        ParserQuery parser(sql.data() + sql.length());
        ASTPtr ast = parseQuery(parser, sql, "", 0, 0);
        query_type = getQueryType(ast);

        DatabaseAndTables database_and_tables;
        getAnyDatabaseAndTable(ast, database_and_tables);

        if (!database_and_tables.empty())
        {
            database = database_and_tables.begin()->first;
            if (database_and_tables.begin()->second.empty())
                table = "";
            else
                table = *(database_and_tables.begin()->second.begin());
        }
    }
    catch (...)
    {
        return getCurrentExceptionMessage(false, false, false);
    }

    WriteBufferFromOwnString out;
    out << query_type << ";" << database << ";" << table;
    return out.str();
}



String QueryFeature::getID() const
{
    return std::to_string((((structs + tables + columns + order_and_group) << 32) & 0xFFFFFFFF00000000) + (filters & 0xFFFFFFFF));
}

void cumulativeFilters(QueryFeature & feature, ASTPtr ast)
{
    WriteBufferFromOwnString out;
    IAST::FormatSettings settings(out, true);
    IAST::FormatState state;
    ast->formatImpl(settings, state, IAST::FormatStateStacked());
    String & expression = out.str();

    IdentifierNameVector vector;
    ast->collectAllIdentifierNames(vector);

    int identifier_sum = 0;
    int identifier_length = 1;  /// avoid divide zero
    for (const auto & identifier : vector)
    {
        identifier_length += identifier.length();
        for (auto n : identifier)
            identifier_sum += static_cast<int>(n);
    }

    int expression_length = expression.length() > MAX_FILTER_LENGTH ? MAX_FILTER_LENGTH : static_cast<int>(expression.length());
    /// Increase identifier sum weight
    feature.filters += identifier_sum * expression_length / identifier_length * 1000;
    for (int i = 0; i < expression_length; ++i)
        feature.filters += static_cast<UInt64>(expression.at(i));
}


void getStatementFeature(ASTPtr ast, QueryFeature & feature) {
    if (ast == nullptr)
        return;

    /// acquire statement structs.
    if (ast->as<ASTSelectWithUnionQuery>())
        feature.structs += UNION;

    if (ast->as<ASTSubquery>())
        feature.structs += SUB_QUERY;

    if (ast->as<ASTTableJoin>())
        feature.structs += JOIN;

    if (auto * select_query = ast->as<ASTSelectQuery>())
    {
        feature.structs += SELECT;
        /// acquire select part
        if (auto select = select_query->select())
        {
            for (const auto & child : select->children)
                feature.columns += child->getTreeHash().low64;
        }

        /// acquire group by
        if (auto group_by = select_query->groupBy())
        {
            for (const auto & child : group_by->children)
                feature.order_and_group += child->getTreeHash().low64;
        }

        /// acquire where clause
        if (auto where = select_query->where())
        {
            /// ignore OR clause
            if (where->getID() != "Function_or")
            {
                if (where->getID() == "Function_and")
                {
                    for (auto expression : where->children[0]->children)
                        cumulativeFilters(feature, expression);
                }
                else
                    cumulativeFilters(feature, where);
            }
        }
    }

    /// acquire oder by
    if (ast->as<ASTOrderByElement>())
        feature.order_and_group += ast->getTreeHash().low64;

    /// acquire table name
    if (auto * table_expression = ast->as<ASTTableExpression>())
    {
        if (table_expression->database_and_table_name)
            if (auto node = table_expression->database_and_table_name)
                feature.tables += node->getTreeHash().low64;
    }

    for (const auto & child : ast->children)
        getStatementFeature(child, feature);
}


String outputQueryClassId(const String & sql) {
    try
    {
        ParserQuery parser(sql.data() + sql.length());
        ASTPtr ast = parseQuery(parser, sql, "", 0, 0);

        QueryFeature feature{0, 0, 0, 0, 0};
        getStatementFeature(ast, feature);
        return feature.getID();
    }
    catch (...)
    {
        return getCurrentExceptionMessage(false, false, false);
    }
}


}
