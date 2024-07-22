set(SRCS)

add_executable(sql_parser SQLParser.cpp ${SRCS})
target_link_libraries(sql_parser PRIVATE clickhouse_parsers)
target_link_libraries(sql_parser PRIVATE dbms)

add_executable(sql_classifier SQLClassifier.cpp ${SRCS})
target_link_libraries(sql_classifier PRIVATE clickhouse_parsers)
target_link_libraries(sql_classifier PRIVATE dbms)
