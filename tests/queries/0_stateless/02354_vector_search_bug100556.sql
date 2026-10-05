-- Tags: no-fasttest, no-ordinary-database

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/100556
-- A huge neighbor count caused ceil2() overflow in usearch reserve(), leading to
-- heap-buffer-overflow in sorted_buffer_gt::insert(). The count is now clamped to the
-- number of vectors in the index, so all rows are returned.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

-- max_limit_for_vector_search_queries must be >= LIMIT for the vector index to be used (default is 1000).
WITH [0.0, 2.0] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec)
LIMIT 9223372036854775807
SETTINGS max_limit_for_vector_search_queries = 9223372036854775807;

DROP TABLE tab;
