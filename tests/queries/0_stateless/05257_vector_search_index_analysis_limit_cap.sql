-- Tags: no-fasttest

-- Tests that `mergeTreeAnalyzeIndexes` rejects a 'vector_search_index_analysis' neighbour count above
-- `max_limit_for_vector_search_queries`, the bound under which a regular vector search query may use the index.
-- Such a count used to be handed to the vector similarity index unchanged, which then reserved memory
-- proportional to it (32 GiB for the count below) instead of to the number of rows in the index.

DROP TABLE IF EXISTS t_vector_index_analysis_limit;

CREATE TABLE t_vector_index_analysis_limit
(
    id UInt32,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO t_vector_index_analysis_limit SELECT number, [number / 100, number / 100] FROM numbers(100);

-- `additional filters present` (the 5th argument) is false and `vector_search_with_rescoring` is left
-- at its default: either one engages a separate, pre-existing bound on the neighbour count, which would
-- hide a missing bound here.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_vector_index_analysis_limit, 1, [],
    'vector_search_index_analysis', array('vec', 'L2Distance', 2147483648, [0.3, 0.3], false, false)); -- { serverError BAD_ARGUMENTS }

-- A neighbour count within the bound is still analyzed, and the index still narrows the part.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_vector_index_analysis_limit, 1, [],
    'vector_search_index_analysis', array('vec', 'L2Distance', 4, [0.3, 0.3], false, false));

-- The bound is the setting, and it is inclusive: the planner uses the index at a LIMIT equal to the
-- setting, so this function must accept exactly that.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_vector_index_analysis_limit, 1, [],
    'vector_search_index_analysis', array('vec', 'L2Distance', 4, [0.3, 0.3], false, false))
SETTINGS max_limit_for_vector_search_queries = 4;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_vector_index_analysis_limit, 1, [],
    'vector_search_index_analysis', array('vec', 'L2Distance', 4, [0.3, 0.3], false, false))
SETTINGS max_limit_for_vector_search_queries = 3; -- { serverError BAD_ARGUMENTS }

-- In a regular vector search query, `hnsw_candidate_list_size_for_search` (no upper bound) and a `LIMIT` under a
-- raised `max_limit_for_vector_search_queries` used to drive the same reservation. Values above the number of
-- rows return the same nearest neighbours. Parallel replicas would move the search into secondary queries.
SELECT id FROM t_vector_index_analysis_limit ORDER BY L2Distance(vec, [0.302, 0.302]) LIMIT 4
SETTINGS hnsw_candidate_list_size_for_search = 2147483648, enable_parallel_replicas = 0, log_comment = '05257_expansion_above_rows';

SELECT count() FROM (SELECT id FROM t_vector_index_analysis_limit ORDER BY L2Distance(vec, [0.302, 0.302]) LIMIT 2147483648)
SETTINGS max_limit_for_vector_search_queries = 2147483648, enable_parallel_replicas = 0, log_comment = '05257_limit_above_rows';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, argMax(ProfileEvents['USearchSearchCount'] > 0 AND memory_usage < 1000000000, event_time_microseconds)
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05257_expansion_above_rows', '05257_limit_above_rows')
    AND type = 'QueryFinish' AND event_date >= yesterday()
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE t_vector_index_analysis_limit;
