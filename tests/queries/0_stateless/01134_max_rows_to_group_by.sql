SET max_block_size = 1;
SET max_rows_to_group_by = 10;
SET group_by_overflow_mode = 'throw';

-- Settings 'max_rows_to_group_by' and 'max_bytes_before_external_group_by' are mutually exclusive.
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT 'test1', number FROM system.numbers GROUP BY number; -- { serverError TOO_MANY_ROWS }

SET group_by_overflow_mode = 'break';
SELECT 'test2', number FROM system.numbers GROUP BY number ORDER BY number;

SET max_rows_to_read = 500;
SELECT 'test3', number FROM system.numbers GROUP BY number ORDER BY number;

SET group_by_overflow_mode = 'any';
SELECT 'test4', number FROM numbers(1000) GROUP BY number ORDER BY number; -- { serverError TOO_MANY_ROWS }

SET max_rows_to_read = 1000;
SELECT 'test5', number FROM numbers(1000) GROUP BY number ORDER BY number;
