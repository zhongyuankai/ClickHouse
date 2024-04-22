-- Tags: disabled

SELECT 'Test sm4Encrypt and sm4Decrypt';
SELECT sm4Encrypt('1234567812345678', 'ce439138e2b96e5f6c9eca0a864f0097','17e82b972fd2b54a900d9a29d285b7c1','534543524554-677a3031-adf130de0d99000','a6cdbdd5981a018b');
SELECT sm4Decrypt('ur8FCt8TDeDZkAAAAX8+W2ToNnmo5nqIee6LOwQcNQezmRbucnbeaUvHiE8R', 'ce439138e2b96e5f6c9eca0a864f0097','17e82b972fd2b54a900d9a29d285b7c1');

SELECT query FROM system.query_log WHERE current_database = currentDatabase() AND (query LIKE '%SELECT sm4Encrypt(%' OR query LIKE '%SELECT sm4Decrypt(%') ORDER BY query;

DROP TABLE IF EXISTS decrypt_tb;
CREATE TABLE decrypt_tb(original String, cipher String) ENGINE = MergeTree() ORDER BY original;
INSERT INTO decrypt_tb
SELECT concat('1234567812345678_abc_', toString(number)) original,
       sm4Encrypt(original, 'ce439138e2b96e5f6c9eca0a864f0097','17e82b972fd2b54a900d9a29d285b7c1','534543524554-677a3031-adf130de0d99000','a6cdbdd5981a018b') cipher
FROM numbers(100000);

SELECT countIf(original == sm4Decrypt(cipher, 'ce439138e2b96e5f6c9eca0a864f0097', '17e82b972fd2b54a900d9a29d285b7c1')) FROM decrypt_tb;

SELECT original, sm4Decrypt(cipher, 'ce439138e2b96e5f6c9eca0a864f0097', '17e82b972fd2b54a900d9a29d285b7c1') FROM decrypt_tb ORDER BY original LIMIT 10;
