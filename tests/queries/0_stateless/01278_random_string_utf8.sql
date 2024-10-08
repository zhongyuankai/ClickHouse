-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

SELECT randomStringUTF8('string'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT lengthUTF8(randomStringUTF8(100));
SELECT toTypeName(randomStringUTF8(10));
SELECT isValidUTF8(randomStringUTF8(100000));
SELECT randomStringUTF8(0);
SELECT lengthUTF8(lowerUTF8(randomStringUTF8(99))); -- bug #49672: msan assert
