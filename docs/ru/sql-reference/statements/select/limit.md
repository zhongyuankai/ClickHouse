---
slug: /ru/sql-reference/statements/select/limit
sidebar_label: LIMIT
---

# Секция LIMIT {#limit-clause}

`LIMIT m` позволяет выбрать из результата первые `m` строк.

`LIMIT n, m` позволяет выбрать из результата первые `m` строк после пропуска первых `n` строк. Синтаксис `LIMIT m OFFSET n` также поддерживается.

`n` и `m` должны быть неотрицательными целыми числами.

При отсутствии секции [ORDER BY](order-by.md), однозначно сортирующей результат, результат может быть произвольным и может являться недетерминированным.

:::note Примечание
Количество возвращаемых строк может зависеть также от настройки [limit](../../../operations/settings/settings.md#limit).
:::
## Модификатор LIMIT ... WITH TIES  {#limit-with-ties}

Когда вы установите модификатор WITH TIES для `LIMIT n[,m]` и указываете `ORDER BY expr_list`, вы получите первые `n` или `n,m` строк и дополнительно все строки с теми же самым значениями полей указанных в `ORDER BY` равными строке на позиции `n` для `LIMIT n` или `m` для `LIMIT n,m`.

Этот модификатор также может быть скомбинирован с [ORDER BY ... WITH FILL модификатором](/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier)

Для примера следующий запрос:
```sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5
```

возвращает
```text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

но после применения модификатора `WITH TIES`
```sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5 WITH TIES
```

возвращает другой набор строк
```text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```
поскольку строка на позиции 6 имеет тоже самое значение "2" для поля `n` что и строка на позиции 5
