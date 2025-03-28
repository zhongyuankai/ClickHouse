---
description: 'Calculates the value of `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) -
  log(P(tag = 0)))` for each category.'
sidebar_position: 115
slug: /sql-reference/aggregate-functions/reference/categoricalinformationvalue
title: 'categoricalInformationValue'
---

Calculates the value of `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` for each category.

```sql
categoricalInformationValue(category1, category2, ..., tag)
```

The result indicates how a discrete (categorical) feature `[category1, category2, ...]` contribute to a learning model which predicting the value of `tag`.
