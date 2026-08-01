# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# BM25 full-text search: `CREATE INDEX ... WITH (BM25)` plus the `@@@` match
# operator and `mz_score()`, served by the BM25 peek fast path.
#
# Scores are computed per batch, so only relative orderings that hold within a
# single batch are asserted, never absolute score values.

> CREATE TABLE items (id int, description text)

> INSERT INTO items VALUES
  (1, 'red running shoes'),
  (2, 'blue walking shoes'),
  (3, 'green summer hat'),
  (4, NULL)

> CREATE INDEX items_search ON items (description) WITH (BM25)

# Bag-of-words match: both shoe rows match, the hat row and the NULL row do not.
> SELECT id FROM items WHERE description @@@ 'shoes' ORDER BY id
1
2

# Tokenization is case insensitive and splits on non-alphanumerics.
> SELECT id FROM items WHERE description @@@ 'SHOES!' ORDER BY id
1
2

# A doc matching two query terms outranks one matching a single term.
> SELECT id FROM items WHERE description @@@ 'running shoes' ORDER BY mz_score() DESC
1
2

# Scores are positive for every match.
> SELECT id, mz_score() > 0 FROM items WHERE description @@@ 'hat'
3 true

# A term in no document matches nothing.
> SELECT id FROM items WHERE description @@@ 'sandals'

# Predicates on other columns compose with the search.
> SELECT id FROM items WHERE description @@@ 'shoes' AND id > 1
2

# EXPLAIN routes the query through the BM25 peek fast path.
? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT id FROM items WHERE description @@@ 'shoes'
Explained Query (fast path):
  Project (#0)
    Bm25Peek materialize.public.items (using materialize.public.items_search) query="shoes"

Used Indexes:
  - materialize.public.items_search (*** full scan ***)

Target cluster: quickstart

# Updates are reflected at peek time.
> UPDATE items SET description = 'green winter hat' WHERE id = 3

> SELECT id FROM items WHERE description @@@ 'summer'

> SELECT id FROM items WHERE description @@@ 'winter'
3

# Deletes are reflected at peek time.
> DELETE FROM items WHERE id = 1

> SELECT id FROM items WHERE description @@@ 'shoes'
2

# `@@@` on a column with no BM25 index is rejected at planning time.
> CREATE TABLE plain (body text)

! SELECT body FROM plain WHERE body @@@ 'shoes'
contains:full-text search without a matching BM25 index not yet supported

# `mz_score()` without a search filter is rejected at planning time.
! SELECT mz_score() FROM items
contains:cannot call mz_score
