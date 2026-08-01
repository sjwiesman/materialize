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
# No absolute score is asserted. Scores are computed per batch and the BM25
# arrangement is exchanged by key hash, so a document's score depends on which
# other documents share its worker. The one ranking assertion therefore runs on
# a single-worker cluster, where every document shares one corpus. Everywhere
# else a score is only asserted to be positive.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_bm25_index = true;

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

# A doc matching two query terms outranks one matching a single term. This
# needs every doc scored against one corpus, so it runs on a single-worker
# cluster with its own BM25 index. `LIMIT 1` is what makes the assertion bite:
# the limit is applied server-side after the ordering, so only the top-ranked
# row crosses the wire. Without it testdrive would sort the result and accept
# either order.
> CREATE CLUSTER bm25_rank SIZE 'scale=1,workers=1'

> CREATE INDEX items_rank IN CLUSTER bm25_rank ON items (description) WITH (BM25)

> SET cluster = bm25_rank

> SELECT id FROM items WHERE description @@@ 'running shoes' ORDER BY mz_score() DESC LIMIT 1
1

> SELECT id FROM items WHERE description @@@ 'running shoes' ORDER BY mz_score() ASC LIMIT 1
2

> SET cluster = quickstart

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

# A BM25 index and a plain index cannot share a key on one cluster. Rendering
# would hand the BM25 index the plain index's arrangement, which has no BM25
# counterpart.
> CREATE INDEX plain_body ON plain (body)

! CREATE INDEX plain_body_search ON plain (body) WITH (BM25)
contains:a BM25 index cannot share its key with a plain index on the same cluster

# The rejection is symmetric. Allowing this order would let a catalog state
# exist that re-planning the BM25 index rejects, which is unbootable.
! CREATE INDEX items_description ON items (description)
contains:a BM25 index cannot share its key with a plain index on the same cluster

# Only the cluster the index lands on matters, so the same pair on separate
# clusters is fine.
> CREATE INDEX plain_body_search IN CLUSTER bm25_rank ON plain (body) WITH (BM25)

# A recursive target is rejected before the catalog commits. Index export in an
# iterative scope has no BM25 counterpart.
> CREATE VIEW recursive_docs AS
  WITH MUTUALLY RECURSIVE
    padded (body text) AS (
      SELECT description FROM items WHERE description IS NOT NULL
      UNION
      SELECT body || ' more' FROM padded WHERE length(body) < 40
    )
  SELECT body FROM padded

! CREATE INDEX recursive_docs_search ON recursive_docs (body) WITH (BM25)
contains:a BM25 index on a recursive view not yet supported

# Merged batches carry no embedded inverted index, so the peek rebuilds one
# from the batch's texts. Each INSERT below commits at its own timestamp, which
# gives the spine enough batches to merge before the searches run.
> CREATE TABLE docs (id int, body text)

> CREATE INDEX docs_search ON docs (body) WITH (BM25)

> INSERT INTO docs VALUES (1, 'common alpha')

> INSERT INTO docs VALUES (2, 'common bravo')

> INSERT INTO docs VALUES (3, 'common charlie quokka')

> INSERT INTO docs VALUES (4, 'common delta')

> INSERT INTO docs VALUES (5, 'common echo')

> INSERT INTO docs VALUES (6, 'common foxtrot')

> INSERT INTO docs VALUES (7, 'common golf')

> INSERT INTO docs VALUES (8, 'common hotel')

> INSERT INTO docs VALUES (9, 'common india')

> INSERT INTO docs VALUES (10, 'common juliett')

> INSERT INTO docs VALUES (11, 'common kilo quokka')

> INSERT INTO docs VALUES (12, 'common lima')

> INSERT INTO docs VALUES (13, 'common mike')

> INSERT INTO docs VALUES (14, 'common november')

> INSERT INTO docs VALUES (15, 'common oscar')

> INSERT INTO docs VALUES (16, 'common papa')

> INSERT INTO docs VALUES (17, 'common quebec')

> INSERT INTO docs VALUES (18, 'common romeo')

> INSERT INTO docs VALUES (19, 'common sierra quokka')

> INSERT INTO docs VALUES (20, 'common tango')

> SELECT id FROM docs WHERE body @@@ 'quokka' ORDER BY id
3
11
19

> SELECT id, mz_score() > 0 FROM docs WHERE body @@@ 'kilo'
11 true

# Every document matches, so this reads across all of the spine's batches.
> SELECT id FROM docs WHERE body @@@ 'common' ORDER BY id
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20

# A retraction of a row indexed in an earlier batch is reflected after merges.
> DELETE FROM docs WHERE id = 11

> SELECT id FROM docs WHERE body @@@ 'quokka' ORDER BY id
3
19

# The feature flag gates index creation.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_bm25_index = false;

! CREATE INDEX docs_search_off ON docs (body) WITH (BM25)
contains:CREATE INDEX ... WITH (BM25) is not available

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_bm25_index = true;

# The Lucene style query language. Every assertion below is on the matched set,
# not on scores, so it holds no matter how the rows hash across workers.

> CREATE TABLE docs2 (id int, body text)

# Row 5 is what makes the grouping assertion below bite. It has the phrase
# "real time" and no "database", so it matches `streaming OR "real time"` while
# failing the `database` conjunct. Grouped and unparenthesized readings of that
# query therefore return different rows.
> INSERT INTO docs2 VALUES
  (1, 'streaming database for real time applications'),
  (2, 'batch data warehouse'),
  (3, 'real slow time series database'),
  (4, 'incremental view maintenance'),
  (5, 'real time weather feed')

> CREATE INDEX docs2_search ON docs2 (body) WITH (BM25)

# AND narrows, OR widens, NOT excludes.
> SELECT id FROM docs2 WHERE body @@@ 'database AND streaming'
1

> SELECT id FROM docs2 WHERE body @@@ 'warehouse OR maintenance' ORDER BY id
2
4

> SELECT id FROM docs2 WHERE body @@@ 'database NOT streaming'
3

# A phrase requires the terms adjacent and in order. The AND control shows row 3
# contains both words too, and only rows 1 and 5 have them adjacent.
> SELECT id FROM docs2 WHERE body @@@ 'real AND time' ORDER BY id
1
3
5

> SELECT id FROM docs2 WHERE body @@@ '"real time"' ORDER BY id
1
5

> SELECT id FROM docs2 WHERE body @@@ '"time real"'

# Fuzzy matching reaches a misspelling one edit away. The same word searched
# exactly matches nothing, which is what makes the fuzzy result meaningful.
> SELECT id FROM docs2 WHERE body @@@ 'databse~1' ORDER BY id
1
3

> SELECT id FROM docs2 WHERE body @@@ 'databse'

# Prefix expansion.
> SELECT id FROM docs2 WHERE body @@@ 'incremen*'
4

# Grouping composes the operators. Parentheses make the disjunction a conjunct
# of `database`, so row 5 is out. Dropping them lets AND bind tighter, leaving
# `(database AND streaming) OR "real time"`, which row 5 satisfies.
> SELECT id FROM docs2 WHERE body @@@ 'database AND (streaming OR "real time")'
1

> SELECT id FROM docs2 WHERE body @@@ 'database AND streaming OR "real time"' ORDER BY id
1
5

# Stage one bag-of-words semantics are unchanged: bare adjacency is OR.
> SELECT id FROM docs2 WHERE body @@@ 'warehouse streaming' ORDER BY id
1
2

# Invalid queries are rejected at planning time.

# A query with no positive clause would exclude from nothing.
! SELECT id FROM docs2 WHERE body @@@ 'NOT database'
contains:invalid search query

! SELECT id FROM docs2 WHERE body @@@ '(database'
contains:invalid search query

# Edit distances above 2 are rejected rather than scanning the dictionary.
! SELECT id FROM docs2 WHERE body @@@ 'database~9'
contains:invalid search query

# BM25 indexes on plain views. The view has no arrangement of its own, so the
# index is the only thing that keeps its `Get` alive through import and serves
# both search and non-search reads of it.

> CREATE TABLE stories (id int, headline text, summary text)

> INSERT INTO stories VALUES
  (1, 'Streaming joins land', 'incremental joins over streaming inputs are now supported'),
  (2, 'Postgres sources faster', 'logical replication snapshots complete in half the time'),
  (3, 'New dashboard templates', 'prebuilt dashboards for common monitoring needs')

> CREATE VIEW story_docs AS
  SELECT id, headline, headline || '. ' || summary AS doc FROM stories

> CREATE INDEX story_search ON story_docs (doc) WITH (BM25)

> SELECT id FROM story_docs WHERE doc @@@ 'streaming'
1

> SELECT id FROM story_docs WHERE doc @@@ 'replicat* AND postgres'
2

> SELECT id, mz_score() > 0 FROM story_docs WHERE doc @@@ '"logical replication"'
2 true

# The search plans as a BM25 peek against the view's index.
? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT id FROM story_docs WHERE doc @@@ 'streaming'
Explained Query (fast path):
  Project (#0)
    Bm25Peek materialize.public.story_docs (using materialize.public.story_search) query="streaming"

Used Indexes:
  - materialize.public.story_search (*** full scan ***)

Target cluster: quickstart

# A non-search read of the view is served by the same index's standard
# arrangement rather than a transient dataflow.
? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT id, headline FROM story_docs
Explained Query (fast path):
  Project (#1, #2)
    ReadIndex on=materialize.public.story_docs story_search=[*** full scan ***]

Used Indexes:
  - materialize.public.story_search (*** full scan ***)

Target cluster: quickstart

> SELECT id, headline FROM story_docs ORDER BY id
1 "Streaming joins land"
2 "Postgres sources faster"
3 "New dashboard templates"

# A second BM25 index on the same key and cluster shares arrangements with the
# first instead of colliding with it.
> CREATE INDEX story_search_dup ON story_docs (doc) WITH (BM25)

> SELECT id FROM story_docs WHERE doc @@@ 'streaming'
1

# Dropping the first leaves search working through the second. Because the
# second reuses the first's arrangement, the drop only removes the first from
# the catalog. Its arrangement stays maintained until the duplicate goes too.
> DROP INDEX story_search

> SELECT id FROM story_docs WHERE doc @@@ 'streaming'
1

# Updates to the table still flow through the view into the index.
> DELETE FROM stories WHERE id = 1

> SELECT id FROM story_docs WHERE doc @@@ 'streaming'
