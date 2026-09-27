# Interactive sequence demo

Run this from the `iceberg/` directory. The demo starts a temporary REST catalog,
creates `demo.identity.items` with a required `BIGINT` identity column, and opens a
Spark SQL prompt. The server binds to loopback on a free port. Its catalog and counters
are discarded when the process exits.

```bash
./gradlew :iceberg-spark:iceberg-spark-4.2_2.13:sequenceDemo --quiet --console=plain
```

The default uses Spark `local[2]`. To exercise the executor-to-driver plugin hop with
two separate executor JVMs, set `SPARK_HOME` to a Spark 4.2 distribution and run:

```bash
SPARK_HOME=/path/to/spark-4.2.0 SPARK_SCALA_VERSION=2.13 \
  ./gradlew :iceberg-spark:iceberg-spark-4.2_2.13:sequenceDemo \
  --args='--cluster' --quiet --console=plain
```

The current workspace also has a temporary Spark home at `/tmp/spark-prototype-home`.
It was assembled from the Gradle test classpath, so it is useful for this checkout
but is not part of the repository.

At the `sql>` prompt, end SQL with `;`. A blank line also runs a pending multiline
statement. Results show at most 20 rows. `:cancel` clears a pending statement and
`:quit` stops Spark and the REST server.

```sql
INSERT INTO demo.identity.items (data) VALUES ('first'), ('second');
SELECT id, data FROM demo.identity.items ORDER BY id;
SELECT count(*), count(DISTINCT id), count(*) - count(id)
FROM demo.identity.items;
```

Type `:concurrent` to submit four inserts of 100 rows each, then print the total,
distinct ID count, and null ID count. Repeating it adds another 400 rows. This command
uses the same concurrent write path as
[`TestSequenceInserts`](../spark/v4.2/spark/src/test/java/org/apache/iceberg/spark/TestSequenceInserts.java).

To see the explicit-value guard, run:

```sql
INSERT INTO demo.identity.items (id, data) VALUES (5, 'explicit');
```

That command should fail without ending the prompt. The expression default is created
through the Iceberg Java catalog API, because this prototype does not add SQL DDL
syntax for it. MERGE and durable counters are outside the prototype guarantee. See
[`PROTOTYPE_SPIKE_RESULTS.md`](PROTOTYPE_SPIKE_RESULTS.md) for the tested behavior and
limits. Spark SQL syntax is documented in the
[Spark SQL reference](https://spark.apache.org/docs/latest/sql-ref.html).
