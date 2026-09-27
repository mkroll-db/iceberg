# Per-record default prototype results

Date: 2026-09-26. Target: Iceberg with Spark 4.2.

The plan refers to `tmp/iceberg/IMPLEMENTATION_GRILL.md`, which is absent from this
checkout. The three preflight outcomes are recorded here instead. All were verified
by `TestSequencePreflight` with ordinary Spark SQL, before the new default
expression is involved:

| Question | Result | Evidence |
|---|---|---|
| Q29: omitted nullable column in a MERGE insert | Pass, `id` was NULL | `omittedNullableMergeInsertGetsNull` |
| Q30: plan-side value reaches `bucket(16, id)` clustering | Pass, 10,000 rows produced at most 16 files | `planSideExpressionClustersBucketWrites` |
| Q22: copy-on-write MERGE preserves existing IDs | Pass, updated and untouched rows retained IDs 1 and 2 | `copyOnWriteMergePreservesIdentityValues` |

The [preflight test](../spark/v4.2/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestSequencePreflight.java)
is executable with:

```bash
./gradlew :iceberg-spark:iceberg-spark-extensions-4.2_2.13:test \
  --tests org.apache.iceberg.spark.extensions.TestSequencePreflight
```

The REST reservation proof is `TestSequenceReservations`. It checked eight concurrent
clients, contiguous disjoint ranges, rejected parameter drift, table UUID isolation,
negative steps, and abandoned or overlapping task attempts.

The Spark proof is `TestSequenceInserts`. It ran on `local-cluster[2,1,1024]` with two
executor JVMs. Four concurrent `INSERT INTO` statements committed 400 rows with 400
distinct non-null IDs. An explicit ID was rejected. An injected task failure left a
gap, and the following insert did not reuse any value. Running this test requires a
Spark 4.2 distribution in `SPARK_HOME` for local-cluster executor launch:

```bash
SPARK_HOME=/path/to/spark-4.2.0 SPARK_SCALA_VERSION=2.13 \
  ./gradlew :iceberg-spark:iceberg-spark-4.2_2.13:test \
  --tests org.apache.iceberg.spark.TestSequenceInserts
```

The implementation keeps counters in the REST test server's memory. Restarting that
server can reissue values. Spark rejects ALTER commands that add a sequence column to
the partition spec or write order, and the writer checks both again for tables created
through the API. Writer-side values are invisible to Spark's distribution and ordering.
Row-level rewrite paths reject sequence tables. Spark may lower an insert-only MERGE
to append, but MERGE remains outside the prototype guarantee. These are the intended
limits until counters become durable and the expression moves into Spark's plan.

Upstream references: [Spark plugin API](https://spark.apache.org/docs/latest/api/java/org/apache/spark/api/plugin/SparkPlugin.html)
and [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/).
