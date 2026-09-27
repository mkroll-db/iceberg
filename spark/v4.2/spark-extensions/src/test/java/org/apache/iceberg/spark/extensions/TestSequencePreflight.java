/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSequencePreflight {
  @TempDir private static Path warehouse;
  private static SparkSession spark;

  @BeforeAll
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.preflight", SparkCatalog.class.getName())
            .config("spark.sql.catalog.preflight.type", "hadoop")
            .config("spark.sql.catalog.preflight.warehouse", warehouse.toUri().toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE preflight.probe").collect();
  }

  @AfterAll
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  @Test
  public void omittedNullableMergeInsertGetsNull() {
    spark
        .sql("CREATE TABLE preflight.probe.merge_insert (id BIGINT, data STRING) USING iceberg")
        .collect();
    spark
        .sql(
            "MERGE INTO preflight.probe.merge_insert t "
                + "USING (SELECT 'new' AS data) s ON t.data = s.data "
                + "WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data)")
        .collect();
    Row result = spark.sql("SELECT count(*), count(id) FROM preflight.probe.merge_insert").head();
    assertThat(result.getLong(0)).isEqualTo(1);
    assertThat(result.getLong(1)).isZero();
  }

  @Test
  public void copyOnWriteMergePreservesIdentityValues() {
    spark
        .sql(
            "CREATE TABLE preflight.probe.merge_update (id BIGINT, data STRING) USING iceberg "
                + "TBLPROPERTIES ('write.merge.mode'='copy-on-write')")
        .collect();
    spark
        .sql("INSERT INTO preflight.probe.merge_update VALUES (1, 'old'), (2, 'untouched')")
        .collect();
    spark
        .sql(
            "MERGE INTO preflight.probe.merge_update t "
                + "USING (SELECT 1 AS id, 'updated' AS data) s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET data = s.data")
        .collect();
    List<Row> rows =
        spark.sql("SELECT id, data FROM preflight.probe.merge_update ORDER BY id").collectAsList();
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getLong(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("updated");
    assertThat(rows.get(1).getLong(0)).isEqualTo(2);
    assertThat(rows.get(1).getString(1)).isEqualTo("untouched");
  }

  @Test
  public void planSideExpressionClustersBucketWrites() {
    spark
        .sql(
            "CREATE TABLE preflight.probe.bucketed (id BIGINT, data STRING) USING iceberg "
                + "PARTITIONED BY (bucket(16, id)) "
                + "TBLPROPERTIES ('write.distribution-mode'='hash')")
        .collect();
    spark
        .sql(
            "INSERT INTO preflight.probe.bucketed "
                + "SELECT cast(rand() * 1000000 as BIGINT), cast(id as STRING) "
                + "FROM range(0, 10000, 1, 4)")
        .collect();
    assertThat(spark.sql("SELECT count(*) FROM preflight.probe.bucketed").head().getLong(0))
        .isEqualTo(10000);
    long fileCount =
        spark.sql("SELECT count(*) FROM preflight.probe.bucketed.files").head().getLong(0);
    assertThat(fileCount).isBetween(1L, 16L);
  }

  @Test
  public void identityColumnCannotBeAddedToPartitionOrWriteOrder() {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse.toUri().toString());
    TableIdentifier table = TableIdentifier.of(Namespace.of("probe"), "restrictions");
    Schema schema =
        new Schema(
            Types.NestedField.required("id")
                .withId(1)
                .ofType(Types.LongType.get())
                .withWriteDefaultExpression(
                    Expressions.apply(
                        Expressions.function("next_value_for"),
                        List.<Object>of(UUID.randomUUID().toString(), 1, 1)))
                .build(),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    catalog.createTable(table, schema);

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "ALTER TABLE preflight.probe.restrictions ADD PARTITION FIELD bucket(16, id)")
                    .collect())
        .hasMessageContaining("Per-record default column cannot be partitioned");
    assertThatThrownBy(
            () ->
                spark.sql("ALTER TABLE preflight.probe.restrictions WRITE ORDERED BY id").collect())
        .hasMessageContaining("Per-record default column cannot be sorted");
    assertThat(catalog.loadTable(table).spec().isUnpartitioned()).isTrue();
    assertThat(catalog.loadTable(table).sortOrder().isUnsorted()).isTrue();
  }
}
