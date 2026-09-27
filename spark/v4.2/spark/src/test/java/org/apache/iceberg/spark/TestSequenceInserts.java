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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestSequenceInserts {
  @RegisterExtension
  private static final RESTServerExtension SERVER =
      new RESTServerExtension(
          Map.of(
              RESTCatalogServer.REST_PORT,
              RESTServerExtension.FREE_PORT,
              CatalogProperties.CLIENT_POOL_SIZE,
              "1"));

  private static SparkSession spark;

  @BeforeAll
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local-cluster[2,1,1024]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.ui.enabled", "false")
            .config("spark.task.maxFailures", "1")
            .config("spark.executor.extraClassPath", System.getProperty("java.class.path"))
            .config("spark.plugins", SequenceSparkPlugin.class.getName())
            .config("spark.sql.catalog.testrest", SparkCatalog.class.getName())
            .config("spark.sql.catalog.testrest.type", "rest")
            .config("spark.sql.catalog.testrest.cache-enabled", "false")
            .config(
                "spark.sql.catalog.testrest.uri",
                SERVER.client().properties().get(CatalogProperties.URI))
            .getOrCreate();
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
  public void concurrentOmittedIdentityInsertsAreUnique() throws Exception {
    String sqlTable = createIdentityTable();

    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Callable<Void>> inserts = new ArrayList<>();
      for (int writer = 0; writer < 4; writer++) {
        int writerId = writer;
        inserts.add(
            () -> {
              spark
                  .sql(
                      "INSERT INTO "
                          + sqlTable
                          + " (data) SELECT concat('writer-"
                          + writerId
                          + "-', cast(id as string)) FROM range(100)")
                  .collect();
              return null;
            });
      }
      for (Future<Void> result : executor.invokeAll(inserts)) {
        result.get();
      }
    } finally {
      executor.shutdownNow();
    }

    Row counts =
        spark
            .sql("SELECT count(*), count(DISTINCT id), count(*) - count(id) FROM " + sqlTable)
            .head();
    assertThat(counts.getLong(0)).isEqualTo(400L);
    assertThat(counts.getLong(1)).isEqualTo(400L);
    assertThat(counts.getLong(2)).isZero();

    assertThatThrownBy(
            () ->
                spark
                    .sql("INSERT INTO " + sqlTable + " (id, data) VALUES (5, 'explicit')")
                    .collect())
        .hasMessageContaining("Explicit values are not allowed");
  }

  @Test
  public void failedTaskLeavesGapWithoutReissuingValues() {
    String sqlTable = createIdentityTable();
    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT INTO "
                            + sqlTable
                            + " (data) SELECT CASE WHEN id = 50 THEN raise_error('injected failure') "
                            + "ELSE cast(id as string) END FROM range(0, 100, 1, 1)")
                    .collect())
        .hasMessageContaining("injected failure");
    assertThat(spark.sql("SELECT count(*) FROM " + sqlTable).head().getLong(0)).isZero();

    spark
        .sql("INSERT INTO " + sqlTable + " (data) SELECT cast(id as string) FROM range(10)")
        .collect();
    Row values = spark.sql("SELECT count(*), count(DISTINCT id), min(id) FROM " + sqlTable).head();
    assertThat(values.getLong(0)).isEqualTo(10);
    assertThat(values.getLong(1)).isEqualTo(10);
    assertThat(values.getLong(2)).isGreaterThan(1);
  }

  @Test
  public void sequenceColumnCannotBePartitionedOrSorted() {
    RESTCatalog catalog = SERVER.client();
    Namespace namespace =
        Namespace.of("restricted_" + UUID.randomUUID().toString().replace('-', '_'));
    catalog.createNamespace(namespace);
    Schema schema = identitySchema(UUID.randomUUID().toString());
    TableIdentifier partitioned = TableIdentifier.of(namespace, "partitioned");
    catalog
        .buildTable(partitioned, schema)
        .withPartitionSpec(PartitionSpec.builderFor(schema).bucket("id", 16).build())
        .create();
    TableIdentifier sorted = TableIdentifier.of(namespace, "sorted");
    catalog
        .buildTable(sorted, schema)
        .withSortOrder(SortOrder.builderFor(schema).asc("id").build())
        .create();

    assertThatThrownBy(
            () ->
                spark
                    .sql(
                        "INSERT INTO testrest."
                            + namespace
                            + ".partitioned (data) VALUES ('partitioned')")
                    .collect())
        .hasMessageContaining("Per-record default column cannot be partitioned");
    assertThatThrownBy(
            () ->
                spark
                    .sql("INSERT INTO testrest." + namespace + ".sorted (data) VALUES ('sorted')")
                    .collect())
        .hasMessageContaining("Per-record default column cannot be sorted");
  }

  @Test
  public void omittedUuidAndCurrentTimeNsAreGeneratedPerRow() {
    String sqlTable = createDefaultTable(defaultsSchema());
    long before = DateTimeUtil.nanosFromTimestamptz(OffsetDateTime.now(ZoneOffset.UTC));
    spark
        .sql("INSERT INTO " + sqlTable + " (data) SELECT cast(id as string) FROM range(20)")
        .collect();
    long after = DateTimeUtil.nanosFromTimestamptz(OffsetDateTime.now(ZoneOffset.UTC));

    List<Row> rows =
        spark.sql("SELECT record_uuid, created_at_ns FROM " + sqlTable).collectAsList();
    assertThat(rows).hasSize(20);
    assertThat(rows.stream().map(row -> UUID.fromString(row.getString(0)))).doesNotHaveDuplicates();
    assertThat(rows).allSatisfy(row -> assertThat(row.getLong(1)).isBetween(before, after));
  }

  @Test
  public void explicitUuidAndCurrentTimeNsArePreserved() {
    String sqlTable = createDefaultTable(defaultsSchema());
    UUID expectedUuid = UUID.randomUUID();
    long expectedTime = DateTimeUtil.nanosFromTimestamptz(OffsetDateTime.now(ZoneOffset.UTC));
    spark
        .sql(
            "INSERT INTO "
                + sqlTable
                + " (record_uuid, created_at_ns, data) VALUES ('"
                + expectedUuid
                + "', "
                + expectedTime
                + ", 'explicit')")
        .collect();

    Row row = spark.sql("SELECT record_uuid, created_at_ns FROM " + sqlTable).head();
    assertThat(row.getString(0)).isEqualTo(expectedUuid.toString());
    assertThat(row.getLong(1)).isEqualTo(expectedTime);
  }

  @Test
  public void uuidAndCurrentTimeNsValidateTypesAndArguments() {
    assertInvalidDefault("uuid", Types.StringType.get(), List.of(), "uuid column");
    assertInvalidDefault("uuid", Types.UUIDType.get(), List.of(1), "uuid arguments");
    assertInvalidDefault("current_time_ns", Types.StringType.get(), List.of(), "long column");
    assertInvalidDefault(
        "current_time_ns", Types.LongType.get(), List.of(1), "current_time_ns arguments");
  }

  private static void assertInvalidDefault(
      String function, Type type, List<Object> arguments, String message) {
    Schema schema =
        new Schema(
            Types.NestedField.required("value")
                .withId(1)
                .ofType(type)
                .withWriteDefaultExpression(
                    Expressions.apply(Expressions.function(function), arguments))
                .build(),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    String sqlTable = createDefaultTable(schema);
    assertThatThrownBy(
            () -> spark.sql("INSERT INTO " + sqlTable + " (data) VALUES ('value')").collect())
        .hasMessageContaining(message);
  }

  private static String createDefaultTable(Schema schema) {
    RESTCatalog catalog = SERVER.client();
    Namespace namespace =
        Namespace.of("defaults_" + UUID.randomUUID().toString().replace('-', '_'));
    catalog.createNamespace(namespace);
    catalog.createTable(TableIdentifier.of(namespace, "items"), schema);
    return "testrest." + namespace + ".items";
  }

  private static Schema defaultsSchema() {
    return new Schema(
        Types.NestedField.required("record_uuid")
            .withId(1)
            .ofType(Types.UUIDType.get())
            .withWriteDefaultExpression(Expressions.apply(Expressions.function("uuid"), List.of()))
            .build(),
        Types.NestedField.required("created_at_ns")
            .withId(2)
            .ofType(Types.LongType.get())
            .withWriteDefaultExpression(
                Expressions.apply(Expressions.function("current_time_ns"), List.of()))
            .build(),
        Types.NestedField.required(3, "data", Types.StringType.get()));
  }

  private static String createIdentityTable() {
    RESTCatalog catalog = SERVER.client();
    Namespace namespace =
        Namespace.of("identity_" + UUID.randomUUID().toString().replace('-', '_'));
    catalog.createNamespace(namespace);
    String sequenceId = UUID.randomUUID().toString();
    TableIdentifier table = TableIdentifier.of(namespace, "items");
    catalog.createTable(table, identitySchema(sequenceId));
    return "testrest." + namespace + ".items";
  }

  private static Schema identitySchema(String sequenceId) {
    return new Schema(
        Types.NestedField.required("id")
            .withId(1)
            .ofType(Types.LongType.get())
            .withWriteDefaultExpression(
                Expressions.apply(
                    Expressions.function("next_value_for"), List.<Object>of(sequenceId, 1, 1)))
            .build(),
        Types.NestedField.required(2, "data", Types.StringType.get()));
  }
}
