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
package org.apache.iceberg.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.SequenceSparkPlugin;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Runs a temporary REST catalog and Spark SQL prompt for the sequence prototype. */
public final class SequenceDemo {
  private static final String CATALOG = "demo";
  private static final String TABLE = "demo.identity.items";

  private SequenceDemo() {}

  public static void main(String[] args) throws Exception {
    if (args.length > 1 || (args.length == 1 && !"--cluster".equals(args[0]))) {
      throw new IllegalArgumentException("Usage: sequenceDemo [--cluster]");
    }

    boolean cluster = args.length == 1;
    if (cluster && System.getenv("SPARK_HOME") == null) {
      throw new IllegalArgumentException("--cluster requires SPARK_HOME with Spark 4.2 jars");
    }

    Map<String, String> config =
        Map.of(
            RESTCatalogServer.REST_PORT,
            String.valueOf(RCKUtils.findFreePort()),
            RESTCatalogServer.REST_HOST,
            InetAddress.getLoopbackAddress().getHostAddress(),
            CatalogProperties.CLIENT_POOL_SIZE,
            "1");
    RESTCatalogServer server = new RESTCatalogServer(config);
    server.start(false);
    try (RESTCatalog catalog = RCKUtils.initCatalogClient(config)) {
      createTable(catalog);
      SparkSession spark = startSpark(catalog, cluster);
      try {
        runPrompt(spark);
      } finally {
        spark.stop();
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
      }
    } finally {
      server.stop();
    }
  }

  private static void createTable(RESTCatalog catalog) {
    Namespace namespace = Namespace.of("identity");
    catalog.createNamespace(namespace);
    String sequenceId = UUID.randomUUID().toString();
    Schema schema =
        new Schema(
            Types.NestedField.required("id")
                .withId(1)
                .ofType(Types.LongType.get())
                .withWriteDefaultExpression(
                    Expressions.apply(
                        Expressions.function("next_value_for"), List.<Object>of(sequenceId, 1, 1)))
                .build(),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    catalog.createTable(TableIdentifier.of(namespace, "items"), schema);
  }

  private static SparkSession startSpark(RESTCatalog catalog, boolean cluster) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("Iceberg sequence demo")
            .master(cluster ? "local-cluster[2,1,1024]" : "local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.plugins", SequenceSparkPlugin.class.getName())
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName())
            .config("spark.sql.catalog." + CATALOG + ".type", "rest")
            .config("spark.sql.catalog." + CATALOG + ".cache-enabled", "false")
            .config(
                "spark.sql.catalog." + CATALOG + ".uri",
                catalog.properties().get(CatalogProperties.URI));
    if (cluster) {
      // local-cluster workers run on this machine, so they can use the Gradle runtime classpath.
      builder.config("spark.executor.extraClassPath", System.getProperty("java.class.path"));
    }
    SparkSession spark = builder.getOrCreate();
    spark.sparkContext().setLogLevel("OFF");
    return spark;
  }

  private static void runPrompt(SparkSession spark) throws IOException {
    BufferedReader input =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    StringBuilder statement = new StringBuilder();
    System.out.printf(
        "Iceberg sequence demo (%s). Table: %s%n", spark.sparkContext().master(), TABLE);
    printHelp();

    while (true) {
      System.out.print(statement.length() == 0 ? "sql> " : "...> ");
      System.out.flush();
      String line = input.readLine();
      if (line == null) {
        return;
      }

      String trimmed = line.trim();
      if (statement.length() == 0 && (":quit".equals(trimmed) || ":exit".equals(trimmed))) {
        return;
      } else if (statement.length() == 0 && ":help".equals(trimmed)) {
        printHelp();
        continue;
      } else if (statement.length() == 0 && ":concurrent".equals(trimmed)) {
        try {
          concurrentInsert(spark);
        } catch (Exception e) {
          printError(e);
        }
        continue;
      } else if (":cancel".equals(trimmed)) {
        statement.setLength(0);
        continue;
      } else if (trimmed.isEmpty() && statement.length() == 0) {
        continue;
      }

      statement.append(line).append('\n');
      if (trimmed.endsWith(";") || trimmed.isEmpty()) {
        String sql = statement.toString().trim();
        statement.setLength(0);
        if (sql.endsWith(";")) {
          sql = sql.substring(0, sql.length() - 1);
        }
        try {
          Dataset<Row> result = spark.sql(sql);
          if (result.schema().isEmpty()) {
            result.collect();
            System.out.println("OK");
          } else {
            result.show(20, false);
          }
        } catch (Exception e) {
          printError(e);
        }
      }
    }
  }

  private static void concurrentInsert(SparkSession spark) throws Exception {
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
                          + TABLE
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
      spark
          .sql(
              "SELECT count(*) AS rows, count(DISTINCT id) AS distinct_ids, "
                  + "count(*) - count(id) AS null_ids FROM "
                  + TABLE)
          .show(false);
    } finally {
      executor.shutdownNow();
    }
  }

  private static void printError(Exception error) {
    String message = error.getMessage();
    System.err.println(
        "ERROR: " + (message == null ? error : message.lines().findFirst().orElse("")));
  }

  private static void printHelp() {
    System.out.println("Enter SQL ending in ';' (or a blank line to run a multiline statement).");
    System.out.println("Commands: :concurrent, :cancel, :help, :quit");
    System.out.println("Try:");
    System.out.println("  INSERT INTO " + TABLE + " (data) VALUES ('first'), ('second');");
    System.out.println("  SELECT id, data FROM " + TABLE + " ORDER BY id;");
    System.out.println("  :concurrent    (four writers, 100 rows each)");
    System.out.println("  INSERT INTO " + TABLE + " (id, data) VALUES (5, 'explicit');");
  }
}
