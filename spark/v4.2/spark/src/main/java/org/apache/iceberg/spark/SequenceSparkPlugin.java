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

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.responses.ReserveRangeResponse;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

/** Relays executor range requests through the driver's authenticated REST catalog. */
public class SequenceSparkPlugin implements SparkPlugin {
  private static volatile PluginContext executorContext;

  public static ReserveRangeResponse reserve(
      String catalogName, String tableName, String sequenceId, long start, long step, int count) {
    PluginContext context = executorContext;
    Preconditions.checkState(
        context != null,
        "Per-record default requires spark.plugins=%s at session start",
        SequenceSparkPlugin.class.getName());
    try {
      return (ReserveRangeResponse)
          context.ask(new ReserveMessage(catalogName, tableName, sequenceId, start, step, count));
    } catch (Exception e) {
      throw new RuntimeException("Failed to reserve sequence range through driver plugin", e);
    }
  }

  @Override
  public DriverPlugin driverPlugin() {
    return new Driver();
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return new Executor();
  }

  private static class Driver implements DriverPlugin {
    @Override
    public Object receive(Object message) {
      ReserveMessage request = (ReserveMessage) message;
      CatalogPlugin catalog =
          SparkSession.active().sessionState().catalogManager().catalog(request.catalogName);
      Preconditions.checkArgument(
          catalog instanceof SparkCatalog,
          "Per-record default requires SparkCatalog: %s",
          request.catalogName);
      return ((SparkCatalog) catalog)
          .reserveSequence(
              TableIdentifier.parse(request.tableName),
              request.sequenceId,
              request.start,
              request.step,
              request.count);
    }
  }

  private static class Executor implements ExecutorPlugin {
    @Override
    public void init(PluginContext context, Map<String, String> extraConf) {
      executorContext = context;
    }

    @Override
    public void shutdown() {
      executorContext = null;
    }
  }

  private static class ReserveMessage implements Serializable {
    private final String catalogName;
    private final String tableName;
    private final String sequenceId;
    private final long start;
    private final long step;
    private final int count;

    private ReserveMessage(
        String catalogName, String tableName, String sequenceId, long start, long step, int count) {
      this.catalogName = catalogName;
      this.tableName = tableName;
      this.sequenceId = sequenceId;
      this.start = start;
      this.step = step;
      this.count = count;
    }
  }
}
