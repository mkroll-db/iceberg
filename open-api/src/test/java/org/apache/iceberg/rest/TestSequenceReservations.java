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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.responses.ReserveRangeResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestSequenceReservations {
  @RegisterExtension
  private static final RESTServerExtension SERVER =
      new RESTServerExtension(
          Map.of(
              RESTCatalogServer.REST_PORT,
              RESTServerExtension.FREE_PORT,
              CatalogProperties.CLIENT_POOL_SIZE,
              "1"));

  @Test
  public void concurrentReservationsAreUniqueAndContiguous() throws Exception {
    RESTCatalog catalog = SERVER.client();
    Namespace namespace =
        Namespace.of("sequence_" + UUID.randomUUID().toString().replace('-', '_'));
    catalog.createNamespace(namespace);
    String sequenceId = UUID.randomUUID().toString();
    Schema schema =
        new Schema(
            Types.NestedField.required("id")
                .withId(1)
                .ofType(Types.LongType.get())
                .withWriteDefaultExpression(
                    Expressions.apply(
                        Expressions.function("next_value_for"), List.<Object>of(sequenceId, 1, 3)))
                .build());
    TableIdentifier first = TableIdentifier.of(namespace, "first");
    TableIdentifier second = TableIdentifier.of(namespace, "second");
    catalog.createTable(first, schema);
    catalog.createTable(second, schema);
    assertThat(catalog.loadTable(first).schema().asStruct()).isEqualTo(schema.asStruct());

    Set<Long> values = ConcurrentHashMap.newKeySet();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Callable<Void>> tasks = new ArrayList<>();
      for (int writer = 0; writer < 8; writer++) {
        tasks.add(
            () -> {
              for (int reservation = 0; reservation < 20; reservation++) {
                ReserveRangeResponse range = catalog.reserveSequence(first, sequenceId, 1, 3, 16);
                assertThat(range.step()).isEqualTo(3);
                assertThat(range.rangeEnd()).isEqualTo(range.rangeStart() + 45);
                for (long value = range.rangeStart(); value <= range.rangeEnd(); value += 3) {
                  assertThat(values.add(value)).isTrue();
                }
              }
              return null;
            });
      }
      for (Future<Void> future : executor.invokeAll(tasks)) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }

    assertThat(values).hasSize(8 * 20 * 16);
    for (int index = 0; index < values.size(); index++) {
      assertThat(values).contains(1L + 3L * index);
    }
    assertThatThrownBy(() -> catalog.reserveSequence(first, sequenceId, 2, 3, 1))
        .hasMessageContaining("sequence start or step changed");
    assertThat(catalog.reserveSequence(first, sequenceId, 1, 3, 1).rangeStart())
        .isEqualTo(1L + 3L * values.size());
    assertThat(catalog.reserveSequence(second, sequenceId, 1, 3, 1).rangeStart()).isEqualTo(1L);
  }

  @Test
  public void negativeStepAndBoundaryStartDoNotOverflow() {
    RESTCatalog catalog = SERVER.client();
    Namespace namespace =
        Namespace.of("descending_" + UUID.randomUUID().toString().replace('-', '_'));
    catalog.createNamespace(namespace);
    TableIdentifier table = TableIdentifier.of(namespace, "items");
    catalog.createTable(
        table, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
    String sequenceId = UUID.randomUUID().toString();
    ReserveRangeResponse range = catalog.reserveSequence(table, sequenceId, Long.MAX_VALUE, -1, 2);
    assertThat(range.rangeStart()).isEqualTo(Long.MAX_VALUE);
    assertThat(range.rangeEnd()).isEqualTo(Long.MAX_VALUE - 1);
  }

  @Test
  public void abandonedAndConcurrentAttemptsGetDisjointRanges() {
    RESTCatalog catalog = SERVER.client();
    Namespace namespace =
        Namespace.of("attempts_" + UUID.randomUUID().toString().replace('-', '_'));
    catalog.createNamespace(namespace);
    TableIdentifier table = TableIdentifier.of(namespace, "items");
    catalog.createTable(
        table, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
    String sequenceId = UUID.randomUUID().toString();

    RangeBuffer failedAttempt =
        new RangeBuffer(
            sequenceId, 1, count -> catalog.reserveSequence(table, sequenceId, 1, 1, count));
    assertThat(failedAttempt.next()).isEqualTo(1);
    // A task can fail after using one value. Its unconsumed reservation is never reissued.
    RangeBuffer retry =
        new RangeBuffer(
            sequenceId, 1, count -> catalog.reserveSequence(table, sequenceId, 1, 1, count));
    RangeBuffer speculativeAttempt =
        new RangeBuffer(
            sequenceId, 1, count -> catalog.reserveSequence(table, sequenceId, 1, 1, count));
    assertThat(retry.next()).isEqualTo(9);
    assertThat(speculativeAttempt.next()).isEqualTo(17);
  }
}
