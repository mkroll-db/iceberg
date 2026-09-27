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
package org.apache.iceberg.spark.source;

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundApply;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.StructType;

public final class PerRecordDefaultColumns {
  private PerRecordDefaultColumns() {}

  public static void validateNoDefaultField(Schema schema, Term term, String operation) {
    Preconditions.checkArgument(term instanceof UnboundTerm, "Unsupported write term: %s", term);
    String name = ((UnboundTerm<?>) term).ref().name();
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(
        field == null || field.writeDefaultExpression() == null,
        "Per-record default column cannot be %s: %s",
        operation,
        name);
  }

  static Schema validationSchema(Schema schema) {
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(schema.columns().size());
    for (Types.NestedField field : schema.columns()) {
      // ponytail: NULL is only a marker for per-record defaults. Keep nullability checks for
      // every other column; a future analyzer rule can replace this marker before the write.
      fields.add(field.writeDefaultExpression() == null ? field : field.asOptional());
    }
    return new Schema(fields);
  }

  static List<Spec> specs(Table table, StructType writeType) {
    List<Spec> specs = Lists.newArrayList();
    String fullName = table.name();
    int separator = fullName.indexOf('.');
    for (Types.NestedField field : table.schema().columns()) {
      UnboundApply<?> expression = field.writeDefaultExpression();
      if (expression == null) {
        continue;
      }

      Preconditions.checkArgument(
          expression.function().catalog() == null
              || "iceberg_functions".equals(expression.function().catalog()),
          "Unsupported per-record default for %s: %s",
          field.name(),
          expression);
      for (PartitionSpec partitionSpec : table.specs().values()) {
        for (PartitionField partitionField : partitionSpec.fields()) {
          Preconditions.checkArgument(
              partitionField.sourceId() != field.fieldId(),
              "Per-record default column cannot be partitioned: %s",
              field.name());
        }
      }
      for (SortOrder sortOrder : table.sortOrders().values()) {
        for (SortField sortField : sortOrder.fields()) {
          Preconditions.checkArgument(
              sortField.sourceId() != field.fieldId(),
              "Per-record default column cannot be sorted: %s",
              field.name());
        }
      }
      switch (expression.function().name()) {
        case "next_value_for":
          Preconditions.checkArgument(
              separator > 0, "Sequence default requires a named REST catalog table: %s", fullName);
          Preconditions.checkArgument(
              field.type().equals(Types.LongType.get()),
              "Sequence column must be a long: %s",
              field.name());
          Preconditions.checkArgument(
              expression.arguments().size() == 3,
              "Invalid next_value_for arguments for %s: %s",
              field.name(),
              expression);
          Object sequenceId = argument(expression, 0);
          Object start = argument(expression, 1);
          Object step = argument(expression, 2);
          Preconditions.checkArgument(
              sequenceId instanceof String
                  && (start instanceof Integer || start instanceof Long)
                  && (step instanceof Integer || step instanceof Long),
              "Invalid next_value_for arguments for %s: %s",
              field.name(),
              expression);
          long stepValue = ((Number) step).longValue();
          Preconditions.checkArgument(
              stepValue != 0, "Sequence step must not be zero: %s", field.name());
          specs.add(
              Spec.sequence(
                  fullName.substring(0, separator),
                  fullName.substring(separator + 1),
                  (String) sequenceId,
                  ((Number) start).longValue(),
                  stepValue,
                  writeType.fieldIndex(field.name())));
          break;

        case "uuid":
          Preconditions.checkArgument(
              field.type().equals(Types.UUIDType.get()),
              "UUID default requires a uuid column: %s",
              field.name());
          Preconditions.checkArgument(
              expression.arguments().isEmpty(),
              "Invalid uuid arguments for %s: %s",
              field.name(),
              expression);
          specs.add(Spec.uuid(writeType.fieldIndex(field.name())));
          break;

        case "current_time_ns":
          Preconditions.checkArgument(
              field.type().equals(Types.LongType.get()),
              "current_time_ns default requires a long column: %s",
              field.name());
          Preconditions.checkArgument(
              expression.arguments().isEmpty(),
              "Invalid current_time_ns arguments for %s: %s",
              field.name(),
              expression);
          specs.add(Spec.currentTimeNs(writeType.fieldIndex(field.name())));
          break;

        default:
          throw new IllegalArgumentException(
              "Unsupported per-record default for " + field.name() + ": " + expression);
      }
    }
    return specs;
  }

  private static Object argument(UnboundApply<?> expression, int index) {
    Object arg = expression.arguments().get(index);
    Preconditions.checkArgument(
        arg instanceof Literal, "Sequence argument must be literal: %s", arg);
    return ((Literal<?>) arg).value();
  }

  static class Spec implements Serializable {
    enum Kind {
      SEQUENCE,
      UUID,
      CURRENT_TIME_NS
    }

    final Kind kind;
    final String catalogName;
    final String tableName;
    final String sequenceId;
    final long start;
    final long step;
    final int ordinal;

    private Spec(
        Kind kind,
        String catalogName,
        String tableName,
        String sequenceId,
        long start,
        long step,
        int ordinal) {
      this.kind = kind;
      this.catalogName = catalogName;
      this.tableName = tableName;
      this.sequenceId = sequenceId;
      this.start = start;
      this.step = step;
      this.ordinal = ordinal;
    }

    private static Spec sequence(
        String catalogName,
        String tableName,
        String sequenceId,
        long start,
        long step,
        int ordinal) {
      return new Spec(Kind.SEQUENCE, catalogName, tableName, sequenceId, start, step, ordinal);
    }

    private static Spec uuid(int ordinal) {
      return new Spec(Kind.UUID, null, null, null, 0, 0, ordinal);
    }

    private static Spec currentTimeNs(int ordinal) {
      return new Spec(Kind.CURRENT_TIME_NS, null, null, null, 0, 0, ordinal);
    }
  }
}
