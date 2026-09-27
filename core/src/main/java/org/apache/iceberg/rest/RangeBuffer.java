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

import java.math.BigInteger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.responses.ReserveRangeResponse;

/** Buffers sequence values from a supplier that may return more values than requested. */
public class RangeBuffer {
  @FunctionalInterface
  public interface RangeSupplier {
    ReserveRangeResponse reserve(int count);
  }

  private final String sequenceId;
  private final long step;
  private final RangeSupplier supplier;
  private int requestCount = 8;
  private long nextValue;
  private long end;
  private boolean hasNext = false;

  public RangeBuffer(String sequenceId, long step, RangeSupplier supplier) {
    Preconditions.checkArgument(step != 0, "Invalid range step: zero");
    this.sequenceId = sequenceId;
    this.step = step;
    this.supplier = supplier;
  }

  public long next() {
    if (!hasNext) {
      refill();
    }
    long value = nextValue;
    if (value == end) {
      hasNext = false;
    } else {
      nextValue = Math.addExact(value, step);
    }
    return value;
  }

  private void refill() {
    ReserveRangeResponse range = supplier.reserve(requestCount);
    Preconditions.checkArgument(range != null, "Empty sequence range response");
    Preconditions.checkArgument(
        sequenceId.equals(range.sequenceId()), "Sequence ID changed: %s", range.sequenceId());
    Preconditions.checkArgument(range.step() == step, "Sequence step changed: %s", range.step());
    BigInteger span =
        BigInteger.valueOf(range.rangeEnd()).subtract(BigInteger.valueOf(range.rangeStart()));
    BigInteger[] division = span.divideAndRemainder(BigInteger.valueOf(step));
    Preconditions.checkArgument(
        division[0].signum() >= 0 && division[1].signum() == 0,
        "Empty or invalid sequence range: %s to %s",
        range.rangeStart(),
        range.rangeEnd());
    nextValue = range.rangeStart();
    end = range.rangeEnd();
    hasNext = true;
    // ponytail: a small doubling buffer is enough for executor refills. A driver buffer can
    // coalesce requests later without changing the executor contract.
    requestCount = Math.min(8192, requestCount * 2);
  }
}
