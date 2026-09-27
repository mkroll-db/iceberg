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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.rest.responses.ReserveRangeResponse;
import org.junit.jupiter.api.Test;

public class TestRangeBuffer {
  @Test
  public void consumesAnOverservedRange() {
    AtomicInteger calls = new AtomicInteger();
    RangeBuffer buffer =
        new RangeBuffer(
            "sequence",
            1,
            count -> {
              assertThat(count).isEqualTo(calls.get() == 0 ? 8 : 16);
              int start = calls.getAndIncrement() * 16;
              return new ReserveRangeResponse("sequence", start, start + 15, 1);
            });
    for (int value = 0; value <= 16; value++) {
      assertThat(buffer.next()).isEqualTo(value);
    }
    assertThat(calls.get()).isEqualTo(2);
  }

  @Test
  public void rejectsStepDriftWithoutRetrying() {
    AtomicInteger calls = new AtomicInteger();
    RangeBuffer buffer =
        new RangeBuffer(
            "sequence",
            1,
            count -> {
              calls.incrementAndGet();
              return new ReserveRangeResponse("sequence", 1, 5, 2);
            });
    assertThatThrownBy(buffer::next).hasMessageContaining("Sequence step changed");
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  public void rejectsEmptyOrMalformedRange() {
    RangeBuffer buffer =
        new RangeBuffer("sequence", 1, count -> new ReserveRangeResponse("sequence", 5, 4, 1));
    assertThatThrownBy(buffer::next).hasMessageContaining("Empty or invalid sequence range");
  }
}
