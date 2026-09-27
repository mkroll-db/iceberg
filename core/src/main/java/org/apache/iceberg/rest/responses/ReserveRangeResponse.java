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
package org.apache.iceberg.rest.responses;

import java.io.Serializable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;

public class ReserveRangeResponse implements RESTResponse, Serializable {
  private final String sequenceId;
  private final long rangeStart;
  private final long rangeEnd;
  private final long step;

  public ReserveRangeResponse(String sequenceId, long rangeStart, long rangeEnd, long step) {
    this.sequenceId = sequenceId;
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
    this.step = step;
    validate();
  }

  public String sequenceId() {
    return sequenceId;
  }

  public long rangeStart() {
    return rangeStart;
  }

  public long rangeEnd() {
    return rangeEnd;
  }

  public long step() {
    return step;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(sequenceId != null, "Invalid sequence ID: null");
    Preconditions.checkArgument(step != 0, "Invalid range step: zero");
  }
}
