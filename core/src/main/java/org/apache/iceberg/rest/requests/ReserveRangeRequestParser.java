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
package org.apache.iceberg.rest.requests;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.util.JsonUtil;

public class ReserveRangeRequestParser {
  private ReserveRangeRequestParser() {}

  public static void toJson(ReserveRangeRequest request, JsonGenerator gen) throws IOException {
    request.validate();
    gen.writeStartObject();
    gen.writeNumberField("start", request.start());
    gen.writeNumberField("step", request.step());
    gen.writeNumberField("count", request.count());
    gen.writeEndObject();
  }

  public static ReserveRangeRequest fromJson(JsonNode json) {
    return new ReserveRangeRequest(
        JsonUtil.getLong("start", json),
        JsonUtil.getLong("step", json),
        JsonUtil.getInt("count", json));
  }
}
