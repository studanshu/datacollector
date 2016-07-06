/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.influxdb;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordConverterUtil {
  private RecordConverterUtil() {}

  public static Map<String, String> getTags(List<String> tagFields, Record record) throws OnRecordErrorException {
    Map<String, String> tags = new HashMap<>();

    for (String fieldPath : tagFields) {
      if (!record.has(fieldPath)) {
        continue;
      }

      Field tagField = record.get(fieldPath);
      switch (tagField.getType()) {
        case MAP:
          // fall through
        case LIST_MAP:
          for (Map.Entry<String, Field> entry : tagField.getValueAsMap().entrySet()) {
            tags.put(entry.getKey(), entry.getValue().getValueAsString());
          }
          break;
        case LIST:
          throw new OnRecordErrorException(Errors.INFLUX_08, fieldPath);
        default:
          tags.put(CollectdRecordConverter.stripPathPrefix(fieldPath), tagField.getValueAsString());
          break;
      }
    }

    return tags;
  }
}
