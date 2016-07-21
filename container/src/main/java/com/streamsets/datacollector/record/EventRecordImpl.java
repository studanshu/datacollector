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
package com.streamsets.datacollector.record;

import com.streamsets.pipeline.api.EventRecord;

import java.util.Date;

public class EventRecordImpl extends RecordImpl implements EventRecord {

  public EventRecordImpl(String type, int version, String stageCreator, String recordSourceId, byte[] raw, String rawMime) {
    super(stageCreator, recordSourceId, raw, rawMime);
    setEventAtributes(type, version);
  }

  private void setEventAtributes(String type, int version) {
    getHeader().setAttribute(EventRecord.TYPE, type);
    getHeader().setAttribute(EventRecord.VERSION, String.valueOf(version));
    getHeader().setAttribute(EventRecord.CREATION_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
  }

}
