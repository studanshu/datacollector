/**
 * Copyright 2015 StreamSets Inc.
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
package com.streamsets.pipeline.kafka.api;

public class MessageAndOffset {

  private final byte[] payload;
  private final long offset;
  private final int partition;

  public MessageAndOffset(byte[] payload, long offset, int partition) {
    this.payload = payload;
    this.offset = offset;
    this.partition = partition;
  }

  public byte[] getPayload() {
    return payload;
  }

  public long getOffset() {
    return offset;
  }

  public int getPartition() {
    return partition;
  }
}
