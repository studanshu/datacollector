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
package com.streamsets.datacollector.execution.snapshot.common;

import com.streamsets.datacollector.execution.SnapshotInfo;

public class SnapshotInfoImpl implements SnapshotInfo {

  private final String id;
  private final String label;
  private final String name;
  private final String rev;
  private final long timestamp;
  private final String user;
  private final boolean inProgress;

  public SnapshotInfoImpl(String user, String id, String label, String name, String rev, long timestamp,
                          boolean inProgress) {
    this.id = id;
    this.label = label;
    this.name = name;
    this.rev = rev;
    this.timestamp = timestamp;
    this.user = user;
    this.inProgress = inProgress;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getLabel() {
    return label;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getRev() {
    return rev;
  }

  @Override
  public long getTimeStamp() {
    return timestamp;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public boolean isInProgress() {
    return inProgress;
  }
}
