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
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.snapshot.common.SnapshotInfoImpl;

public class SnapshotInfoJson {

  private final SnapshotInfo snapshotInfo;

  @JsonCreator
  public SnapshotInfoJson(@JsonProperty("user") String user,
                          @JsonProperty("id") String id,
                          @JsonProperty("label") String label,
                          @JsonProperty("name") String name,
                          @JsonProperty("rev") String rev,
                          @JsonProperty("timeStamp") long timeStamp,
                          @JsonProperty("inProgress") boolean inProgress) {
    snapshotInfo = new SnapshotInfoImpl(user, id, label, name, rev, timeStamp, inProgress);
  }


  public SnapshotInfoJson(SnapshotInfo snapshotInfo) {
    this.snapshotInfo = snapshotInfo;
  }

  public String getId() {
    return snapshotInfo.getId();
  }

  public String getLabel() {
    return snapshotInfo.getLabel();
  }

  public String getName() {
    return snapshotInfo.getName();
  }

  public String getRev() {
    return snapshotInfo.getRev();
  }

  public long getTimeStamp() {
    return snapshotInfo.getTimeStamp();
  }

  public String getUser() {
    return snapshotInfo.getUser();
  }

  public boolean isInProgress() {
    return snapshotInfo.isInProgress();
  }

  @JsonIgnore
  public SnapshotInfo getSnapshotInfo() {
    return snapshotInfo;
  }
}
