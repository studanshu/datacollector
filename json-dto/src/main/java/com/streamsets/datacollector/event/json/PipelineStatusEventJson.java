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
package com.streamsets.datacollector.event.json;

import java.util.Collection;

import com.streamsets.datacollector.config.json.PipelineStatusJson;
import com.streamsets.datacollector.config.json.ValidationStatusJson;

public class PipelineStatusEventJson implements EventJson {

  private String name;
  private String rev;
  private PipelineStatusJson pipelineStatus;
  private String message;
  private ValidationStatusJson validationStatus;
  private String issues;
  private boolean isRemote;
  private Collection<WorkerInfoJson> workerInfos;
  private boolean isClusterMode;

  public PipelineStatusJson getPipelineStatus() {
    return pipelineStatus;
  }

  public void setPipelineStatus(PipelineStatusJson pipelineStatus) {
    this.pipelineStatus = pipelineStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public ValidationStatusJson getValidationStatus() {
    return validationStatus;
  }

  public boolean isRemote() {
    return isRemote;
  }

  public void setRemote(boolean isRemote) {
    this.isRemote = isRemote;
  }

  public void setValidationStatus(ValidationStatusJson validationStatus) {
    this.validationStatus = validationStatus;
  }

  public String getIssues() {
    return issues;
  }

  public void setIssues(String issues) {
    this.issues = issues;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getRev() {
    return rev;
  }

  public void setRev(String rev) {
    this.rev = rev;
  }

  public Collection<WorkerInfoJson> getWorkerInfos() {
    return workerInfos;
  }

  public void setWorkerInfos(Collection<WorkerInfoJson> workerInfos) {
    this.workerInfos = workerInfos;
  }

  public boolean isClusterMode() {
    return isClusterMode;
  }

  public void setClusterMode(boolean clusterMode) {
    isClusterMode = clusterMode;
  }
}
