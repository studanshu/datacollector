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
package com.streamsets.datacollector.event.handler.remote;

import java.util.Collection;

import com.streamsets.datacollector.config.dto.ValidationStatus;
import com.streamsets.datacollector.event.dto.WorkerInfo;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.validation.Issues;

public class PipelineAndValidationStatus {
    private final String name;
    private final String rev;
    private final PipelineStatus pipelineStatus;
    private final boolean isRemote;
    private ValidationStatus validationStatus;
    private Issues issues;
    private String message;
    private Collection<WorkerInfo> workerInfos;
    private boolean isClusterMode;

    public PipelineAndValidationStatus(
      String name,
      String rev,
      boolean isRemote,
      PipelineStatus pipelineStatus,
      String message,
      Collection<WorkerInfo> workerInfos,
      boolean isClusterMode) {

      this.name = name;
      this.rev = rev;
      this.isRemote = isRemote;
      this.pipelineStatus = pipelineStatus;
      this.message = message;
      this.workerInfos = workerInfos;
      this.isClusterMode = isClusterMode;
    }

    public void setValidationStatus(ValidationStatus validationStatus) {
      this.validationStatus = validationStatus;
    }

    public void setIssues(Issues issues) {
      this.issues = issues;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public ValidationStatus getValidationStatus() {
      return validationStatus;
    }

    public Collection<WorkerInfo> getWorkerInfos() {
      return workerInfos;
    }

    public Issues getIssues() {
      return issues;
    }

    public String getMessage() {
      return message;
    }

    public String getName() {
      return name;
    }

    public String getRev() {
      return rev;
    }

    public PipelineStatus getPipelineStatus() {
      return pipelineStatus;
    }

    public boolean isRemote() {
      return isRemote;
    }

    public boolean isClusterMode() {
      return isClusterMode;
    }
}

