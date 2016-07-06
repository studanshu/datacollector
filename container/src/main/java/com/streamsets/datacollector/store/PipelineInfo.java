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
package com.streamsets.datacollector.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class PipelineInfo implements Serializable {
  private String name;
  private String description;
  private Date created;
  private Date lastModified;
  private String creator;
  private String lastModifier;
  private String lastRev;
  private UUID uuid;
  private boolean valid;
  private Map<String, Object> metadata;


  @JsonCreator
  public PipelineInfo(
      @JsonProperty("name") String name,
      @JsonProperty("description") String description,
      @JsonProperty("created") Date created,
      @JsonProperty("lastModified") Date lastModified,
      @JsonProperty("creator") String creator,
      @JsonProperty("lastModifier") String lastModifier,
      @JsonProperty("lastRev") String lastRev,
      @JsonProperty("uuid") UUID uuid,
      @JsonProperty("valid") boolean valid,
      @JsonProperty("metadata") Map<String, Object> metadata
      ) {
    this.name = name;
    this.description = description;
    this.created = created;
    this.lastModified = lastModified;
    this.creator = creator;
    this.lastModifier = lastModifier;
    this.lastRev = lastRev;
    this.uuid = uuid;
    this.valid = valid;
    this.metadata = metadata;
  }

  public PipelineInfo(
      PipelineInfo pipelineInfo,
      String description,
      Date lastModified,
      String lastModifier,
      String lastRev,
      UUID uuid, boolean valid,
      Map<String, Object> metadata
  ) {
    this(pipelineInfo.getName(), description, pipelineInfo.getCreated(), lastModified,
         pipelineInfo.getCreator(), lastModifier, lastRev, uuid, valid, metadata);
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public Date getCreated() {
    return created;
  }

  public Date getLastModified() {
    return lastModified;
  }

  public String getCreator() {
    return creator;
  }

  public String getLastModifier() {
    return lastModifier;
  }

  public String getLastRev() {
    return lastRev;
  }

  public UUID getUuid() {
    return uuid;
  }

  public boolean isValid() {
    return valid;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }
}
