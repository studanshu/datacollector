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
package com.streamsets.datacollector.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.client.StringUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


@ApiModel(description = "")
@javax.annotation.Generated(
    value = "class io.swagger.codegen.languages.JavaClientCodegen",
    date = "2015-09-11T14:51:29.367-07:00"
)
public class PipelineInfoJson   {

  private String name = null;
  private String description = null;
  private Date created = null;
  private Date lastModified = null;
  private String creator = null;
  private String lastModifier = null;
  private String lastRev = null;
  private String uuid = null;
  private Boolean valid = null;
  private Map<String, Object> metadata = new HashMap<String, Object>();

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("name")
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }
  public void setDescription(String description) {
    this.description = description;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("created")
  public Date getCreated() {
    return created;
  }
  public void setCreated(Date created) {
    this.created = created;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("lastModified")
  public Date getLastModified() {
    return lastModified;
  }
  public void setLastModified(Date lastModified) {
    this.lastModified = lastModified;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("creator")
  public String getCreator() {
    return creator;
  }
  public void setCreator(String creator) {
    this.creator = creator;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("lastModifier")
  public String getLastModifier() {
    return lastModifier;
  }
  public void setLastModifier(String lastModifier) {
    this.lastModifier = lastModifier;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("lastRev")
  public String getLastRev() {
    return lastRev;
  }
  public void setLastRev(String lastRev) {
    this.lastRev = lastRev;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("uuid")
  public String getUuid() {
    return uuid;
  }
  public void setUuid(String uuid) {
    this.uuid = uuid;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("valid")
  public Boolean getValid() {
    return valid;
  }
  public void setValid(Boolean valid) {
    this.valid = valid;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("metadata")
  public Map<String, Object> getMetadata() {
    return metadata;
  }
  public void setMetadata(Map<String, Object> metadata) {
    this.metadata = metadata;
  }

  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class PipelineInfoJson {\n");

    sb.append("    name: ").append(StringUtil.toIndentedString(name)).append("\n");
    sb.append("    description: ").append(StringUtil.toIndentedString(description)).append("\n");
    sb.append("    created: ").append(StringUtil.toIndentedString(created)).append("\n");
    sb.append("    lastModified: ").append(StringUtil.toIndentedString(lastModified)).append("\n");
    sb.append("    creator: ").append(StringUtil.toIndentedString(creator)).append("\n");
    sb.append("    lastModifier: ").append(StringUtil.toIndentedString(lastModifier)).append("\n");
    sb.append("    lastRev: ").append(StringUtil.toIndentedString(lastRev)).append("\n");
    sb.append("    uuid: ").append(StringUtil.toIndentedString(uuid)).append("\n");
    sb.append("    valid: ").append(StringUtil.toIndentedString(valid)).append("\n");
    sb.append("    metadata: ").append(StringUtil.toIndentedString(metadata)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
