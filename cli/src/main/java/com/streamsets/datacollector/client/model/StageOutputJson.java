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

import com.streamsets.datacollector.client.StringUtil;
import com.streamsets.datacollector.client.model.RecordJson;
import com.streamsets.datacollector.client.model.ErrorMessageJson;
import java.util.*;


import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2015-09-11T14:51:29.367-07:00")
public class StageOutputJson   {

  private String instanceName = null;
  private Map<String, List<RecordJson>> output = new HashMap<String, List<RecordJson>>();
  private List<RecordJson> errorRecords = new ArrayList<RecordJson>();
  private List<ErrorMessageJson> stageErrors = new ArrayList<ErrorMessageJson>();


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("instanceName")
  public String getInstanceName() {
    return instanceName;
  }
  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("output")
  public Map<String, List<RecordJson>> getOutput() {
    return output;
  }
  public void setOutput(Map<String, List<RecordJson>> output) {
    this.output = output;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorRecords")
  public List<RecordJson> getErrorRecords() {
    return errorRecords;
  }
  public void setErrorRecords(List<RecordJson> errorRecords) {
    this.errorRecords = errorRecords;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stageErrors")
  public List<ErrorMessageJson> getStageErrors() {
    return stageErrors;
  }
  public void setStageErrors(List<ErrorMessageJson> stageErrors) {
    this.stageErrors = stageErrors;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class StageOutputJson {\n");

    sb.append("    instanceName: ").append(StringUtil.toIndentedString(instanceName)).append("\n");
    sb.append("    output: ").append(StringUtil.toIndentedString(output)).append("\n");
    sb.append("    errorRecords: ").append(StringUtil.toIndentedString(errorRecords)).append("\n");
    sb.append("    stageErrors: ").append(StringUtil.toIndentedString(stageErrors)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
