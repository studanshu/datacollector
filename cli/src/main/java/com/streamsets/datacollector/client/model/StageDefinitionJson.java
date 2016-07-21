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

import java.util.ArrayList;
import java.util.List;


// This class was originally generated, however it's now maintained manually
@ApiModel(description = "")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen",
  date = "2015-09-11T14:51:29.367-07:00")
public class StageDefinitionJson   {

  private String name = null;

  public enum TypeEnum {
    SOURCE("SOURCE"), PROCESSOR("PROCESSOR"), TARGET("TARGET");

    private final String value;

    TypeEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private TypeEnum type = null;
  private String className = null;
  private String label = null;
  private String libraryLabel = null;
  private ConfigGroupDefinitionJson configGroupDefinition = null;
  private RawSourceDefinitionJson rawSourceDefinition = null;
  private Boolean errorStage = null;
  private Boolean variableOutputStreams = null;
  private Integer outputStreams = null;
  private String outputStreamLabelProviderClass = null;
  private List<String> outputStreamLabels = new ArrayList<String>();

  public enum ExecutionModesEnum {
    STANDALONE("STANDALONE"),
    CLUSTER_BATCH("CLUSTER_BATCH"),
    CLUSTER_YARN_STREAMING("CLUSTER_YARN_STREAMING"),
    CLUSTER_MESOS_STREAMING("CLUSTER_MESOS_STREAMING"),
    SLAVE("SLAVE");

    private final String value;

    ExecutionModesEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private List<ExecutionModesEnum> executionModes = new ArrayList<ExecutionModesEnum>();
  private String description = null;
  private Boolean privateClassLoader = null;
  private String library = null;
  private List<ConfigDefinitionJson> configDefinitions = new ArrayList<ConfigDefinitionJson>();
  private String version = null;
  private String icon = null;
  private Boolean onRecordError = null;
  private Boolean preconditions = null;
  private Boolean resetOffset = null;
  private Boolean producingEvents = null;


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
  @JsonProperty("type")
  public TypeEnum getType() {
    return type;
  }
  public void setType(TypeEnum type) {
    this.type = type;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("className")
  public String getClassName() {
    return className;
  }
  public void setClassName(String className) {
    this.className = className;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("label")
  public String getLabel() {
    return label;
  }
  public void setLabel(String label) {
    this.label = label;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("libraryLabel")
  public String getLibraryLabel() {
    return libraryLabel;
  }
  public void setLibraryLabel(String libraryLabel) {
    this.libraryLabel = libraryLabel;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configGroupDefinition")
  public ConfigGroupDefinitionJson getConfigGroupDefinition() {
    return configGroupDefinition;
  }
  public void setConfigGroupDefinition(ConfigGroupDefinitionJson configGroupDefinition) {
    this.configGroupDefinition = configGroupDefinition;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("rawSourceDefinition")
  public RawSourceDefinitionJson getRawSourceDefinition() {
    return rawSourceDefinition;
  }
  public void setRawSourceDefinition(RawSourceDefinitionJson rawSourceDefinition) {
    this.rawSourceDefinition = rawSourceDefinition;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("errorStage")
  public Boolean getErrorStage() {
    return errorStage;
  }
  public void setErrorStage(Boolean errorStage) {
    this.errorStage = errorStage;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("variableOutputStreams")
  public Boolean getVariableOutputStreams() {
    return variableOutputStreams;
  }
  public void setVariableOutputStreams(Boolean variableOutputStreams) {
    this.variableOutputStreams = variableOutputStreams;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("outputStreams")
  public Integer getOutputStreams() {
    return outputStreams;
  }
  public void setOutputStreams(Integer outputStreams) {
    this.outputStreams = outputStreams;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("outputStreamLabelProviderClass")
  public String getOutputStreamLabelProviderClass() {
    return outputStreamLabelProviderClass;
  }
  public void setOutputStreamLabelProviderClass(String outputStreamLabelProviderClass) {
    this.outputStreamLabelProviderClass = outputStreamLabelProviderClass;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("outputStreamLabels")
  public List<String> getOutputStreamLabels() {
    return outputStreamLabels;
  }
  public void setOutputStreamLabels(List<String> outputStreamLabels) {
    this.outputStreamLabels = outputStreamLabels;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("executionModes")
  public List<ExecutionModesEnum> getExecutionModes() {
    return executionModes;
  }
  public void setExecutionModes(List<ExecutionModesEnum> executionModes) {
    this.executionModes = executionModes;
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
  @JsonProperty("privateClassLoader")
  public Boolean getPrivateClassLoader() {
    return privateClassLoader;
  }
  public void setPrivateClassLoader(Boolean privateClassLoader) {
    this.privateClassLoader = privateClassLoader;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("library")
  public String getLibrary() {
    return library;
  }
  public void setLibrary(String library) {
    this.library = library;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configDefinitions")
  public List<ConfigDefinitionJson> getConfigDefinitions() {
    return configDefinitions;
  }
  public void setConfigDefinitions(List<ConfigDefinitionJson> configDefinitions) {
    this.configDefinitions = configDefinitions;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("version")
  public String getVersion() {
    return version;
  }
  public void setVersion(String version) {
    this.version = version;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("icon")
  public String getIcon() {
    return icon;
  }
  public void setIcon(String icon) {
    this.icon = icon;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("onRecordError")
  public Boolean getOnRecordError() {
    return onRecordError;
  }
  public void setOnRecordError(Boolean onRecordError) {
    this.onRecordError = onRecordError;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("preconditions")
  public Boolean getPreconditions() {
    return preconditions;
  }
  public void setPreconditions(Boolean preconditions) {
    this.preconditions = preconditions;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("resetOffset")
  public Boolean getResetOffset() {
    return resetOffset;
  }
  public void setResetOffset(Boolean resetOffset) {
    this.resetOffset = resetOffset;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("producingEvents")
  public Boolean getProducingEvents() {
    return producingEvents;
  }
  public void setProducingEvents(Boolean producingEvents) {
    this.producingEvents = producingEvents;
  }

  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class StageDefinitionJson {\n");

    sb.append("    name: ").append(StringUtil.toIndentedString(name)).append("\n");
    sb.append("    type: ").append(StringUtil.toIndentedString(type)).append("\n");
    sb.append("    className: ").append(StringUtil.toIndentedString(className)).append("\n");
    sb.append("    label: ").append(StringUtil.toIndentedString(label)).append("\n");
    sb.append("    libraryLabel: ").append(StringUtil.toIndentedString(libraryLabel)).append("\n");
    sb.append("    configGroupDefinition: ").append(StringUtil.toIndentedString(configGroupDefinition)).append("\n");
    sb.append("    rawSourceDefinition: ").append(StringUtil.toIndentedString(rawSourceDefinition)).append("\n");
    sb.append("    errorStage: ").append(StringUtil.toIndentedString(errorStage)).append("\n");
    sb.append("    variableOutputStreams: ").append(StringUtil.toIndentedString(variableOutputStreams)).append("\n");
    sb.append("    outputStreams: ").append(StringUtil.toIndentedString(outputStreams)).append("\n");
    sb.append("    outputStreamLabelProviderClass: ").append(StringUtil.toIndentedString(outputStreamLabelProviderClass)).append("\n");
    sb.append("    outputStreamLabels: ").append(StringUtil.toIndentedString(outputStreamLabels)).append("\n");
    sb.append("    executionModes: ").append(StringUtil.toIndentedString(executionModes)).append("\n");
    sb.append("    description: ").append(StringUtil.toIndentedString(description)).append("\n");
    sb.append("    privateClassLoader: ").append(StringUtil.toIndentedString(privateClassLoader)).append("\n");
    sb.append("    library: ").append(StringUtil.toIndentedString(library)).append("\n");
    sb.append("    configDefinitions: ").append(StringUtil.toIndentedString(configDefinitions)).append("\n");
    sb.append("    version: ").append(StringUtil.toIndentedString(version)).append("\n");
    sb.append("    icon: ").append(StringUtil.toIndentedString(icon)).append("\n");
    sb.append("    onRecordError: ").append(StringUtil.toIndentedString(onRecordError)).append("\n");
    sb.append("    preconditions: ").append(StringUtil.toIndentedString(preconditions)).append("\n");
    sb.append("    producingEvents: ").append(StringUtil.toIndentedString(producingEvents)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
