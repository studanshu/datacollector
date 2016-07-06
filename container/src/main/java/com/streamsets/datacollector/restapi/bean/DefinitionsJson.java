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

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefinitionsJson {
  List<PipelineDefinitionJson> pipeline;
  List<StageDefinitionJson> stages;
  Map<String,Map<String, List<String>>> rulesDefinitions;
  Map<String, Object> elCatalog;
  Set<Object> runtimeConfigs;
  Map<String, String> stageIcons;

  public List<PipelineDefinitionJson> getPipeline() {
    return pipeline;
  }

  public void setPipeline(List<PipelineDefinitionJson> pipeline) {
    this.pipeline = pipeline;
  }

  public List<StageDefinitionJson> getStages() {
    return stages;
  }

  public void setStages(List<StageDefinitionJson> stages) {
    this.stages = stages;
  }

  public Map<String,Map<String, List<String>>> getRulesElMetadata() {
    return rulesDefinitions;
  }

  public void setRulesElMetadata(Map<String,Map<String, List<String>>> rulesDefinitions) {
    this.rulesDefinitions = rulesDefinitions;
  }

  public Map<String, Object> getElCatalog() {
    return elCatalog;
  }

  public void setElCatalog(Map<String, Object> elCatalog) {
    this.elCatalog = elCatalog;
  }

  public Set<Object> getRuntimeConfigs() {
    return runtimeConfigs;
  }

  public void setRuntimeConfigs(Set<Object> runtimeConfigs) {
    this.runtimeConfigs = runtimeConfigs;
  }

  public Map<String, String> getStageIcons() {
    return stageIcons;
  }

  public void setStageIcons(Map<String, String> stageIcons) {
    this.stageIcons = stageIcons;
  }
}
