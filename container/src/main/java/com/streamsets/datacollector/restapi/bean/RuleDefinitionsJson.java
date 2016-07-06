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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.config.RuleDefinitions;

import java.util.List;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleDefinitionsJson {

  private final RuleDefinitions ruleDefinitions;

  @JsonCreator
  public RuleDefinitionsJson(
    @JsonProperty("metricsRuleDefinitions") List<MetricsRuleDefinitionJson> metricsRuleDefinitionJsons,
    @JsonProperty("dataRuleDefinitions") List<DataRuleDefinitionJson> dataRuleDefinitionJsons,
    @JsonProperty("driftRuleDefinitions") List<DriftRuleDefinitionJson> driftRuleDefinitionJsons,
    @JsonProperty("emailIds") List<String> emailIds,
    @JsonProperty("uuid") UUID uuid) {
    this.ruleDefinitions = new com.streamsets.datacollector.config.RuleDefinitions(
      BeanHelper.unwrapMetricRuleDefinitions(metricsRuleDefinitionJsons),
      BeanHelper.unwrapDataRuleDefinitions(dataRuleDefinitionJsons),
      BeanHelper.unwrapDriftRuleDefinitions(driftRuleDefinitionJsons),
      emailIds,
      uuid
    );
  }

  public RuleDefinitionsJson(com.streamsets.datacollector.config.RuleDefinitions ruleDefinitions) {
    this.ruleDefinitions = ruleDefinitions;
  }

  public List<MetricsRuleDefinitionJson> getMetricsRuleDefinitions() {
    return BeanHelper.wrapMetricRuleDefinitions(ruleDefinitions.getMetricsRuleDefinitions());
  }

  public List<DataRuleDefinitionJson> getDataRuleDefinitions() {
    return BeanHelper.wrapDataRuleDefinitions(ruleDefinitions.getDataRuleDefinitions());
  }

  public List<DriftRuleDefinitionJson> getDriftRuleDefinitions() {
    return BeanHelper.wrapDriftRuleDefinitions(ruleDefinitions.getDriftRuleDefinitions());
  }

  public List<String> getEmailIds() {
    return ruleDefinitions.getEmailIds();
  }

  public List<RuleIssueJson> getRuleIssues() {
    return BeanHelper.wrapRuleIssues(ruleDefinitions.getRuleIssues());
  }

  public void setRuleIssues(List<RuleIssueJson> ruleIssueJsons) {
    //NO-OP, for jackson
  }

  public UUID getUuid() {
    return ruleDefinitions.getUuid();
  }

  @JsonIgnore
  public com.streamsets.datacollector.config.RuleDefinitions getRuleDefinitions() {
    return ruleDefinitions;
  }
}
