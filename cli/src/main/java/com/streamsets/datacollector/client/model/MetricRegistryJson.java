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
import com.streamsets.datacollector.client.model.HistogramJson;
import com.streamsets.datacollector.client.model.MeterJson;
import com.streamsets.datacollector.client.model.CounterJson;
import com.streamsets.datacollector.client.model.TimerJson;
import java.util.*;


import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaClientCodegen", date = "2015-09-11T14:51:29.367-07:00")
public class MetricRegistryJson   {

  private String version = null;
  private Map<String, Object> gauges = new HashMap<String, Object>();
  private Map<String, CounterJson> counters = new HashMap<String, CounterJson>();
  private Map<String, HistogramJson> histograms = new HashMap<String, HistogramJson>();
  private Map<String, MeterJson> meters = new HashMap<String, MeterJson>();
  private Map<String, TimerJson> timers = new HashMap<String, TimerJson>();
  private List<String> slaves = new ArrayList<String>();


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
  @JsonProperty("gauges")
  public Map<String, Object> getGauges() {
    return gauges;
  }
  public void setGauges(Map<String, Object> gauges) {
    this.gauges = gauges;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("counters")
  public Map<String, CounterJson> getCounters() {
    return counters;
  }
  public void setCounters(Map<String, CounterJson> counters) {
    this.counters = counters;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("histograms")
  public Map<String, HistogramJson> getHistograms() {
    return histograms;
  }
  public void setHistograms(Map<String, HistogramJson> histograms) {
    this.histograms = histograms;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("meters")
  public Map<String, MeterJson> getMeters() {
    return meters;
  }
  public void setMeters(Map<String, MeterJson> meters) {
    this.meters = meters;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("timers")
  public Map<String, TimerJson> getTimers() {
    return timers;
  }
  public void setTimers(Map<String, TimerJson> timers) {
    this.timers = timers;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("slaves")
  public List<String> getSlaves() {
    return slaves;
  }
  public void setSlaves(List<String> slaves) {
    this.slaves = slaves;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class MetricRegistryJson {\n");

    sb.append("    version: ").append(StringUtil.toIndentedString(version)).append("\n");
    sb.append("    gauges: ").append(StringUtil.toIndentedString(gauges)).append("\n");
    sb.append("    counters: ").append(StringUtil.toIndentedString(counters)).append("\n");
    sb.append("    histograms: ").append(StringUtil.toIndentedString(histograms)).append("\n");
    sb.append("    meters: ").append(StringUtil.toIndentedString(meters)).append("\n");
    sb.append("    timers: ").append(StringUtil.toIndentedString(timers)).append("\n");
    sb.append("    slaves: ").append(StringUtil.toIndentedString(slaves)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
