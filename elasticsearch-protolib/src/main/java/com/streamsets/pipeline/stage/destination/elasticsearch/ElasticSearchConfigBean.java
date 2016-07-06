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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.DataUtilEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;

import java.util.List;
import java.util.Map;

public class ElasticSearchConfigBean {

  public static final String CONF_PREFIX = "elasticSearchConfigBean.";

  @ConfigDefBean
  public ShieldConfigBean shieldConfigBean;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "elasticsearch",
      label = "Cluster Name",
      description = "",
      displayPosition = 10,
      group = "ELASTIC_SEARCH"
  )
  public String clusterName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      label = "Cluster URIs",
      defaultValue = "[\"hostname:port\"]",
      description = "Elasticsearch Node URIs",
      displayPosition = 20,
      group = "ELASTIC_SEARCH"
  )
  public List<String> uris;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Cluster HTTP URI",
      defaultValue = "hostname:port",
      description = "Elasticsearch HTTP Endpoint. " +
          "This is needed to verify version compatibility between the stage library and Elasticsearch cluster.",
      displayPosition = 21,
      group = "ELASTIC_SEARCH"
  )
  public String httpUri;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Shield",
      defaultValue = "false",
      description = "Use Shield",
      displayPosition = 22,
      group = "ELASTIC_SEARCH"
  )
  public boolean useShield;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Elastic Found Cluster",
      defaultValue = "false",
      description = "Select this option when connecting to an Elastic Found hosted cluster",
      displayPosition = 23,
      group = "ELASTIC_SEARCH"
  )
  public boolean useFound;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Detect Additional Nodes in Cluster",
      defaultValue = "false",
      description = "Select to automatically discover additional Elasticsearch nodes in the cluster. " +
          "Do not use if the Data Collector is on a different network from the cluster.",
      displayPosition = 24,
      group = "ELASTIC_SEARCH"
  )
  public boolean clientSniff;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      defaultValue = "",
      label = "Additional Configuration",
      description = "Additional Elasticsearch client configuration properties",
      displayPosition = 30,
      group = "ELASTIC_SEARCH"
  )
  public Map<String, String> configs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "${time:now()}",
      label = "Time Basis",
      description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
          "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<field path>\")}'.",
      displayPosition = 35,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String timeDriver;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use to resolve the datetime of a time based index",
      displayPosition = 37,
      group = "ELASTIC_SEARCH"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Index",
      description = "",
      displayPosition = 40,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String indexTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Mapping",
      description = "",
      displayPosition = 50,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String typeTemplate;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Document ID",
      description = "An expression which evaluates to a document ID. Required for upserts. Leave blank to use " +
          "auto-generated IDs.",
      displayPosition = 60,
      group = "ELASTIC_SEARCH",
      elDefs = {RecordEL.class, DataUtilEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT
  )
  public String docIdTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "Data Charset",
      displayPosition = 70,
      group = "ELASTIC_SEARCH"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String charset;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Enable Upsert",
      description = "Select to enable upserts. A Document ID expression is required.",
      displayPosition = 80,
      group = "ELASTIC_SEARCH"
  )
  public boolean upsert;
}
