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
package com.streamsets.pipeline.stage.destination.solr;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingModeChooserValues;

import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = 2,
    label = "Solr",
    description = "Upload data to an Apache Solr",
    icon = "solr.png",
    onlineHelpRefUrl = "index.html#Destinations/Solr.html#task_ld1_phr_wr",
    upgrader = SolrDTargetUpgrader.class
)
@ConfigGroups(Groups.class)
public class SolrDTarget extends DTarget {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "SINGLE_NODE",
    label = "Instance Type",
    description = "",
    displayPosition = 10,
    group = "SOLR"
  )
  @ValueChooserModel(InstanceTypeOptionsChooserValues.class)
  public InstanceTypeOptions instanceType;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "http://localhost:8983/solr/corename",
    label = "Solr URI",
    description = "",
    displayPosition = 20,
    group = "SOLR",
    dependsOn = "instanceType",
    triggeredByValue = { "SINGLE_NODE"}
  )
  public String solrURI;


  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "localhost:9983",
    label = "ZooKeeper Connection String",
    description = "Comma-separated list of the Zookeeper <HOST>:<PORT> used by the SolrCloud",
    displayPosition = 30,
    group = "SOLR",
    dependsOn = "instanceType",
    triggeredByValue = { "SOLR_CLOUD"}
  )
  public String zookeeperConnect;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Default Collection Name",
      description = "",
      displayPosition = 30,
      group = "SOLR",
      dependsOn = "instanceType",
      triggeredByValue = { "SOLR_CLOUD"}
  )
  public String defaultCollection;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "BATCH",
    label = "Record Indexing Mode",
    description = "If 'Record by Record' the destination indexes one record at a time, if 'Record batch' " +
      "the destination bulk indexes all the records in the batch. ",
    displayPosition = 40,
    group = "SOLR"
  )
  @ValueChooserModel(ProcessingModeChooserValues.class)
  public ProcessingMode indexingMode;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue="",
    label = "Fields",
    description = "Selected fields are mapped to columns of the same name. These should match your schema",
    displayPosition = 50,
    group = "SOLR"
  )
  @ListBeanModel
  public List<SolrFieldMappingConfig> fieldNamesMap;

  @Override
  protected Target createTarget() {
    return new SolrTarget(instanceType, solrURI, zookeeperConnect, indexingMode, fieldNamesMap, defaultCollection);
  }

}
