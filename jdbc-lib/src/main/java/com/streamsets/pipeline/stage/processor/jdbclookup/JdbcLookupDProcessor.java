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
package com.streamsets.pipeline.stage.processor.jdbclookup;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;

import java.util.List;

@StageDef(
    version = 1,
    label = "JDBC Lookup",
    description = "Lookup values via JDBC to enrich records.",
    icon = "rdbms.png",
    onlineHelpRefUrl = "index.html#Processors/JDBCLookup.html#task_kbr_2cy_hw"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class JdbcLookupDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.TEXT,
      mode = ConfigDef.Mode.SQL,
      label = "SQL Query",
      description = "SELECT <column>, ... FROM <table name> WHERE <column> <operator>  <expression>",
      elDefs = {StringEL.class, RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 20,
      group = "JDBC"
  )
  public String query;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Column Mappings",
      defaultValue = "",
      description = "Mappings from column names to field names",
      displayPosition = 40,
      group = "JDBC"
  )
  @ListBeanModel
  public List<JdbcFieldColumnMapping> columnMappings;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Clob Size (Characters)",
      displayPosition = 150,
      group = "JDBC"
  )
  public int maxClobSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Blob Size (Bytes)",
      displayPosition = 151,
      group = "JDBC"
  )
  public int maxBlobSize;

  @ConfigDefBean()
  public HikariPoolConfigBean hikariConfigBean;

  @Override
  protected Processor createProcessor() {
    return new JdbcLookupProcessor(
        query,
        columnMappings,
        maxClobSize,
        maxBlobSize,
        hikariConfigBean
    );
  }
}