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
package com.streamsets.pipeline.stage.processor.hbase;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;

public class HBaseLookupParameterConfig {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Row Expression",
      description = "An EL expression defining the row to use for a lookup.",
      elDefs = {StringEL.class, RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 10,
      group = "#0"
  )
  public String rowExpr;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Column Expression",
      description = "An EL expression defining the column. Use format <COLUMNFAMILY>:<QUALIFIER>. " +
      "The column family must exist",
      elDefs = {StringEL.class, RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 10,
      group = "#0"
  )
  public String columnExpr;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "TimeStamp Expression",
      description = "An EL expression defining the timestamp to use for a lookup.",
      elDefs = {StringEL.class, RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 15,
      group = "#0"
  )
  public String timestampExpr = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "TimeStamp Expression",
      description = "An EL expression defining the timestamp to use for a lookup.",
      elDefs = {StringEL.class, RecordEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 15,
      group = "#0"
  )
  public String tables = "t1";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Output Field",
      displayPosition = 20,
      group = "#0"
  )
  @FieldSelectorModel(singleValued = true)
  public String outputFieldPath;
}
