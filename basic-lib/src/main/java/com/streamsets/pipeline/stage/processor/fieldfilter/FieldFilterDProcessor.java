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
package com.streamsets.pipeline.stage.processor.fieldfilter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version=1,
    label="Field Remover",
    description="Removes fields from a record",
    icon="filter.png",
    onlineHelpRefUrl = "index.html#Processors/FieldRemover.html#task_c1j_btr_wq"
)
@ConfigGroups(Groups.class)
@HideConfigs(onErrorRecord = true)
@GenerateResourceBundle
public class FieldFilterDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="REMOVE",
      label = "Action",
      description = "",
      displayPosition = 10,
      group = "FILTER"
  )
  @ValueChooserModel(FilterOperationChooserValues.class)
  public FilterOperation filterOperation;

  @ConfigDef(
      required = true,
      type = Type.MODEL,
      defaultValue="",
      label = "Fields",
      description = "",
      displayPosition = 20,
      group = "FILTER"
  )
  @FieldSelectorModel
  public List<String> fields;

  @Override
  protected Processor createProcessor() {
    return new FieldFilterProcessor(filterOperation, fields);
  }

}