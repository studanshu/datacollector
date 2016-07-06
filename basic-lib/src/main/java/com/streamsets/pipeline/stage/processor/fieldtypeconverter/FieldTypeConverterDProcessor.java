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
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDef.Type;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DProcessor;

import java.util.List;

@StageDef(
    version=1,
    label="Field Converter",
    description = "Converts the data type of a field",
    icon="converter.png",
    onlineHelpRefUrl = "index.html#Processors/FieldConverter.html#task_g23_2tq_wq"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FieldTypeConverterDProcessor extends DProcessor {

  @ConfigDef(
      required = false,
      type = Type.MODEL,
      defaultValue="",
      label = "",
      description = "",
      displayPosition = 10,
      group = "TYPE_CONVERSION"
  )
  @ListBeanModel
  public List<FieldTypeConverterConfig> fieldTypeConverterConfigs;

  @Override
  protected Processor createProcessor() {
    return new FieldTypeConverterProcessor((fieldTypeConverterConfigs));
  }
}
