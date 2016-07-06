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
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.hbase.common.HBaseConnectionConfig;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;
import com.streamsets.pipeline.stage.processor.kv.LookupMode;
import com.streamsets.pipeline.stage.processor.kv.LookupModeChooserValues;

import java.util.ArrayList;
import java.util.List;

public class HBaseLookupConfig {
  @ConfigDefBean(groups = "HBASE")
  public HBaseConnectionConfig hBaseConnectionConfig = new HBaseConnectionConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Mode",
      description = "Whether to perform a bulk lookup of all keys in the batch, or perform individual lookups per key.",
      defaultValue = "BATCH",
      displayPosition = 10,
      group = "#0"
  )
  @ValueChooserModel(LookupModeChooserValues.class)
  public LookupMode mode = LookupMode.BATCH;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Lookup Parameters",
      displayPosition = 20,
      group = "LOOKUP"
  )
  @ListBeanModel
  public List<HBaseLookupParameterConfig> lookups = new ArrayList<>();

  @ConfigDefBean(groups = "LOOKUP")
  public CacheConfig cache = new CacheConfig();
}
