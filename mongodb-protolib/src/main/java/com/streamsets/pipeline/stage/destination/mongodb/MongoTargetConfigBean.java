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
package com.streamsets.pipeline.stage.destination.mongodb;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;

public class MongoTargetConfigBean {

  @ConfigDefBean(groups = {"MONGODB", "CREDENTIALS", "ADVANCED"})
  public MongoDBConfig mongoConfig;

  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      label = "Unique Key Field",
      description = "Unique key field is required for upserts and optional for inserts and deletes",
      required = false,
      displayPosition = 1000,
      group = "MONGODB"
  )
  @FieldSelectorModel(singleValued = true)
  public String uniqueKeyField;

  @ConfigDef(
      type = ConfigDef.Type.MODEL,
      label = "Write Concern",
      description = "Sets the write concern",
      defaultValue = "JOURNALED",
      required = true,
      displayPosition = 1001,
      group = "MONGODB"
  )
  @ValueChooserModel(WriteConcernChooserValues.class)
  public WriteConcernLabel writeConcern = WriteConcernLabel.JOURNALED;

}
