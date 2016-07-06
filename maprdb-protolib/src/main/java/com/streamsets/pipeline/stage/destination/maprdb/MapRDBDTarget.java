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
package com.streamsets.pipeline.stage.destination.maprdb;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.stage.destination.hbase.HBaseDTarget;

import java.util.List;

@StageDef(
  version = 1,
  label = "MapR DB",
  description = "Writes to a MapR DB",
  icon = "mapr.png",
  privateClassLoader = false,
  onlineHelpRefUrl = "index.html#Destinations/MapRDB.html#task_pgk_p2z_yv"
)
@HideConfigs(
  value = {
    "hBaseConnectionConfig.zookeeperQuorum",
    "hBaseConnectionConfig.clientPort",
    "hBaseConnectionConfig.zookeeperParentZnode"
  }
)
@GenerateResourceBundle
public class MapRDBDTarget extends HBaseDTarget {

  @Override
  protected Target createTarget() {
    return new MapRDBTarget(
      hBaseConnectionConfig.zookeeperQuorum,
      hBaseConnectionConfig.clientPort,
      hBaseConnectionConfig.zookeeperParentZnode,
      hBaseConnectionConfig.tableName,
      hbaseRowKey,
      rowKeyStorageType,
      hbaseFieldColumnMapping,
      hBaseConnectionConfig.kerberosAuth,
      hBaseConnectionConfig.hbaseConfDir,
      hBaseConnectionConfig.hbaseConfigs,
      hBaseConnectionConfig.hbaseUser,
      implicitFieldMapping,
      ignoreMissingFieldPath,
      ignoreInvalidColumn,
      timeDriver
    );
  }
}