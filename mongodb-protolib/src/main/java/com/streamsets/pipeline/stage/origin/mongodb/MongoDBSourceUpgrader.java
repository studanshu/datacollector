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
package com.streamsets.pipeline.stage.origin.mongodb;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBConfig;

import java.util.ArrayList;
import java.util.List;

public class MongoDBSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(
      String library,
      String stageName,
      String stageInstance,
      int fromVersion,
      int toVersion,
      List<Config> configs
  ) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "mongoConnectionString":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "connectionString", config.getValue()));
          configsToRemove.add(config);
          break;
        case "database":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "database", config.getValue()));
          configsToRemove.add(config);
          break;
        case "collection":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "collection", config.getValue()));
          configsToRemove.add(config);
          break;
        case "authenticationType":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "authenticationType", config.getValue()));
          configsToRemove.add(config);
          break;
        case "username":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "username", config.getValue()));
          configsToRemove.add(config);
          break;
        case "password":
          configsToAdd.add(new Config(MongoDBConfig.MONGO_CONFIG_PREFIX + "password", config.getValue()));
          configsToRemove.add(config);
          break;
        case "isCapped":
          configsToAdd.add(new Config(MongoDBConfig.CONFIG_PREFIX + "isCapped", config.getValue()));
          configsToRemove.add(config);
          break;
        case "initialOffset":
          configsToAdd.add(new Config(MongoDBConfig.CONFIG_PREFIX + "initialOffset", config.getValue()));
          configsToRemove.add(config);
          break;
        case "offsetField":
          configsToAdd.add(new Config(MongoDBConfig.CONFIG_PREFIX + "offsetField", config.getValue()));
          configsToRemove.add(config);
          break;
        case "batchSize":
          configsToAdd.add(new Config(MongoDBConfig.CONFIG_PREFIX + "batchSize", config.getValue()));
          configsToRemove.add(config);
          break;
        case "maxBatchWaitTime":
          configsToAdd.add(new Config(MongoDBConfig.CONFIG_PREFIX + "maxBatchWaitTime", config.getValue()));
          configsToRemove.add(config);
          break;
        case "readPreference":
          configsToAdd.add(new Config(MongoDBConfig.CONFIG_PREFIX + "readPreference", config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }
}
