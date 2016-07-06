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
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.jdbc.JdbcBaseUpgrader;

import java.util.List;

/** {@inheritDoc} */
public class JdbcSourceUpgrader extends JdbcBaseUpgrader {

  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        // fall through
      case 4:
        upgradeV4toV5(configs);
        // fall through
      case 5:
        upgradeV5toV6(configs);
      case 6:
        upgradeV6toV7(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV3ToV4(List<Config> configs) {
    configs.add(new Config("maxBatchSize", 1000));
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("txnIdColumnName", ""));
    configs.add(new Config("txnMaxSize", 10000));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("jdbcRecordType", "MAP"));
  }

  private void upgradeV4toV5(List<Config> configs) {
    configs.add(new Config("maxClobSize", 1000));
  }

  private void upgradeV5toV6(List<Config> configs) {
    upgradeToConfigBeanV1(configs);
  }

  private void upgradeV6toV7(List<Config> configs) {
    configs.add(new Config("createJDBCNsHeaders", false));
    configs.add(new Config("jdbcNsHeaderPrefix", "jdbc."));
  }

}
