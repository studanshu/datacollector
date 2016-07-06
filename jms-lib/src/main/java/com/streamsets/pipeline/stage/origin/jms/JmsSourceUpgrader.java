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
package com.streamsets.pipeline.stage.origin.jms;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.Compression;

import java.util.ArrayList;
import java.util.List;

public class JmsSourceUpgrader implements StageUpgrader {
  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
                              List<Config> configs) throws StageException {
    switch(fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
      case 2:
        upgradeV2ToV3(configs);
      case 3:
        upgradeV3ToV4(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    configs.add(new Config("dataFormatConfig.compression", Compression.NONE));
    configs.add(new Config("dataFormatConfig.filePatternInArchive", "*"));
  }
  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("jmsConfig.destinationType", "UNKNOWN"));
    configs.add(new Config("dataFormatConfig.csvSkipStartLines", 0));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    // This looks weird, but it's correct. Despite the fact that contextProperties
    // is a MAP config, the UI renders it as a list of maps, so new ArrayList gets
    // us what we want.
    configs.add(new Config("jmsConfig.contextProperties", new ArrayList<>()));
  }
}