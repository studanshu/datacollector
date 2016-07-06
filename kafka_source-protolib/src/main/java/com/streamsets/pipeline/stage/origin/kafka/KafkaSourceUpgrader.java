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
package com.streamsets.pipeline.stage.origin.kafka;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.List;

public class KafkaSourceUpgrader implements StageUpgrader {

  private static final String CONF = "kafkaConfigBean";
  private static final String DATA_FORMAT_CONFIG= "dataFormatConfig";
  private static final Joiner joiner = Joiner.on(".");

  private final List<Config> configsToRemove = new ArrayList<>();
  private final List<Config> configsToAdd = new ArrayList<>();

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
    configs.add(new Config("csvCustomDelimiter", '|'));
    configs.add(new Config("csvCustomEscape", '\\'));
    configs.add(new Config("csvCustomQuote", '\"'));
    configs.add(new Config("csvRecordType", "LIST"));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    configs.add(new Config("csvSkipStartLines", 0));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    for (Config config : configs) {
      switch (config.getName()) {
        case "dataFormat":
        case "metadataBrokerList":
        case "zookeeperConnect":
        case "consumerGroup":
        case "topic":
        case "produceSingleRecordPerMessage":
        case "maxBatchSize":
        case "maxWaitTime":
        case "kafkaConsumerConfigs":
          configsToAdd.add(new Config(joiner.join(CONF, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        case "charset":
        case "removeCtrlChars":
        case "textMaxLineLen":
        case "jsonContent":
        case "jsonMaxObjectLen":
        case "csvFileFormat":
        case "csvHeader":
        case "csvMaxObjectLen":
        case "csvCustomDelimiter":
        case "csvCustomEscape":
        case "csvCustomQuote":
        case "csvRecordType":
        case "csvSkipStartLines":
        case "xmlRecordElement":
        case "xmlMaxObjectLen":
        case "logMode":
        case "logMaxObjectLen":
        case "retainOriginalLine":
        case "customLogFormat":
        case "regex":
        case "fieldPathsToGroupName":
        case "grokPatternDefinition":
        case "grokPattern":
        case "onParseError":
        case "maxStackTraceLines":
        case "enableLog4jCustomLogFormat":
        case "log4jCustomLogFormat":
        case "schemaInMessage":
        case "avroSchema":
        case "binaryMaxObjectLen":
        case "protoDescriptorFile":
        case "messageType":
          configsToAdd.add(new Config(joiner.join(CONF, DATA_FORMAT_CONFIG, config.getName()), config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no-op
      }
    }
    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);
  }
}
