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
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;

import java.util.ArrayList;
import java.util.List;

public class AmazonS3TargetUpgrader implements StageUpgrader {
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
        // fall through
      case 2:
        upgradeV2ToV3(configs);
        // fall through
      case 3:
        upgradeV3ToV4(configs);
        // fall through
      case 4:
        upgradeV4ToV5(configs);
        // fall through
      case 5:
        upgradeV5ToV6(configs);
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
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "charset":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "csvFileFormat":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "csvHeader":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "csvReplaceNewLines":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "jsonMode":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "textFieldPath":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "textEmptyLineIfNull":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "avroSchema":
        case S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "binaryFieldPath":
          configsToRemove.add(config);
          configsToAdd.add(
              new Config(
                  S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig." + config.getName().substring(19),
                  config.getValue()
              )
          );
          break;
        default:
          // no upgrade needed
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);

    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.csvCustomDelimiter", '|'));
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.csvCustomEscape", '\\'));
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.csvCustomQuote", '\"'));
  }

  private void upgradeV2ToV3(List<Config> configs) {
    AWSUtil.renameAWSCredentialsConfigs(configs);
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig.avroCompression", "NULL"));
  }

  private void upgradeV3ToV4(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case S3TargetConfigBean.S3_CONFIG_PREFIX + "folder":
          configsToAdd.add(new Config(S3TargetConfigBean.S3_CONFIG_PREFIX + "commonPrefix", config.getValue()));
          configsToRemove.add(config);
          break;
        default:
          // no op
      }
    }

    configs.addAll(configsToAdd);
    configs.removeAll(configsToRemove);

    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "partitionTemplate", ""));
  }

  private void upgradeV4ToV5(List<Config> configs) {
    configs.add(new Config(S3TargetConfigBean.S3_SEE_CONFIG_PREFIX + "useSSE", "false"));
  }

  private void upgradeV5ToV6(List<Config> configs) {
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "timeZoneID", "UTC"));
    configs.add(new Config(S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "timeDriverTemplate", "${time:now()}"));
  }
}
