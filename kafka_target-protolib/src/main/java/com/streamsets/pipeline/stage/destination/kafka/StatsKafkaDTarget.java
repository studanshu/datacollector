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
package com.streamsets.pipeline.stage.destination.kafka;


import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StatsAggregatorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;

@StageDef(
    version = 2,
    label = "Write to Kafka",
    description = "Writes Pipeline Statistic records to Kafka",
    icon = "",
    onlineHelpRefUrl = "",
    upgrader = KafkaTargetUpgrader.class)
@StatsAggregatorStage
@HideConfigs(
    preconditions = true,
    onErrorRecord = true,
    value = {"kafkaConfigBean.dataFormat", "kafkaConfigBean.kafkaConfig.singleMessagePerBatch"}
)
@GenerateResourceBundle
public class StatsKafkaDTarget extends KafkaDTarget {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "localhost:2181",
    label = "ZooKeeper URI",
    description = "Comma-separated list of ZooKeepers followed by optional chroot path. Use format: <HOST1>:<PORT1>,<HOST2>:<PORT2>,<HOST3>:<PORT3>/<ital><CHROOT_PATH></ital>",
    displayPosition = 100,
    group = "KAFKA"
  )
  public String zookeeperConnect;

  @Override
  protected Target createTarget() {
    kafkaConfigBean.dataFormat = DataFormat.SDC_JSON;
    return new KafkaTarget(kafkaConfigBean);
  }

}
