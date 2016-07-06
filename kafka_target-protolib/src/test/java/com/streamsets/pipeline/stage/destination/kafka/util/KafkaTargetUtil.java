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
package com.streamsets.pipeline.stage.destination.kafka.util;

import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.PartitionStrategy;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTargetConfig;
import com.streamsets.pipeline.stage.destination.kafka.KafkaConfigBean;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTarget;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.Map;

public class KafkaTargetUtil {

  public static KafkaTarget createKafkaTarget(
      String metadataBrokerList,
      String topic,
      String partition,
      Map<String, String> kafkaProducerConfigs,
      boolean singleMessagePerBatch,
      PartitionStrategy partitionStrategy,
      boolean runtimeTopicResolution,
      String topicExpression,
      String topicWhiteList,
      KafkaTargetConfig kafkaConfig,
      DataFormat dataFormat,
      DataGeneratorFormatConfig dataGeneratorFormatConfig
  ) {
    kafkaConfig.metadataBrokerList = metadataBrokerList;
    kafkaConfig.topic = topic;
    kafkaConfig.partition = partition;
    kafkaConfig.kafkaProducerConfigs = kafkaProducerConfigs;
    kafkaConfig.singleMessagePerBatch = singleMessagePerBatch;
    kafkaConfig.partitionStrategy = partitionStrategy;
    kafkaConfig.runtimeTopicResolution = runtimeTopicResolution;
    kafkaConfig.topicExpression = topicExpression;
    kafkaConfig.topicWhiteList = topicWhiteList;

    KafkaConfigBean kafkaConfigBean = new KafkaConfigBean();
    kafkaConfigBean.dataFormat = dataFormat;
    kafkaConfigBean.kafkaConfig = kafkaConfig;
    kafkaConfigBean.dataGeneratorFormatConfig = dataGeneratorFormatConfig;

    return new KafkaTarget(kafkaConfigBean);
  }
}
