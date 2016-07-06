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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.streamsets.pipeline.kafka.api.KafkaDestinationGroups;

@StageDef(
  version = 2,
  label = "Kafka Producer",
  description = "Writes data to Kafka",
  icon = "kafka.png",
  upgrader = KafkaTargetUpgrader.class,
  onlineHelpRefUrl = "index.html#Destinations/KProducer.html#task_q4d_4yl_zq"
)
@ConfigGroups(value = KafkaDestinationGroups.class)
@GenerateResourceBundle
public class KafkaDTarget extends DTarget {

  @ConfigDefBean()
  public KafkaConfigBean kafkaConfigBean;

  @Override
  protected Target createTarget() {
    return new KafkaTarget(kafkaConfigBean);
  }
}
