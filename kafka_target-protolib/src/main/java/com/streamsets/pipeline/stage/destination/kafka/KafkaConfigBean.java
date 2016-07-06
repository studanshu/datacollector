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
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.KafkaDestinationGroups;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.List;

public class KafkaConfigBean {

  public static final String KAFKA_CONFIG_BEAN_PREFIX = "kafkaConfigBean.";
  @ConfigDefBean(groups = {"KAFKA"})
  public KafkaTargetConfig kafkaConfig;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "SDC_JSON",
    label = "Data Format",
    description = "",
    displayPosition = 60,
    group = "KAFKA"
  )
  @ValueChooserModel(ProducerDataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean(groups = {"KAFKA"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  public void init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    dataGeneratorFormatConfig.init(
        context,
        dataFormat,
        KafkaDestinationGroups.KAFKA.name(),
        KAFKA_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig",
        issues
    );
    kafkaConfig.init(context, dataFormat, issues);
  }


  public void destroy() {
    kafkaConfig.destroy();
  }
}
