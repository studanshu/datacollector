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
package com.streamsets.pipeline.stage.destination.maprstreams;


import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTarget;

@StageDef(
    version = 1,
    label = "Write to MapR Streams",
    description = "Writes records to MapR Streams as SDC Records",
    icon = "",
    onlineHelpRefUrl = "index.html#Pipeline_Configuration/ErrorHandling.html#concept_kgc_l4y_5r")
@ErrorStage
@HideConfigs(preconditions = true, onErrorRecord = true, value = {"maprStreamsTargetConfigBean.dataFormat"})
@GenerateResourceBundle
public class ToErrorMapRStreamsDTarget extends MapRStreamsDTarget {

  @Override
  protected Target createTarget() {
    maprStreamsTargetConfigBean.dataFormat = DataFormat.SDC_JSON;
    return new KafkaTarget(convertToKafkaConfigBean(maprStreamsTargetConfigBean));
  }

}
