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
package com.streamsets.pipeline.stage.origin.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.lib.kinesis.KinesisConfigBean;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

public class KinesisConsumerConfigBean extends KinesisConfigBean {

  @ConfigDefBean(groups = "KINESIS")
  public DataParserFormatConfig dataFormatConfig;

  @ConfigDefBean(groups = "ADVANCED")
  public ProxyConfig proxyConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Application Name",
      description = "Kinesis equivalent of a Kafka Consumer Group",
      displayPosition = 30,
      group = "#0"
  )
  public String applicationName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Initial Position",
      defaultValue = "LATEST",
      description = "When using a new application name, whether to get only new messages, or read from the oldest.",
      displayPosition = 40,
      group = "#0"
  )
  @ValueChooserModel(InitialPositionInStreamChooserValues.class)
  public InitialPositionInStream initialPositionInStream;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data format to use when receiving records from Kinesis",
      displayPosition = 50,
      group = "#0"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (messages)",
      description = "Max number of records per batch. Kinesis will not return more than 2MB/s/shard.",
      displayPosition = 60,
      group = "#0",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxBatchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Read Interval (ms)",
      description = "Time KCL should wait between requests per shard. Cannot be set below 200ms. >250ms recommended.",
      displayPosition = 70,
      group = "#0",
      min = 200,
      max = Integer.MAX_VALUE
  )
  public long idleTimeBetweenReads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Batch Wait Time (ms)",
      description = "Max time to wait for data before sending a batch",
      displayPosition = 80,
      group = "#0",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public long maxWaitTime;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "5000",
      label = "Preview Batch Wait Time (ms)",
      description = "Max time to wait for data for preview mode. This should be at least several seconds for Kinesis.",
      displayPosition = 90,
      group = "#0",
      min = 1000,
      max = Integer.MAX_VALUE
  )
  public long previewWaitTime;
}
