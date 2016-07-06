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

import com.google.common.net.HostAndPort;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.kafka.api.ConsumerFactorySettings;
import com.streamsets.pipeline.kafka.api.KafkaOriginGroups;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumer;
import com.streamsets.pipeline.kafka.api.SdcKafkaConsumerFactory;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtil;
import com.streamsets.pipeline.kafka.api.SdcKafkaValidationUtilFactory;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public abstract class BaseKafkaSource extends BaseSource implements OffsetCommitter {

  protected final KafkaConfigBean conf;
  protected SdcKafkaConsumer kafkaConsumer;

  private ErrorRecordHandler errorRecordHandler;
  private DataParserFactory parserFactory;
  private int originParallelism = 0;
  private SdcKafkaValidationUtil kafkaValidationUtil;

  public BaseKafkaSource(KafkaConfigBean conf) {
    this.conf = conf;
    kafkaValidationUtil = SdcKafkaValidationUtilFactory.getInstance().create();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = new ArrayList<>();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    if (conf.topic == null || conf.topic.isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KafkaConfigBean.KAFKA_CONFIG_BEAN_PREFIX + "topic",
              KafkaErrors.KAFKA_05
          )
      );
    }
    //maxWaitTime
    if (conf.maxWaitTime < 1) {
      issues.add(
          getContext().createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KafkaConfigBean.KAFKA_CONFIG_BEAN_PREFIX + "maxWaitTime",
              KafkaErrors.KAFKA_35
          )
      );
    }

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        KafkaOriginGroups.KAFKA.name(),
        KafkaConfigBean.DATA_FROMAT_CONFIG_BEAN_PREFIX,
        issues
    );
    if (conf.dataFormat == DataFormat.XML && conf.produceSingleRecordPerMessage) {
      issues.add(
          getContext().createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KafkaConfigBean.KAFKA_CONFIG_BEAN_PREFIX + "produceSingleRecordPerMessage",
              KafkaErrors.KAFKA_40
          )
      );
    }

    parserFactory = conf.dataFormatConfig.getParserFactory();

    // Validate broker config
    List<HostAndPort> kafkaBrokers = kafkaValidationUtil.validateKafkaBrokerConnectionString(
        issues,
        conf.metadataBrokerList,
        KafkaOriginGroups.KAFKA.name(),
        KafkaConfigBean.KAFKA_CONFIG_BEAN_PREFIX + "metadataBrokerList",
        getContext()
    );

    if(!kafkaBrokers.isEmpty()) {
      try {
        int partitionCount = kafkaValidationUtil.getPartitionCount(
          conf.metadataBrokerList,
          conf.topic,
          conf.kafkaConsumerConfigs == null ?
            Collections.<String, Object>emptyMap() :
            new HashMap<String, Object>(conf.kafkaConsumerConfigs),
          3,
          1000
        );
        if (partitionCount < 1) {
          issues.add(
            getContext().createConfigIssue(
              KafkaOriginGroups.KAFKA.name(),
              KafkaConfigBean.KAFKA_CONFIG_BEAN_PREFIX + "topic",
              KafkaErrors.KAFKA_42,
              conf.topic
            )
          );
        } else {
          //cache the partition count as parallelism for future use
          originParallelism = partitionCount;
        }
      } catch (StageException e) {
        issues.add(
          getContext().createConfigIssue(
            KafkaOriginGroups.KAFKA.name(),
            KafkaConfigBean.KAFKA_CONFIG_BEAN_PREFIX + "topic",
            KafkaErrors.KAFKA_41,
            conf.topic,
            e.toString(),
            e
          )
        );
      }
    }

    // Validate zookeeper config
    kafkaValidationUtil.validateZkConnectionString(
        issues,
        conf.zookeeperConnect,
        KafkaOriginGroups.KAFKA.name(),
        KafkaConfigBean.KAFKA_CONFIG_BEAN_PREFIX + "zookeeperConnect",
        getContext()
    );

    //consumerGroup
    if (conf.consumerGroup == null || conf.consumerGroup.isEmpty()) {
      issues.add(
        getContext().createConfigIssue(
          KafkaOriginGroups.KAFKA.name(),
          KafkaConfigBean.KAFKA_CONFIG_BEAN_PREFIX + "consumerGroup",
          KafkaErrors.KAFKA_33
        )
      );
    }

    //validate connecting to kafka
    if (issues.isEmpty()) {
      ConsumerFactorySettings settings = new ConsumerFactorySettings(
          conf.zookeeperConnect,
          conf.metadataBrokerList,
          conf.topic,
          conf.maxWaitTime,
          getContext(),
          conf.kafkaConsumerConfigs == null ?
              Collections.<String, Object>emptyMap() :
              new HashMap<String, Object>(conf.kafkaConsumerConfigs),
              conf.consumerGroup
      );
      kafkaConsumer = SdcKafkaConsumerFactory.create(settings).create();
      kafkaConsumer.validate(issues, getContext());
    }

    return issues;
  }

  // This API is being used by ClusterKafkaSource
  public int getParallelism() throws StageException {
    if (originParallelism == 0) {
      //origin parallelism is not yet calculated
      originParallelism = kafkaValidationUtil.getPartitionCount(
          conf.metadataBrokerList,
          conf.topic,
          conf.kafkaConsumerConfigs == null ?
              Collections.<String, Object>emptyMap() :
              new HashMap<String, Object>(conf.kafkaConsumerConfigs),
          3,
          1000
      );
      if(originParallelism < 1) {
        throw new StageException(KafkaErrors.KAFKA_42, conf.topic);
      }
    }
    return originParallelism;
  }

  protected List<Record> processKafkaMessage(String partition, long offset, String messageId, byte[] payload) throws StageException {
    List<Record> records = new ArrayList<>();
    try (DataParser parser = parserFactory.getParser(messageId, payload)) {
      Record record = parser.parse();
      while (record != null) {
        record.getHeader().setAttribute(HeaderAttributeConstants.TOPIC, conf.topic);
        record.getHeader().setAttribute(HeaderAttributeConstants.PARTITION, partition);
        record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, String.valueOf(offset));

        records.add(record);
        record = parser.parse();
      }
    } catch (IOException|DataParserException ex) {
      Record record = getContext().createRecord(messageId);
      record.set(Field.create(payload));
      errorRecordHandler.onError(
          new OnRecordErrorException(
              record,
              KafkaErrors.KAFKA_37,
              messageId,
              ex.toString(),
              ex
          )
      );
    }
    if (conf.produceSingleRecordPerMessage) {
      List<Field> list = new ArrayList<>();
      for (Record record : records) {
        list.add(record.get());
      }
      Record record = records.get(0);
      record.set(Field.create(list));
      records.clear();
      records.add(record);
    }
    return records;
  }

}
