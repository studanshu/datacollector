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

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.kafka.api.SdcKafkaProducer;
import com.streamsets.pipeline.lib.kafka.exception.KafkaConnectionException;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.kafka.KafkaErrors;

import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class KafkaTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);

  private final KafkaConfigBean kafkaConfigBean;

  private long recordCounter = 0;
  private SdcKafkaProducer kafkaProducer;
  private ErrorRecordHandler errorRecordHandler;

  public KafkaTarget(KafkaConfigBean kafkaConfigBean) {
    this.kafkaConfigBean = kafkaConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    kafkaConfigBean.init(getContext(), issues);
    kafkaProducer = kafkaConfigBean.kafkaConfig.getKafkaProducer();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    if (kafkaConfigBean.kafkaConfig.singleMessagePerBatch) {
      writeOneMessagePerBatch(batch);
    } else {
      writeOneMessagePerRecord(batch);
    }
  }

  private void writeOneMessagePerBatch(Batch batch) throws StageException {
    int count = 0;
    //Map of topic->(partition->Records)
    Map<String, Map<String, List<Record>>> perTopic = new HashMap<>();
    Iterator<Record> records = batch.getRecords();
    while (records.hasNext()) {
      boolean topicError = true;
      boolean partitionError = true;
      Record record = records.next();
      String topic = null;
      String partitionKey = null;
      try {
        topic = kafkaConfigBean.kafkaConfig.getTopic(record);
        topicError = false;
        partitionKey = kafkaConfigBean.kafkaConfig.getPartitionKey(record, topic);
        partitionError = false;
      } catch (KafkaConnectionException ex) {
        //Kafka connection exception is thrown when the client cannot connect to the list of brokers
        //even after retrying with backoff as specified in the retry and backoff config options
        //In this case we fail pipeline.
        throw ex;
      } catch (StageException ex) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                ex.getErrorCode(),
                ex.getParams()
            )
        );
      }
      if(!topicError && !partitionError) {
        Map<String, List<Record>> perPartition = perTopic.get(topic);
        if (perPartition == null) {
          perPartition = new HashMap<>();
          perTopic.put(topic, perPartition);
        }
        List<Record> list = perPartition.get(partitionKey);
        if (list == null) {
          list = new ArrayList<>();
          perPartition.put(partitionKey, list);
        }
        list.add(record);
      }
    }
    if (!perTopic.isEmpty()) {
      for( Map.Entry<String, Map<String, List<Record>>> topicEntry : perTopic.entrySet()) {
        String entryTopic = topicEntry.getKey();
        Map<String, List<Record>> perPartition = topicEntry.getValue();
        if(perPartition != null) {
          for (Map.Entry<String, List<Record>> entry : perPartition.entrySet()) {
            String partition = entry.getKey();
            List<Record> list = entry.getValue();
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 * list.size());
            Record currentRecord = null;
            try {
              DataGenerator generator = kafkaConfigBean.dataGeneratorFormatConfig.getDataGeneratorFactory()
                .getGenerator(baos);
              for (Record record : list) {
                currentRecord = record;
                generator.write(record);
                count++;
              }
              currentRecord = null;
              generator.close();
              byte[] bytes = baos.toByteArray();
              kafkaProducer.enqueueMessage(entryTopic, bytes, partition);
            } catch (StageException ex) {
              errorRecordHandler.onError(
                  list,
                  new StageException(
                      ex.getErrorCode(),
                      ex.getParams()
                  )
              );
            } catch (IOException ex) {
              //clear the message list
              kafkaProducer.clearMessages();
              String sourceId = (currentRecord == null) ? "<NONE>" : currentRecord.getHeader().getSourceId();
              errorRecordHandler.onError(
                  list,
                  new StageException(
                      KafkaErrors.KAFKA_60,
                      sourceId,
                      batch.getSourceOffset(),
                      partition,
                      ex.toString(),
                      ex
                  )
              );
            }
            try {
              kafkaProducer.write();
            } catch (StageException ex) {
              if (ex.getErrorCode().getCode().equals(KafkaErrors.KAFKA_69.name())) {
                List<Exception> failedRecordException = (List<Exception>) ex.getParams()[1];
                Exception error = failedRecordException.get(0);
                errorRecordHandler.onError(
                    list,
                    new StageException(
                        KafkaErrors.KAFKA_60,
                        "<NONE>",
                        batch.getSourceOffset(),
                        partition,
                        error.toString(),
                        error
                    )
                );
              } else {
                throw ex;
              }
            }
            recordCounter += count;
            LOG.debug("Wrote {} records in this batch.", count);
          }
        }
      }
    }
  }

  private void writeOneMessagePerRecord(Batch batch) throws StageException {
    long count = 0;
    Iterator<Record> records = batch.getRecords();
    List<Record> recordList = new ArrayList<>();
    while (records.hasNext()) {
      Record record = records.next();
      recordList.add(record);
      try {
        String topic = kafkaConfigBean.kafkaConfig.getTopic(record);
        String partitionKey = kafkaConfigBean.kafkaConfig.getPartitionKey(record, topic);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataGenerator generator = kafkaConfigBean.dataGeneratorFormatConfig.getDataGeneratorFactory().getGenerator(baos);
        generator.write(record);
        generator.close();
        byte[] bytes = baos.toByteArray();
        kafkaProducer.enqueueMessage(topic, bytes, partitionKey);
        count++;
      } catch (KafkaConnectionException ex) {
        // Kafka connection exception is thrown when the client cannot connect to the list of brokers
        // even after retrying with backoff as specified in the retry and backoff config options
        // In this case we fail pipeline.
        throw ex;
      } catch (StageException ex) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                ex.getErrorCode(),
                ex.getParams()
            )
        );
      } catch (IOException ex) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                KafkaErrors.KAFKA_51,
                record.getHeader().getSourceId(),
                ex.toString(),
                ex
            )
        );
      }
    }
    try {
      kafkaProducer.write();
    } catch (StageException ex) {
      if (ex.getErrorCode().getCode().equals(KafkaErrors.KAFKA_69.name())) {
        List<Integer> failedRecordIndices = (List<Integer>) ex.getParams()[0];
        List<Exception> failedRecordExceptions = (List<Exception>) ex.getParams()[1];
        for (int i = 0; i < failedRecordIndices.size(); i++) {
          Record record = recordList.get(failedRecordIndices.get(i));
          Exception error = failedRecordExceptions.get(i);
          errorRecordHandler.onError(
              new OnRecordErrorException(
                  record,
                  KafkaErrors.KAFKA_51,
                  record.getHeader().getSourceId(),
                  error.toString(),
                  error
              )
          );
        }
      } else {
        throw ex;
      }
    }
    recordCounter += count;
    LOG.debug("Wrote {} records in this batch.", count);
  }

  @Override
  public void destroy() {
    LOG.info("Wrote {} number of records to Kafka Broker", recordCounter);
    kafkaConfigBean.destroy();
  }
}
