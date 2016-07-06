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
package com.streamsets.datacollector.kafka.standalone;

import com.google.common.io.Resources;
import com.streamsets.datacollector.base.PipelineRunStandaloneIT;
import com.streamsets.pipeline.kafka.common.KafkaTestUtil;
import kafka.javaapi.producer.Producer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@Ignore
public class KafkaOriginMultiPartitionPipelineRunIT extends PipelineRunStandaloneIT {

  private static final String TOPIC = "TestKafkaOriginMultiPartition";

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    KafkaTestUtil.startZookeeper();
    KafkaTestUtil.startKafkaBrokers(5);
    KafkaTestUtil.createTopic(TOPIC, 3, 2);
    Producer<String, String> producer = KafkaTestUtil.createProducer(KafkaTestUtil.getMetadataBrokerURI(), false);
    producer.send(KafkaTestUtil.produceStringMessages(TOPIC, 3, 1000));
  }

  @After
  @Override
  public void tearDown() {
    KafkaTestUtil.shutdown();
  }

  @Override
  protected String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("kafka_origin_pipeline_standalone.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    pipelineJson = pipelineJson.replace("topicName", TOPIC);
    pipelineJson = pipelineJson.replaceAll("localhost:9092", KafkaTestUtil.getMetadataBrokerURI());
    pipelineJson = pipelineJson.replaceAll("localhost:2181", KafkaTestUtil.getZkConnect());
    return pipelineJson;
  }

  @Override
  protected int getRecordsInOrigin() {
    return 1000;
  }

  @Override
  protected int getRecordsInTarget() {
    return 1000;
  }

  @Override
  protected String getPipelineName() {
    return "kafka_origin_pipeline";
  }

  @Override
  protected String getPipelineRev() {
    return "0";
  }
}
