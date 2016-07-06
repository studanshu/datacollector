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
package com.streamsets.datacollector.cluster;

public class ClusterModeConstants {
  public static final String API_LIB = "api-lib";
  public static final String CONTAINER_LIB = "container-lib";
  public static final String STREAMSETS_LIBS = "streamsets-libs";
  public static final String USER_LIBS = "user-libs";

  public static final String NUM_EXECUTORS_KEY = "num-executors";
  public static final String CLUSTER_PIPELINE_NAME = "cluster.pipeline.name";
  public static final String CLUSTER_PIPELINE_REV = "cluster.pipeline.rev";
  public static final String CLUSTER_PIPELINE_USER = "cluster.pipeline.user";
  public static final String CLUSTER_PIPELINE_REMOTE = "cluster.pipeline.remote";

  public static final String SPARK_KAFKA_JAR_REGEX = "spark-streaming-kafka.*";
  public static final String AVRO_MAPRED_JAR_REGEX = "avro-mapred.*";
  public static final String AVRO_JAR_REGEX = "avro-\\d+.*";

  private ClusterModeConstants() {}
}
