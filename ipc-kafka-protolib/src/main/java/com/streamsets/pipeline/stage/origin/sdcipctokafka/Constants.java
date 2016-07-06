/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.sdcipctokafka;

// copy of com.streamsets.pipeline.stage.destination.sdcipc.Constants
public interface Constants {
  String X_SDC_APPLICATION_ID_HEADER = "X-SDC-APPLICATION-ID";
  String X_SDC_PING_HEADER = "X-SDC-PING";
  String X_SDC_PING_VALUE = "ping";
  String X_SDC_COMPRESSION_HEADER = "X-SDC-COMPRESSION";
  String SNAPPY_COMPRESSION = "snappy";
  String CONTENT_TYPE_HEADER = "Content-Type";
  String APPLICATION_BINARY = "application/binary";
  String X_SDC_JSON1_FRAGMENTABLE_HEADER = "X-SDC-JSON1-FRAGMENTABLE";

  String SSL_CERTIFICATE = "SunX509";
  String[] SSL_ENABLED_PROTOCOLS = {"TLSv1"};

  String PING_PATH = "/ping";

  String IPC_PATH = "/ipc/v1";
}
