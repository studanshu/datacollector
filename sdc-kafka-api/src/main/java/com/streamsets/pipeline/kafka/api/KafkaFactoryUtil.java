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
package com.streamsets.pipeline.kafka.api;

import com.google.common.io.Resources;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Properties;

public class KafkaFactoryUtil {
  private KafkaFactoryUtil() {}

  public static String getFactoryClass(String factoryDefinitionFile, String factoryClassKey) {
    Properties def = new Properties();
    try {
      def.load(Resources.getResource(factoryDefinitionFile).openStream());
    } catch (Exception e) {
      throw new RuntimeException(
        Utils.format("Error creating an instance of SdcKafkaConsumerFactory : {}", e.toString()),
        e
      );
    }
    return def.getProperty(factoryClassKey);
  }
}
