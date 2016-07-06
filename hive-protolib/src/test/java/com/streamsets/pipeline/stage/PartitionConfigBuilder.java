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
package com.streamsets.pipeline.stage;

import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.processor.hive.PartitionConfig;

import java.util.LinkedList;
import java.util.List;

/**
 * Helper class to build PartitionConfig list easily
 */
public class PartitionConfigBuilder {

  private List<PartitionConfig> partitions;

  public PartitionConfigBuilder() {
    partitions = new LinkedList<>();
  }

  public PartitionConfigBuilder addPartition(String name, HiveType type, String valueEL) {
    PartitionConfig p = new PartitionConfig();
    p.name = name;
    p.valueType = type;
    p.valueEL = valueEL;
    partitions.add(p);

    return this;
  }

  public List<PartitionConfig> build() {
    return partitions;
  }
}
