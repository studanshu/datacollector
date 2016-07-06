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
package com.streamsets.datacollector.runner.production;

import com.streamsets.datacollector.runner.BatchImpl;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.datacollector.runner.StageRuntime;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;

import java.util.List;

public class StatsAggregationHandler {
  private static final String STATS_AGGREGATOR = "statsAggregator";
  private final StageRuntime statsAggregator;

  public StatsAggregationHandler(StageRuntime statsAggregator) {
    this.statsAggregator = statsAggregator;
  }

  public String getInstanceName() {
    return statsAggregator.getInfo().getInstanceName();
  }

  public List<Issue> init(StagePipe.Context context) {
    return  statsAggregator.init();
  }

  public void handle(String sourceOffset, List<Record> statsRecords) throws StageException {
    ((Target) statsAggregator.getStage()).write(new BatchImpl(STATS_AGGREGATOR, sourceOffset, statsRecords));
  }

  public void destroy() {
    statsAggregator.destroy();
  }

}
