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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.redis;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DSource;

@StageDef(
    version = 2,
    label = "Redis Consumer",
    description = "Reads data from Redis",
    icon = "redis.png",
    upgrader = RedisSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#Origins/Redis.html#task_dtz_npv_jw",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true

)
@ConfigGroups(value = Groups.class)
@HideConfigs(value = {"redisOriginConfigBean.dataFormatConfig.compression"})
@GenerateResourceBundle
public class RedisDSource extends DSource {

  @ConfigDefBean()
  public RedisOriginConfigBean redisOriginConfigBean;

  /**
   * {@inheritDoc}
   */
  @Override
  protected Source createSource() {
    return new RedisSubscriptionSource(redisOriginConfigBean);
  }
}
