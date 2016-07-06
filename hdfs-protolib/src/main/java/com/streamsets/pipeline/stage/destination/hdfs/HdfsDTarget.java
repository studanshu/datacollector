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
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

@StageDef(
    version = 3,
    label = "Hadoop FS",
    description = "Writes to a Hadoop file system",
    icon = "hdfs.png",
    privateClassLoader = true,
    upgrader = HdfsTargetUpgrader.class,
    onlineHelpRefUrl = "index.html#Destinations/HadoopFS-destination.html#task_m2m_skm_zq"
)
@ConfigGroups(Groups.class)
@HideConfigs(value = {"hdfsTargetConfigBean.dataGeneratorFormatConfig.includeSchema"})
@GenerateResourceBundle
public class HdfsDTarget extends DTarget {

  @ConfigDefBean
  public HdfsTargetConfigBean hdfsTargetConfigBean;

  @Override
  protected Target createTarget() {
    return new HdfsTarget(hdfsTargetConfigBean);
  }

}
