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
package com.streamsets.pipeline.stage.origin.hdfs.cluster;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.configurablestage.DClusterSourceOffsetCommitter;

@StageDef(
  version = 4,
  label = "Hadoop FS",
  description = "Reads data from Hadoop file system",
  execution = ExecutionMode.CLUSTER_BATCH,
  libJarsRegex = {"avro-\\d+.*", "avro-mapred.*"},
  icon = "hdfs.png",
  privateClassLoader = true,
  upgrader = ClusterHdfsSourceUpgrader.class,
  onlineHelpRefUrl = "index.html#Origins/HadoopFS-origin.html#task_hgl_vgn_vs"
)
@ConfigGroups(value = Groups.class)
@HideConfigs(value = {"clusterHDFSConfigBean.dataFormatConfig.schemaInMessage", "clusterHDFSConfigBean.dataFormatConfig.compression"})
@GenerateResourceBundle
public class ClusterHdfsDSource extends DClusterSourceOffsetCommitter implements ErrorListener {
  private ClusterHdfsSource clusterHDFSSource;

  @ConfigDefBean
  public ClusterHdfsConfigBean clusterHDFSConfigBean;

  @Override
  protected Source createSource() {
    clusterHDFSSource = new ClusterHdfsSource(clusterHDFSConfigBean);
    return clusterHDFSSource;
  }

  @Override
  public Source getSource() {
    return clusterHDFSSource;
  }

  @Override
  public void errorNotification(Throwable throwable) {
    ClusterHdfsSource source = this.clusterHDFSSource;
    if (source != null) {
      source.errorNotification(throwable);
    }
  }

  @Override
  public void shutdown() {
    ClusterHdfsSource source = this.clusterHDFSSource;
    if (source != null) {
      source.shutdown();
    }
  }
}
