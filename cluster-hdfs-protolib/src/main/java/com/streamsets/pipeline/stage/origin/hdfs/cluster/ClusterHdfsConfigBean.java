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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.List;
import java.util.Map;

public class ClusterHdfsConfigBean {

  public static final String CLUSTER_HDFS_CONFIG_BEAN_PREFIX = "clusterHDFSConfigBean.";

  @ConfigDefBean(groups = "HADOOP_FS")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Data Format",
      description = "Format of data in the files",
      displayPosition = 0,
      group = "HADOOP_FS"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Hadoop FS URI",
      description = "Include the HDFS scheme and authority: <scheme>://<authority> (for example" +
          " hdfs://nameservice). If this is not set, the URI will be set to the value of 'fs.defaultFS'" +
          " configuration.",
      displayPosition = 10,
      group = "HADOOP_FS")
  public String hdfsUri;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.LIST,
      defaultValue = "[]",
      label = "Input Paths",
      description = "Location of the input data to be read",
      displayPosition = 20,
      group = "HADOOP_FS"
  )
  public List<String> hdfsDirLocations; // hdfsDirLocation

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Include All Subdirectories",
      defaultValue = "true",
      description = "Reads all subdirectories within the input paths",
      displayPosition = 30,
      group = "HADOOP_FS"
  )
  public boolean recursive;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Produce Single Record",
      description = "Generates a single record for multiple objects within a message",
      displayPosition = 40,
      group = "HADOOP_FS")
  public boolean produceSingleRecordPerMessage;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Kerberos Authentication",
      defaultValue = "false",
      description = "",
      displayPosition = 50,
      group = "HADOOP_FS"
  )
  public boolean hdfsKerberos;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Hadoop FS Configuration Directory",
      description = "An SDC resource directory or symbolic link with HDFS configuration files core-site.xml, " +
          "hdfs-site.xml, yarn-site.xml, and mapred-site.xml",
      displayPosition = 60,
      group = "HADOOP_FS"
  )
  public String hdfsConfDir;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HDFS User",
      description = "If set, the data collector will read from HDFS as this user. " +
          "The data collector user must be configured as a proxy user in HDFS.",
      displayPosition = 70,
      group = "HADOOP_FS"
  )
  public String hdfsUser;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Hadoop FS Configuration",
      description = "Additional Hadoop properties to pass to the underlying Hadoop FileSystem",
      displayPosition = 80,
      group = "HADOOP_FS"
  )
  public Map<String, String> hdfsConfigs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Max Batch Size (records)",
      description = "Max number of records per batch",
      displayPosition = 90,
      group = "HADOOP_FS",
      min = 1,
      max = Integer.MAX_VALUE
  )
  public int maxBatchSize;
}
