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
package com.streamsets.pipeline.stage.lib.aws;

import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class AWSConfig {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Access Key ID",
      description = "",
      defaultValue = "",
      displayPosition = -110,
      elDefs = VaultEL.class,
      group = "#0"
  )
  public String awsAccessKeyId;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Secret Access Key",
      description = "",
      defaultValue = "",
      displayPosition = -100,
      elDefs = VaultEL.class,
      group = "#0"
  )
  public String awsSecretAccessKey;

}
