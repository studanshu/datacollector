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
package com.streamsets.pipeline.stage.origin.omniture;

import com.streamsets.pipeline.api.ConfigDef;

public class HttpProxyConfigBean {
  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Proxy URI",
      displayPosition = 10,
      group = "#0",
      dependsOn = "useProxy^",
      triggeredByValue = "true"
  )
  public String proxyUri = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Username",
      displayPosition = 20,
      group = "#0",
      dependsOn = "useProxy^",
      triggeredByValue = "true"
  )
  public String username = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Password",
      displayPosition = 30,
      group = "#0",
      dependsOn = "useProxy^",
      triggeredByValue = "true"
  )
  public String password = "";
}
