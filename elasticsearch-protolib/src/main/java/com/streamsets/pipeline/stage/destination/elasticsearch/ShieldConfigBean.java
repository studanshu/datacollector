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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.streamsets.pipeline.api.ConfigDef;

public class ShieldConfigBean {

  public static final String CONF_PREFIX = ElasticSearchConfigBean.CONF_PREFIX + "shieldConfigBean.";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "username:password",
      label = "Shield Username/Password",
      description = "",
      dependsOn = "useShield^",
      triggeredByValue = "true",
      displayPosition = 10,
      group = "SHIELD"
  )
  public String shieldUser;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable SSL",
      defaultValue = "true",
      description = "Enable SSL on the transport and HTTP layers",
      dependsOn = "useShield^",
      triggeredByValue = "true",
      displayPosition = 20,
      group = "SHIELD"
  )
  public boolean shieldTransportSsl;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "SSL Keystore Path",
      description = "",
      dependsOn = "useShield^",
      triggeredByValue = "true",
      displayPosition = 30,
      group = "SHIELD"
  )
  public String sslKeystorePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "SSL Keystore Password",
      description = "",
      dependsOn = "useShield^",
      triggeredByValue = "true",
      displayPosition = 40,
      group = "SHIELD"
  )
  public String sslKeystorePassword;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "SSL Truststore Path",
      description = "",
      dependsOn = "useShield^",
      triggeredByValue = "true",
      displayPosition = 50,
      group = "SHIELD"
  )
  public String sslTruststorePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "SSL Truststore Password",
      description = "",
      dependsOn = "useShield^",
      triggeredByValue = "true",
      displayPosition = 60,
      group = "SHIELD"
  )
  public String sslTruststorePassword;
}
