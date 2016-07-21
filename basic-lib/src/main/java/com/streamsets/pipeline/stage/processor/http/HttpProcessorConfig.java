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
package com.streamsets.pipeline.stage.processor.http;

import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.DataFormatChooserValues;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.http.HttpProxyConfigBean;
import com.streamsets.pipeline.lib.http.OAuthConfigBean;
import com.streamsets.pipeline.lib.http.PasswordAuthConfigBean;
import com.streamsets.pipeline.lib.http.SslConfigBean;
import com.streamsets.pipeline.stage.origin.http.AuthenticationTypeChooserValues;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Bean specifying the configuration for an HttpProcessor instance.
 */
public class HttpProcessorConfig {
  @ConfigDefBean(groups = "HTTP")
  public DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Output Field",
      description = "Field in which to place the result of the HTTP request",
      displayPosition = 10,
      group = "HTTP"
  )
  @FieldSelectorModel(singleValued = true)
  public String outputField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data Format of the response. Response will be parsed before being placed in the record.",
      displayPosition = 15,
      group = "HTTP"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Resource URL",
      description = "The HTTP resource URL",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 20,
      group = "HTTP"
  )
  public String resourceUrl = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Headers",
      description = "Headers to include in the request",
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 30,
      elDefs = {RecordEL.class, VaultEL.class},
      group = "HTTP"
  )
  public Map<String, String> headers = new HashMap<>();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "HTTP Method",
      defaultValue = "GET",
      description = "HTTP method to send",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      displayPosition = 40,
      group = "HTTP"
  )
  @ValueChooserModel(HttpMethodChooserValues.class)
  public HttpMethod httpMethod = HttpMethod.GET;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "HTTP Method Expression",
      description = "Expression used to determine the HTTP method to use",
      displayPosition = 50,
      dependsOn = "httpMethod",
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      triggeredByValue = { "EXPRESSION" },
      group = "HTTP"
  )
  public String methodExpression = "";

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.TEXT,
      label = "Request Data",
      description = "Data that should be included as a part of the request",
      displayPosition = 60,
      lines = 2,
      dependsOn = "httpMethod",
      elDefs = {RecordEL.class, VaultEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      triggeredByValue = { "POST", "PUT", "DELETE", "EXPRESSION" },
      group = "HTTP"
  )
  public String requestBody = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Request Timeout",
      defaultValue = "1000",
      description = "HTTP request timeout in milliseconds.",
      displayPosition = 70,
      group = "HTTP"
  )
  public long requestTimeoutMillis = 1000;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Maximum parallel requests",
      defaultValue = "10",
      description = "Maximum number of requests to make in parallel.",
      displayPosition = 80,
      group = "HTTP"
  )
  public int numThreads = 10;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Authentication Type",
      defaultValue = "NONE",
      displayPosition = 90,
      group = "HTTP"
  )
  @ValueChooserModel(AuthenticationTypeChooserValues.class)
  public AuthenticationType authType = AuthenticationType.NONE;

  @ConfigDefBean(groups = "CREDENTIALS")
  public OAuthConfigBean oauth = new OAuthConfigBean();

  @ConfigDefBean(groups = "CREDENTIALS")
  public PasswordAuthConfigBean basicAuth = new PasswordAuthConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Proxy",
      defaultValue = "false",
      displayPosition = 100,
      group = "HTTP"
  )
  public boolean useProxy = false;

  @ConfigDefBean(groups = "PROXY")
  public HttpProxyConfigBean proxy = new HttpProxyConfigBean();

  @ConfigDefBean(groups = "SSL")
  public SslConfigBean sslConfig = new SslConfigBean();
}
