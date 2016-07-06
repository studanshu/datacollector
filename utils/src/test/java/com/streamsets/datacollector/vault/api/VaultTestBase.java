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
package com.streamsets.datacollector.vault.api;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Rule;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.Header;

import java.io.IOException;

import static org.mockserver.model.Header.header;

public class VaultTestBase {

  @Rule
  public MockServerRule mockServerRule = new MockServerRule(this);

  protected MockServerClient mockServerClient;

  protected static final String token = "d25dd11c-ec80-b00c-31de-1c62222f356d";
  protected static final Header tokenHeader = header("X-Vault-Token", token);
  protected static final Header contentJson = header("Content-Type", "application/json");
  protected static final String localhost = "127.0.0.1";
  protected final String address = "http://" + localhost + ":" + mockServerRule.getPort();

  protected String getBody(String path) throws IOException {
    return Resources.toString(Resources.getResource(path), Charsets.UTF_8);
  }
}
