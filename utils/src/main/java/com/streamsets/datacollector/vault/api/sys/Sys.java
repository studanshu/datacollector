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
package com.streamsets.datacollector.vault.api.sys;

import com.google.api.client.http.HttpTransport;
import com.streamsets.datacollector.vault.VaultConfiguration;
import com.streamsets.datacollector.vault.api.VaultException;

public class Sys {
  private final Lease lease;
  private final Auth auth;

  public Sys(VaultConfiguration conf, HttpTransport httpTransport) throws VaultException {
    this.lease = new Lease(conf, httpTransport);
    this.auth = new Auth(conf, httpTransport);
  }

  public Lease lease() {
    return lease;
  }
  public Auth auth() {
    return auth;
  }
}
