/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.remote;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

public class RemoteDownloadSourceUpgrader implements StageUpgrader {
  private static final String CONF = "conf";
  private static final Joiner joiner = Joiner.on(".");
  @Override
  public List<Config> upgrade(String library, String stageName, String stageInstance,
                              int fromVersion, int toVersion, List<Config> configs) throws StageException {
    switch (fromVersion) {
      case 1:
        upgradeV1ToV2(configs);
        break;
      default:
        throw new IllegalStateException(Utils.format("Unexpected fromVersion {}", fromVersion));
    }
    return configs;
  }

  private void upgradeV1ToV2(List<Config> configs) {
    String pollingInterval = joiner.join(CONF, "pollInterval");
    Config confToRemove = null;
    for (Config conf: configs) {
      if (conf.getName().equals(pollingInterval)) {
        confToRemove = conf;
      }
    }
    configs.remove(confToRemove);
  }
}
