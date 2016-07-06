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
package com.streamsets.datacollector.validation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.LocalizableString;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Issue implements Serializable {
  private final String instanceName;
  private final String configGroup;
  private final String configName;
  private final LocalizableString message;
  private Map<String, Object> additionalInfo;

  protected Issue(String instanceName, String configGroup, String configName, ErrorCode error, Object... args) {
    this.instanceName = instanceName;
    this.configGroup = configGroup;
    this.configName = configName;
    message = new ErrorMessage(error, args);
  }

  public void setAdditionalInfo(String key, Object value) {
    if (additionalInfo == null) {
      additionalInfo = new HashMap<>();
    }
    additionalInfo.put(key, value);
  }

  public Map getAdditionalInfo() {
    return additionalInfo;
  }

  public String getMessage() {
    return message.getLocalized();
  }

  public String getErrorCode() {
    return ((ErrorMessage)message).getErrorCode();
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getLevel() {
    String level;
    if (instanceName == null) {
      level = (getConfigName() == null) ? "PIPELINE" : "PIPELINE_CONFIG";
    } else {
      level = (getConfigName() == null) ? "STAGE" : "STAGE_CONFIG";
    }
    return level;
  }

  public String getConfigGroup() {
    return configGroup;
  }

  public String getConfigName() {
    return configName;
  }

  @Override
  public String toString() {
    return Utils.format("Issue[instance='{}' group='{}' config='{}' message='{}']", instanceName, configGroup,
                        configName, message.getNonLocalized());
  }

}
