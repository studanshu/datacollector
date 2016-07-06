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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.streamsets.pipeline.api.Config;

import java.util.ArrayList;
import java.util.List;

public class AWSUtil {

  private AWSUtil() {}

  public static AWSCredentialsProvider getCredentialsProvider(AWSConfig config) {
    AWSCredentialsProvider credentialsProvider;
    if (!config.awsAccessKeyId.isEmpty() && !config.awsSecretAccessKey.isEmpty()) {
      credentialsProvider = new StaticCredentialsProvider(
          new BasicAWSCredentials(config.awsAccessKeyId, config.awsSecretAccessKey)
      );
    } else {
      credentialsProvider = new DefaultAWSCredentialsProviderChain();
    }
    return credentialsProvider;
  }

  public static ClientConfiguration getClientConfiguration(ProxyConfig config) {
    ClientConfiguration clientConfig = new ClientConfiguration();

    // Optional proxy settings
    if (config.useProxy) {
      if (config.proxyHost != null && !config.proxyHost.isEmpty()) {
        clientConfig.setProxyHost(config.proxyHost);
        clientConfig.setProxyPort(config.proxyPort);

        if (config.proxyUser != null && !config.proxyUser.isEmpty()) {
          clientConfig.setProxyUsername(config.proxyUser);
        }

        if (config.proxyPassword != null) {
          clientConfig.setProxyPassword(config.proxyPassword);
        }
      }
    }
    return clientConfig;
  }

  public static void renameAWSCredentialsConfigs(List<Config> configs) {
    List<Config> configsToRemove = new ArrayList<>();
    List<Config> configsToAdd = new ArrayList<>();

    for (Config config : configs) {
      switch (config.getName()) {
        case "s3ConfigBean.s3Config.accessKeyId":
          configsToRemove.add(config);
          configsToAdd.add(new Config("s3ConfigBean.s3Config.awsConfig.awsAccessKeyId", config.getValue()));
          break;
        case "s3ConfigBean.s3Config.secretAccessKey":
          configsToRemove.add(config);
          configsToAdd.add(new Config("s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey", config.getValue()));
          break;

        case "s3TargetConfigBean.s3Config.accessKeyId":
          configsToRemove.add(config);
          configsToAdd.add(new Config("s3TargetConfigBean.s3Config.awsConfig.awsAccessKeyId", config.getValue()));
          break;
        case "s3TargetConfigBean.s3Config.secretAccessKey":
          configsToRemove.add(config);
          configsToAdd.add(new Config("s3TargetConfigBean.s3Config.awsConfig.awsSecretAccessKey", config.getValue()));
          break;

        case "kinesisConfig.awsAccessKeyId":
          configsToRemove.add(config);
          configsToAdd.add(new Config("kinesisConfig.awsConfig.awsAccessKeyId", config.getValue()));
          break;
        case "kinesisConfig.awsSecretAccessKey":
          configsToRemove.add(config);
          configsToAdd.add(new Config("kinesisConfig.awsConfig.awsSecretAccessKey", config.getValue()));
          break;

        default:
          break;
      }
    }

    configs.removeAll(configsToRemove);
    configs.addAll(configsToAdd);
  }

}
