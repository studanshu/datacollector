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
package com.streamsets.pipeline.stage.origin.ipctokafka;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.destination.kafka.KafkaConfigBean;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@SuppressWarnings({"squid:S2095", "squid:S00112"})
public class IpcToKafkaServer {
  private static final Logger LOG = LoggerFactory.getLogger(IpcToKafkaServer.class);

  private final Stage.Context context;
  private final RpcConfigs configs;
  private final KafkaConfigBean kafkaConfigBean;
  private final int kafkaMaxMessageSize;
  private final BlockingQueue<Exception> errorQueue;
  private Server httpServer;
  private IpcToKafkaServlet servlet;

  public IpcToKafkaServer(
      Stage.Context context,
      RpcConfigs configs,
      KafkaConfigBean kafkaConfigBean,
      int kafkaMaxMessageSize,
      BlockingQueue<Exception> errorQueue
  ) {
    this.context = context;
    this.configs = configs;
    this.kafkaConfigBean = kafkaConfigBean;
    this.kafkaMaxMessageSize = kafkaMaxMessageSize;
    this.errorQueue = errorQueue;
  }

  private int getJettyServerThreads(int maxConcurrentRequests) {
    // per Jetty hardcoded logic, the minimum number of threads we can have is determined by the following formula
    int cores = Runtime.getRuntime().availableProcessors();
    int acceptors = Math.max(1, Math.min(4,cores/8));
    int selectors = (cores + 1) / 2;
    return acceptors + selectors + maxConcurrentRequests;
  }

  public void start() throws Exception {
    int maxThreads = getJettyServerThreads(configs.maxConcurrentRequests);
    int minThreads = Math.max(maxThreads / 2, getJettyServerThreads(1));

    QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, 60000,
                                                       new ArrayBlockingQueue<Runnable>(maxThreads));
    threadPool.setName("sdcipc-server");
    threadPool.setDaemon(true);
    Server server = new Server(threadPool);

    ServerConnector connector;
    if (configs.sslEnabled) {
      LOG.debug("Configuring over HTTPS");
      HttpConfiguration httpsConf = new HttpConfiguration();
      httpsConf.addCustomizer(new SecureRequestCustomizer());
      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setKeyStorePath(configs.getKeyStoreFile(context).getAbsolutePath());
      sslContextFactory.setKeyStorePassword(configs.keyStorePassword);
      sslContextFactory.setKeyManagerPassword(configs.keyStorePassword);
      connector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory, "http/1.1"),
                                      new HttpConnectionFactory(httpsConf));
    } else {
      LOG.debug("Configuring over HTTP");
      connector = new ServerConnector(server);
    }
    connector.setPort(configs.port);
    server.setConnectors(new Connector[]{connector});

    servlet = new IpcToKafkaServlet(context, configs, kafkaConfigBean, kafkaMaxMessageSize, errorQueue);
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.addServlet(new ServletHolder(servlet), Constants.IPC_PATH);
    contextHandler.setContextPath("/");
    server.setHandler(contextHandler);
    server.start();

    LOG.info("Running, port '{}', TLS '{}'", configs.port, configs.sslEnabled);

    httpServer = server;
  }

  public void stop() {
    LOG.info("Shutting down, port '{}', TLS '{}'", configs.port, configs.sslEnabled);
    if (httpServer != null) {
      try {
        servlet.setShuttingDown();
        httpServer.stop();
      } catch (Exception ex) {
        LOG.warn("Error while shutting down: {}", ex.toString(), ex);
      }
      httpServer = null;
    }
  }

}
