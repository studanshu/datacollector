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
package com.streamsets.pipeline.kafka.impl;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;

import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Ignore
public class TestSaslEnabledKafka extends SecureKafkaBase {

  private static final String JAAS_CONF =
      "KafkaServer {\n" +
      "  com.sun.security.auth.module.Krb5LoginModule required\n" +
      "  useKeyTab=\"true\"\n" +
      "  storeKey=\"true\"\n" +
      "  keyTab=\"" + "keyTabFile" + "\"\n" +
      "  principal=\"kafkaBroker/localhost\";\n" +
      "};\n" +
      "\n" +
      "KafkaClient {\n" +
      "  com.sun.security.auth.module.Krb5LoginModule required\n" +
      "  useKeyTab=\"true\"\n" +
      "  storeKey=\"true\"\n" +
      "  keyTab=\"" + "keyTabFile" + "\"\n" +
      "  principal=\"kafkaClient/localhost\";\n" +
      "};";
  public static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
  public static final String TEST_KEYTAB = "test.keytab";
  public static final String KDC = "kdc";
  public static final String KAFKA_BROKER_PRINCIPAL = "kafkaBroker/localhost";
  public static final String KAFKA_CLIENT_PRINCIPAL = "kafkaClient/localhost";
  public static final String KAFKA_JAAS_CONF = "kafka_jaas.conf";

  private static File testDir;
  private static File keytabFile;
  private static MiniKdc miniKdc;
  private static File jaasConfigFile;

  private static int plainTextPort;
  private static int securePort;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    Assert.assertTrue(testDir.mkdirs());

    File kdcDir = new File(testDir, KDC);
    Assert.assertTrue(kdcDir.mkdirs());
    keytabFile = new File(testDir, TEST_KEYTAB);

    miniKdc = new MiniKdc(MiniKdc.createConf(), kdcDir);
    miniKdc.start();
    miniKdc.createPrincipal(keytabFile, KAFKA_BROKER_PRINCIPAL, KAFKA_CLIENT_PRINCIPAL);

    jaasConfigFile = new File(testDir, KAFKA_JAAS_CONF);
    jaasConfigFile.createNewFile();
    jaasConfigFile.setReadable(true);
    String jaasConf = JAAS_CONF.replaceAll("keyTabFile", keytabFile.getAbsolutePath());
    FileOutputStream outputStream = new FileOutputStream(jaasConfigFile);
    IOUtils.write(jaasConf, outputStream);
    outputStream.close();

    plainTextPort = TestUtil.getFreePort();
    securePort = TestUtil.getFreePort();

    // reload configuration when getConfiguration is called next
    Configuration.setConfiguration(null);
    System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, jaasConfigFile.getAbsolutePath());

    SecureKafkaBase.beforeClass();
  }

  @AfterClass
  public static void afterClass() {
    SecureKafkaBase.afterClass();
    if (miniKdc != null) {
      miniKdc.stop();
      miniKdc = null;
    }
    System.clearProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG);
    Configuration.setConfiguration(null);
    if(jaasConfigFile.exists()) {
      jaasConfigFile.delete();
    }
    if(keytabFile.exists()) {
      keytabFile.delete();
    }
  }

  @Override
  protected void addBrokerSecurityConfig(Properties props) {
    props.setProperty("security.inter.broker.protocol", "SASL_PLAINTEXT");
    props.setProperty("sasl.kerberos.service.name", "kafkaBroker");
    StringBuilder listeners = new StringBuilder();
    listeners
      .append(String.format("PLAINTEXT://localhost:%d", getPlainTextPort()))
      .append(",")
      .append(String.format("SASL_PLAINTEXT://localhost:%d", getSecurePort()));
    // security config
    props.setProperty("listeners", listeners.toString());
  }

  @Override
  protected void addClientSecurityConfig(Map<String, Object> props) {
    props.put("security.protocol", "SASL_PLAINTEXT");
    props.put("sasl.kerberos.service.name", "kafkaBroker");
  }

  @Override
  protected int getPlainTextPort() {
    return plainTextPort;
  }

  @Override
  protected int getSecurePort() {
    return securePort;
  }

  @Override
  protected String getTopic() {
    return "TestSaslEnabledKafka";
  }
}
