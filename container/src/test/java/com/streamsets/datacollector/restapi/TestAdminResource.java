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
package com.streamsets.datacollector.restapi;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.DPMInfoJson;
import com.streamsets.datacollector.util.Configuration;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class TestAdminResource extends JerseyTest {

  private static File confDir;

  @Test
  public void testEnableDPM() throws IOException {
    DPMInfoJson dpmInfo = new DPMInfoJson();
    dpmInfo.setBaseURL("http://dpmbaseURL");
    dpmInfo.setUserID("admin@admin");
    dpmInfo.setUserPassword("admin@admin");
    dpmInfo.setOrganization("admin");
    dpmInfo.setLabels(ImmutableList.of("l1", "l2"));
    boolean exceptionTriggered = false;
    Response response = null;
    try {
      response = target("/v1/system/enableDPM")
          .request()
          .header("X-Requested-By", "SDC")
          .post(Entity.json(dpmInfo));
    } catch (Exception e) {
      exceptionTriggered = true;
    } finally {
      if (response != null) {
        response.close();
      }
    }
    Assert.assertTrue(exceptionTriggered);

    // test for null check
    exceptionTriggered = false;
    response = null;
    try {
      response = target("/v1/system/enableDPM")
          .request()
          .header("X-Requested-By", "SDC")
          .post(null);
    } catch (ProcessingException e) {
      Assert.assertTrue(e.getCause().getMessage().contains("DPMInfo cannot be null"));
      exceptionTriggered = true;
    } finally {
      if (response != null) {
        response.close();
      }
    }
    Assert.assertTrue(exceptionTriggered);
  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new AdminResourceConfig());
        register(AdminResource.class);
      }
    };
  }

  static class AdminResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(RuntimeInfoTestInjector.class).to(RuntimeInfo.class);
      bindFactory(ConfigurationTestInjector.class).to(Configuration.class);
    }
  }

  static class RuntimeInfoTestInjector implements Factory<RuntimeInfo> {

    public RuntimeInfoTestInjector() {
    }

    @Singleton
    @Override
    public RuntimeInfo provide() {
      confDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
      confDir.mkdirs();
      Assert.assertTrue("Could not create: " + confDir, confDir.isDirectory());
      RuntimeInfo mock = Mockito.mock(RuntimeInfo.class);
      Mockito.when(mock.getConfigDir()).thenReturn(confDir.getAbsolutePath());
      return mock;
    }

    @Override
    public void dispose(RuntimeInfo runtimeInfo) {

    }
  }


  public static class ConfigurationTestInjector implements Factory<Configuration> {
    @Singleton
    @Override
    public Configuration provide() {
      Configuration configuration = Mockito.mock(Configuration.class);
      return configuration;
    }

    @Override
    public void dispose(Configuration configuration) {
    }

  }
}

