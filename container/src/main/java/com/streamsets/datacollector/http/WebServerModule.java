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
package com.streamsets.datacollector.http;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.execution.EventListenerManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.publicrestapi.PublicRestAPI;
import com.streamsets.datacollector.restapi.RestAPI;
import com.streamsets.datacollector.restapi.configuration.BuildInfoInjector;
import com.streamsets.datacollector.restapi.configuration.ConfigurationInjector;
import com.streamsets.datacollector.restapi.configuration.PipelineStoreInjector;
import com.streamsets.datacollector.restapi.configuration.RestAPIResourceConfig;
import com.streamsets.datacollector.restapi.configuration.RuntimeInfoInjector;
import com.streamsets.datacollector.restapi.configuration.StageLibraryInjector;
import com.streamsets.datacollector.restapi.configuration.StandAndClusterManagerInjector;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.task.TaskWrapper;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.websockets.SDCWebSocketServlet;

import com.streamsets.lib.security.http.CORSConstants;
import com.streamsets.pipeline.http.MDCFilter;
import dagger.Module;
import dagger.Provides;
import dagger.Provides.Type;

import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.servlets.GzipFilter;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;

import javax.servlet.DispatcherType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Module(injects = {TaskWrapper.class, Manager.class}, library = true, complete = false)
public class WebServerModule {

  private final Manager mgr;

  public WebServerModule(Manager pipelineManager) {
    mgr = pipelineManager;
  }

  @Provides
  public Manager provideManager() {
    return mgr;
  }

  private final String SWAGGER_PACKAGE = "io.swagger.jaxrs.listing";

  @Provides(type = Type.SET_VALUES)
  Set<WebAppProvider> provideWebApps() {
    return Collections.emptySet();
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideStaticWeb(final RuntimeInfo runtimeInfo) {
    return new ContextConfigurator() {

      @Override
      public void init(ServletContextHandler context) {
        ServletHolder servlet = new ServletHolder(new DefaultServlet());
        servlet.setInitParameter("dirAllowed", "true");
        servlet.setInitParameter("resourceBase", runtimeInfo.getStaticWebDir());
        servlet.setInitParameter("cacheControl","max-age=0,public");
        context.addServlet(servlet, "/*");
      }

    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideMesosDir(final RuntimeInfo runtimeInfo) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        ServletHolder servlet = new ServletHolder(new DefaultServlet());
        // can't allow listing of dir as mesos dir will be hosting the jar file
        servlet.setInitParameter("dirAllowed", "false");
        servlet.setInitParameter("resourceBase", runtimeInfo.getDataDir());
        context.addServlet(servlet, "/mesos/*");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideGzipFilter() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        FilterHolder filter = new FilterHolder(GzipFilter.class);
        context.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST));
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideMDCFilter() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        FilterHolder filter = new FilterHolder(new MDCFilter());
        context.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST));
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideLocaleDetector() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        FilterHolder filter = new FilterHolder(new LocaleDetectorFilter());
        context.addFilter(filter, "/rest/*", EnumSet.of(DispatcherType.REQUEST));
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideJMX(final MetricRegistry metrics) {
    return new ContextConfigurator() {
      private JmxReporter reporter;
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute("com.codahale.metrics.servlets.MetricsServlet.registry", metrics);
        ServletHolder servlet = new ServletHolder(new JMXJsonServlet());
        context.addServlet(servlet, "/jmx");
      }

      @Override
      public void start() {
        reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
      }

      @Override
      public void stop() {
        if(reporter != null) {
          reporter.stop();
          reporter.close();
        }
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideLoginServlet() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        ServletHolder holderEvents = new ServletHolder(new LoginServlet());
        context.addServlet(holderEvents, "/login");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideCrossOriginFilter(final Configuration conf) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        FilterHolder crossOriginFilter = new FilterHolder(CrossOriginFilter.class);
        Map<String, String> params = new HashMap<>();

        params.put(CrossOriginFilter.ALLOWED_ORIGINS_PARAM,
            conf.get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_ORIGIN,
                CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_ORIGIN_DEFAULT));

        params.put(CrossOriginFilter.ALLOWED_METHODS_PARAM,
            conf.get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_METHODS,
                CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_METHODS_DEFAULT));

        params.put(CrossOriginFilter.ALLOWED_HEADERS_PARAM,
            conf.get(CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_HEADERS,
                CORSConstants.HTTP_ACCESS_CONTROL_ALLOW_HEADERS_DEFAULT));

        crossOriginFilter.setInitParameters(params);
        context.addFilter(crossOriginFilter, "/*", EnumSet.of(DispatcherType.REQUEST));
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideWebSocketServlet(final Configuration configuration, final RuntimeInfo runtimeInfo,
                                        final EventListenerManager eventListenerManager) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        ServletHolder holderEvents = new ServletHolder(new SDCWebSocketServlet(configuration, runtimeInfo,
          eventListenerManager));
        context.addServlet(holderEvents, "/rest/v1/webSocket");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideNoAuthenticationRoles(final Configuration configuration) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        if (configuration.get(WebServerTask.AUTHENTICATION_KEY, WebServerTask.AUTHENTICATION_DEFAULT).equals("none")) {
          FilterHolder filter = new FilterHolder(new AlwaysAllRolesFilter());
          context.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST));
        }
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideJersey() {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        // REST API that requires authentication
        ServletHolder protectedRest = new ServletHolder(new ServletContainer());
        protectedRest.setInitParameter(
            ServerProperties.PROVIDER_PACKAGES, SWAGGER_PACKAGE + "," +
            RestAPI.class.getPackage().getName()
        );
        protectedRest.setInitParameter(ServletProperties.JAXRS_APPLICATION_CLASS, RestAPIResourceConfig.class.getName());
        context.addServlet(protectedRest, "/rest/*");

        // REST API that it does not require authentication
        ServletHolder publicRest = new ServletHolder(new ServletContainer());
        publicRest.setInitParameter(ServerProperties.PROVIDER_PACKAGES, PublicRestAPI.class.getPackage().getName());
        publicRest.setInitParameter(ServletProperties.JAXRS_APPLICATION_CLASS, RestAPIResourceConfig.class.getName());
        context.addServlet(publicRest, "/public-rest/*");
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final PipelineStoreTask pipelineStore) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(PipelineStoreInjector.PIPELINE_STORE, pipelineStore);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final StageLibraryTask stageLibrary) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(StageLibraryInjector.STAGE_LIBRARY, stageLibrary);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStore(final Configuration configuration) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(ConfigurationInjector.CONFIGURATION, configuration);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator providePipelineStateManager(final Manager pipelineManager) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(StandAndClusterManagerInjector.PIPELINE_MANAGER_MGR, pipelineManager);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideRuntimeInfo(final RuntimeInfo runtimeInfo) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(RuntimeInfoInjector.RUNTIME_INFO, runtimeInfo);
      }
    };
  }

  @Provides(type = Type.SET)
  ContextConfigurator provideBuildInfo(final BuildInfo buildInfo) {
    return new ContextConfigurator() {
      @Override
      public void init(ServletContextHandler context) {
        context.setAttribute(BuildInfoInjector.BUILD_INFO, buildInfo);
      }
    };
  }

}
