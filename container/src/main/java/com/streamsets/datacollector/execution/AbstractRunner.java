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

package com.streamsets.datacollector.execution;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.alerts.EmailNotifier;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.util.ValidationUtil;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.api.StageException;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract  class AbstractRunner implements Runner {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRunner.class);

  @Inject protected EventListenerManager eventListenerManager;
  @Inject protected PipelineStoreTask pipelineStore;
  @Inject protected StageLibraryTask stageLibrary;
  @Inject protected RuntimeInfo runtimeInfo;
  @Inject protected Configuration configuration;

  protected PipelineConfiguration getPipelineConf(String name, String rev) throws PipelineStoreException,
    PipelineRunnerException {
    PipelineConfiguration load = pipelineStore.load(name, rev);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, load);
    PipelineConfiguration validate = validator.validate();
    if(validator.getIssues().hasIssues()) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0158, ValidationUtil.getFirstIssueAsString(name,
        validator.getIssues()));
    }
    return validate;
  }

  protected void registerEmailNotifierIfRequired(PipelineConfigBean pipelineConfigBean, String name, String rev) {
    //remove existing email notifier
    StateEventListener toRemove = null;
    List<StateEventListener> stateEventListenerList = eventListenerManager.getStateEventListenerList();
    for(StateEventListener s : stateEventListenerList) {
      if(s instanceof EmailNotifier &&
        ((EmailNotifier)s).getName().equals(name) &&
        ((EmailNotifier)s).getRev().equals(rev)) {
        toRemove = s;
      }
    }

    if(toRemove != null) {
      eventListenerManager.removeStateEventListener(toRemove);
    }

    //register new one if required
    if(pipelineConfigBean.notifyOnStates != null && !pipelineConfigBean.notifyOnStates.isEmpty() &&
      pipelineConfigBean.emailIDs != null && !pipelineConfigBean.emailIDs.isEmpty()) {
      Set<String> states = new HashSet<>();
      for(com.streamsets.datacollector.config.PipelineState s : pipelineConfigBean.notifyOnStates) {
        states.add(s.name());
      }
      EmailNotifier emailNotifier = new EmailNotifier(name, rev, runtimeInfo, new EmailSender(configuration),
        pipelineConfigBean.emailIDs, states);
      eventListenerManager.addStateEventListener(emailNotifier);
    }
  }

  protected boolean isRemotePipeline() throws PipelineStoreException {
    Object isRemote = getState().getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    // remote attribute will be null for pipelines with version earlier than 1.3
    return isRemote != null && (boolean) isRemote;
  }

  protected ScheduledFuture<Void> scheduleForRetries(ScheduledExecutorService runnerExecutor, long delay) {
    LOG.info("Scheduling retry in '{}' milliseconds", delay);
    ScheduledFuture<Void> future = runnerExecutor.schedule(new Callable<Void>() {
      @Override
      public Void call() throws StageException, PipelineException {
        LOG.info("Starting the runner now");
        prepareForStart();
        start();
        return null;
      }
    }, delay, TimeUnit.MILLISECONDS);
    return future;
  }
}
