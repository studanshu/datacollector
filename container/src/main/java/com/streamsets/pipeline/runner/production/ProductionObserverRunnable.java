/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.production;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.alerts.AlertManager;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProductionObserverRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionObserverRunnable.class);

  private final ProductionPipelineManagerTask pipelineManager;
  private final BlockingQueue<Object> requestQueue;
  private volatile Thread runningThread;
  private final MetricRegistry metrics;
  private final ObserverRunner observerRunner;

  public ProductionObserverRunnable(ProductionPipelineManagerTask pipelineManager,
                                    BlockingQueue<Object> requestQueue, AlertManager alertManager,
                                    Configuration configuration) {
    this.pipelineManager = pipelineManager;
    this.requestQueue = requestQueue;
    this.metrics = this.pipelineManager.getMetrics();
    this.observerRunner = new ObserverRunner(metrics, alertManager, configuration);
  }

  @Override
  public void run() {
    Thread.currentThread().setName("ProductionObserverRunnable");
    runningThread = Thread.currentThread();
    while(true) {
      try {
        Object request = requestQueue.poll(1000, TimeUnit.MILLISECONDS);
        if(request != null) {
          if (request instanceof DataRulesEvaluationRequest) {
            //data monitoring
            observerRunner.handleDataRulesEvaluationRequest((DataRulesEvaluationRequest) request);
          } else if (request instanceof MetricRulesEvaluationRequest) {
            observerRunner.handleMetricRulesEvaluationRequest((MetricRulesEvaluationRequest) request);
          } else if (request instanceof RulesConfigurationChangeRequest) {
            //configuration changes
            observerRunner.handleConfigurationChangeRequest((RulesConfigurationChangeRequest) request);
          } else if (request instanceof  PipelineErrorNotificationRequest) {
            observerRunner.handlePipelineErrorNotificationRequest((PipelineErrorNotificationRequest)request);
          } else {
            LOG.error("Unknown request: " + request.getClass().getName());
          }
        }
      } catch(InterruptedException e){
        LOG.error("Stopping the Pipeline Observer, Reason: {}", e.getMessage(), e);
        runningThread = null;
        return;
      }
    }
  }

  public List<Record> getSampledRecords(String ruleId, int size) {
    return this.observerRunner.getSampledRecords(ruleId, size);
  }

}