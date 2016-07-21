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
package com.streamsets.pipeline.stage.destination.solr;


import com.esotericsoftware.minlog.Log;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.util.JsonUtil;
import com.streamsets.pipeline.solr.api.Errors;
import com.streamsets.pipeline.solr.api.SdcSolrTarget;
import com.streamsets.pipeline.solr.api.SdcSolrTargetFactory;
import com.streamsets.pipeline.solr.api.TargetFactorySettings;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SolrTarget extends BaseTarget {
  private final static Logger LOG = LoggerFactory.getLogger(SolrTarget.class);

  private final InstanceTypeOptions instanceType;
  private final String solrURI;
  private final String zookeeperConnect;
  private final String defaultCollection;
  private final ProcessingMode indexingMode;
  private final List<SolrFieldMappingConfig> fieldNamesMap;

  private ErrorRecordHandler errorRecordHandler;
  private SdcSolrTarget sdcSolrTarget;

  public SolrTarget(final InstanceTypeOptions instanceType, final String solrURI, final String zookeeperConnect,
                    final ProcessingMode indexingMode, final List<SolrFieldMappingConfig> fieldNamesMap,
                    String defaultCollection) {
    this.instanceType = instanceType;
    this.solrURI = solrURI;
    this.zookeeperConnect = zookeeperConnect;
    this.defaultCollection = defaultCollection;
    this.indexingMode = indexingMode;
    this.fieldNamesMap = fieldNamesMap;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    boolean solrInstanceInfo = true;

    if(SolrInstanceType.SINGLE_NODE.equals(instanceType.getInstanceType()) && (solrURI == null || solrURI.isEmpty())) {
      solrInstanceInfo = false;
      issues.add(getContext().createConfigIssue(Groups.SOLR.name(), "solrURI", Errors.SOLR_00));
    } else if(SolrInstanceType.SOLR_CLOUD.equals(instanceType.getInstanceType()) &&
      (zookeeperConnect == null || zookeeperConnect.isEmpty())) {
      solrInstanceInfo = false;
      issues.add(getContext().createConfigIssue(Groups.SOLR.name(), "zookeeperConnect", Errors.SOLR_01));
    }

    if(fieldNamesMap == null || fieldNamesMap.isEmpty()) {
      issues.add(getContext().createConfigIssue(Groups.SOLR.name(), "fieldNamesMap", Errors.SOLR_02));
    }

    if (solrInstanceInfo) {
      TargetFactorySettings settings = new TargetFactorySettings(
          instanceType.toString(),
          solrURI,
          zookeeperConnect,
          defaultCollection
      );
      sdcSolrTarget = SdcSolrTargetFactory.create(settings).create();
      try {
        sdcSolrTarget.init();
      } catch (Exception ex) {
        String configName = "solrURI";
        if(InstanceTypeOptions.SOLR_CLOUD.equals(instanceType.getInstanceType())) {
          configName = "zookeeperConnect";
        }
        issues.add(getContext().createConfigIssue(Groups.SOLR.name(), configName, Errors.SOLR_03, ex.toString(), ex));
      }
    }

    return issues;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    List<Map<String, Object>> batchFieldMap = new ArrayList();
    List<Record> recordsBackup = new ArrayList<>();
    boolean atLeastOne = false;

    while (it.hasNext()) {
      atLeastOne = true;
      Record record = it.next();
      try {
        Map<String, Object> fieldMap = new HashMap();
        for(SolrFieldMappingConfig fieldMapping: fieldNamesMap) {
          Field field = record.get(fieldMapping.field);
          if (field == null) {
            throw new OnRecordErrorException(record, Errors.SOLR_06, fieldMapping.field);
          }
          fieldMap.put(fieldMapping.solrFieldName, JsonUtil.fieldToJsonObject(record, field));
        }

        if(ProcessingMode.BATCH.equals(indexingMode)) {
          batchFieldMap.add(fieldMap);
          recordsBackup.add(record);
        } else {
          sdcSolrTarget.add(fieldMap);
        }

      } catch (StageException ex) {
        errorRecordHandler.onError(
            new OnRecordErrorException(
                record,
                Errors.SOLR_04,
                record.getHeader().getSourceId(),
                ex.toString(),
                ex
            )
        );
      }
    }

    if(atLeastOne) {
      try {
        if(ProcessingMode.BATCH.equals(indexingMode)) {
          sdcSolrTarget.add(batchFieldMap);
        }
        sdcSolrTarget.commit();
      } catch (StageException ex) {
        try {
          sdcSolrTarget.rollback();
          errorRecordHandler.onError(recordsBackup, ex);
        } catch (StageException ex2) {
          errorRecordHandler.onError(recordsBackup, ex2);
        }
      }
    }
  }

  @Override
  public void destroy() {
    if(this.sdcSolrTarget != null){
      try {
        this.sdcSolrTarget.destroy();
      } catch (Exception e) {
        Log.error(e.toString());
      }
    }
    super.destroy();
  }
}
