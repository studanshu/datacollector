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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestSolrTarget  extends SolrJettyTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestSolrTarget.class);

  @BeforeClass
  public static void beforeTest() throws Exception {
    File solrHomeDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(solrHomeDir.mkdirs());
    URL url = Thread.currentThread().getContextClassLoader().getResource("solr/");
    Assert.assertNotNull(url);
    FileUtils.copyDirectoryToDirectory(new File(url.toURI()), solrHomeDir);
    createJetty(solrHomeDir.getAbsolutePath() + "/solr", null, null);
  }

  @Test
  public void testValidations() throws Exception {
    String solrURI = jetty.getBaseUrl().toString() + "/" + "collection1";

    Target target = new SolrTarget(InstanceTypeOptions.SINGLE_NODE, null, null, ProcessingMode.BATCH, null, null);

    TargetRunner runner = new TargetRunner.Builder(SolrDTarget.class, target).build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(2, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.SOLR_00.name()));
    Assert.assertTrue(issues.get(1).toString().contains(Errors.SOLR_02.name()));

    target = new SolrTarget(InstanceTypeOptions.SOLR_CLOUD, null, null, ProcessingMode.BATCH, null, null);
    runner = new TargetRunner.Builder(SolrDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(2, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.SOLR_01.name()));
    Assert.assertTrue(issues.get(1).toString().contains(Errors.SOLR_02.name()));


    //Valid Solr URI
    target = new SolrTarget(InstanceTypeOptions.SINGLE_NODE, solrURI, null, ProcessingMode.BATCH, null, null);
    runner = new TargetRunner.Builder(SolrDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.SOLR_02.name()));


    List<SolrFieldMappingConfig> fieldNamesMap = new ArrayList<>();
    fieldNamesMap.add(new SolrFieldMappingConfig("/field", "solrFieldMapping"));
    target = new SolrTarget(InstanceTypeOptions.SINGLE_NODE, "invalidSolrURI", null, ProcessingMode.BATCH,
        fieldNamesMap, null);
    runner = new TargetRunner.Builder(SolrDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.SOLR_03.name()));
  }

  private Target createTarget() {
    String solrURI = jetty.getBaseUrl().toString() + "/" + "collection1";
    List<SolrFieldMappingConfig> fieldNamesMap = new ArrayList<>();
    fieldNamesMap.add(new SolrFieldMappingConfig("/a", "id"));
    fieldNamesMap.add(new SolrFieldMappingConfig("/a", "name"));
    fieldNamesMap.add(new SolrFieldMappingConfig("/b", "sku"));
    fieldNamesMap.add(new SolrFieldMappingConfig("/c", "manu"));
    fieldNamesMap.add(new SolrFieldMappingConfig("/titleMultiValued", "title"));
    return new SolrTarget(InstanceTypeOptions.SINGLE_NODE, solrURI, null, ProcessingMode.BATCH, fieldNamesMap, null);
  }


  @Test
  public void testWriteRecords() throws Exception {
    Target target = createTarget();
    TargetRunner runner = new TargetRunner.Builder(SolrDTarget.class, target).build();
    try {
      SolrServer solrClient = getSolrServer();

      solrClient.ping();

      //delete all index
      solrClient.deleteByQuery("*:*");

      runner.runInit();
      List<Record> records = new ArrayList<>();

      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of(
          "a", Field.create("Hello"),
          "b", Field.create("i"),
          "c", Field.create("t"),
          "titleMultiValued", Field.create(ImmutableList.of(Field.create("title1"), Field.create("title2")))
      )));

      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of(
          "a", Field.create("Bye"),
          "b", Field.create("i"),
          "c", Field.create("t"),
          "titleMultiValued", Field.create(ImmutableList.of(Field.create("title1"), Field.create("title2")))
      )));

      records.add(record1);
      records.add(record2);

      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());


      SolrQuery parameters = new SolrQuery();
      parameters.set("q", "name:Hello");
      QueryResponse queryResponse = solrClient.query(parameters);

      SolrDocumentList solrDocuments = queryResponse.getResults();
      Assert.assertEquals(1, solrDocuments.size());

      SolrDocument solrDocument = solrDocuments.get(0);
      String fieldAVal = (String) solrDocument.get("name");
      Assert.assertNotNull(fieldAVal);
      Assert.assertEquals("Hello", fieldAVal);

      String fieldBVal = (String) solrDocument.get("sku");
      Assert.assertNotNull(fieldBVal);
      Assert.assertEquals("i", fieldBVal);

      String fieldCVal = (String) solrDocument.get("manu");
      Assert.assertNotNull(fieldCVal);
      Assert.assertEquals("t", fieldCVal);


      List<String> titleCVal = (List<String>) solrDocument.get("title");
      Assert.assertNotNull(titleCVal);
      Assert.assertEquals(2, titleCVal.size());
      Assert.assertEquals("title1", titleCVal.get(0));
      Assert.assertEquals("title2", titleCVal.get(1));

    } catch (Exception e) {
      LOG.error("Exception while writing records", e);
      throw e;
    }
    finally {
      runner.runDestroy();
    }
  }


  @Test
  public void testWriteRecordsOnErrorDiscard() throws Exception {
    Target target = createTarget();
    TargetRunner runner = new TargetRunner.Builder(SolrDTarget.class, target).setOnRecordError(OnRecordError.DISCARD)
      .build();
    try {

      SolrServer solrClient = getSolrServer();

      //delete all index
      solrClient.deleteByQuery("*:*");

      runner.runInit();
      List<Record> records = new ArrayList<>();

      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("nota", Field.create("Hello"),
        "b", Field.create("i1"), "c", Field.create("t1"))));

      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
        "b", Field.create("i2"), "c2", Field.create("t2"))));

      records.add(record1);
      records.add(record2);

      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());

      SolrQuery parameters = new SolrQuery();
      parameters.set("q", "sku:i1");
      QueryResponse queryResponse = solrClient.query(parameters);

      SolrDocumentList solrDocuments = queryResponse.getResults();
      Assert.assertEquals(0, solrDocuments.size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWriteRecordsOnErrorToError() throws Exception {
    Target target = createTarget();
    TargetRunner runner = new TargetRunner.Builder(SolrDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    try {
      SolrServer solrClient = getSolrServer();

      //delete all index
      solrClient.deleteByQuery("*:*");

      runner.runInit();
      List<Record> records = new ArrayList<>();

      Record record1 = RecordCreator.create();
      // intentionally create Record with mismatching field name ("nota" instead of "a") to trigger an error
      record1.set(Field.create(ImmutableMap.of("nota", Field.create("Hello"),
        "b", Field.create("i1"), "c", Field.create("t1"))));

      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
        "b", Field.create("i2"), "c", Field.create("t2"))));

      records.add(record1);
      records.add(record2);

      runner.runWrite(records);
      Assert.assertEquals(2, runner.getErrorRecords().size());
      Assert.assertEquals("Hello", runner.getErrorRecords().get(0).get("/nota").getValueAsString());
      Assert.assertTrue(runner.getErrors().isEmpty());

      SolrQuery parameters = new SolrQuery();
      parameters.set("q", "sku:i1");
      QueryResponse queryResponse = solrClient.query(parameters);

      SolrDocumentList solrDocuments = queryResponse.getResults();
      Assert.assertEquals(0, solrDocuments.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWriteRecordsOnErrorToErrorDuringIndexing() throws Exception {

    //Create target without solr field id
    String solrURI = jetty.getBaseUrl().toString() + "/" + "collection1";
    List<SolrFieldMappingConfig> fieldNamesMap = new ArrayList<>();
    fieldNamesMap.add(new SolrFieldMappingConfig("/a", "name"));
    Target target = new SolrTarget(InstanceTypeOptions.SINGLE_NODE, solrURI, null, ProcessingMode.BATCH, fieldNamesMap
        , null);

    TargetRunner runner = new TargetRunner.Builder(SolrDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    try {
      SolrServer solrClient = getSolrServer();

      //delete all index
      solrClient.deleteByQuery("*:*");

      runner.runInit();
      List<Record> records = new ArrayList<>();

      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("nota", Field.create("Hello"),
        "b", Field.create("i1"), "c", Field.create("t1"))));

      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
        "b", Field.create("i2"), "c", Field.create("t2"))));

      records.add(record1);
      records.add(record2);

      runner.runWrite(records);
      Assert.assertEquals(2, runner.getErrorRecords().size());
      Assert.assertEquals("Hello", runner.getErrorRecords().get(0).get("/nota").getValueAsString());
      Assert.assertTrue(runner.getErrors().isEmpty());

      SolrQuery parameters = new SolrQuery();
      parameters.set("q", "sku:i1");
      QueryResponse queryResponse = solrClient.query(parameters);

      SolrDocumentList solrDocuments = queryResponse.getResults();
      Assert.assertEquals(0, solrDocuments.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testWriteRecordsOnErrorStopPipeline() throws Exception {
    Target target = createTarget();
    TargetRunner runner = new TargetRunner.Builder(SolrDTarget.class, target)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();

      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("nota", Field.create("Hello"),
        "b", Field.create("i1"), "c", Field.create("t1"))));

      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
        "b", Field.create("i2"), "c", Field.create("t2"))));

      records.add(record1);
      records.add(record2);

      runner.runWrite(records);
    } finally {
      runner.runDestroy();
    }
  }

}
