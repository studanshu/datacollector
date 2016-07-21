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
package com.streamsets.pipeline.stage.processor.listpivot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.OnStagePreConditionFailure;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestListPivot {

  @Test
  public void testListPivot() throws StageException {
    ListPivotProcessor processor = new ListPivotProcessor("/list_field", null, false, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(ListPivotDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      List<Field> listField = ImmutableList.of(
          Field.create("aval"),
          Field.create("bval"));
      map.put("list_field", Field.create(listField));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(2, output.getRecords().get("a").size());
      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof String);
      Assert.assertEquals(field.getValue(), "aval");
      field = output.getRecords().get("a").get(1).get();
      Assert.assertTrue(field.getValue() instanceof String);
      Assert.assertEquals(field.getValue(), "bval");
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testListPivotMap() throws StageException {
    ListPivotProcessor processor = new ListPivotProcessor("/list_field", null, false, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(ListPivotDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      List<Field> listField = ImmutableList.of(
          Field.create(ImmutableMap.of("a", Field.create("aval"))),
          Field.create(ImmutableMap.of("b", Field.create("bval"))));
      map.put("list_field", Field.create(listField));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(2, output.getRecords().get("a").size());

      Field field = output.getRecords().get("a").get(0).get();
      Assert.assertTrue(field.getValue() instanceof Map);
      Assert.assertTrue(field.getValueAsMap().containsKey("a"));
      Assert.assertEquals("aval", field.getValueAsMap().get("a").getValueAsString());
      Assert.assertTrue(!field.getValueAsMap().containsKey("b"));

      field = output.getRecords().get("a").get(1).get();
      Assert.assertTrue(field.getValueAsMap().containsKey("b"));
      Assert.assertEquals("bval", field.getValueAsMap().get("b").getValueAsString());
      Assert.assertTrue(!field.getValueAsMap().containsKey("a"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCopyFields() throws StageException {
    ListPivotProcessor processor = new ListPivotProcessor("/list_field", null, true, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(ListPivotDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      List<Field> listField = ImmutableList.of(
          Field.create(ImmutableMap.of("a", Field.create("aval"))),
          Field.create(ImmutableMap.of("b", Field.create("bval"))));
      map.put("list_field", Field.create(listField));
      map.put("copied", Field.create("rval"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(2, output.getRecords().get("a").size());

      Field field = output.getRecords().get("a").get(0).get();
      Field newListField = field.getValueAsMap().get("list_field");
      Assert.assertTrue(field.getValue() instanceof Map);
      Assert.assertTrue(newListField.getValueAsMap().containsKey("a"));
      Assert.assertTrue(!newListField.getValueAsMap().containsKey("b"));
      Assert.assertEquals("rval", field.getValueAsMap().get("copied").getValueAsString());

      field = output.getRecords().get("a").get(1).get();
      newListField = field.getValueAsMap().get("list_field");
      Assert.assertTrue(newListField.getValueAsMap().containsKey("b"));
      Assert.assertTrue(!newListField.getValueAsMap().containsKey("a"));
      Assert.assertEquals("rval", field.getValueAsMap().get("copied").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCopyFieldsNewPath() throws StageException {
    ListPivotProcessor processor = new ListPivotProcessor("/list_field", "/op", true, OnStagePreConditionFailure.CONTINUE);

    ProcessorRunner runner = new ProcessorRunner.Builder(ListPivotDProcessor.class, processor)
        .addOutputLane("a").build();
    runner.runInit();

    try {
      Map<String, Field> map = new LinkedHashMap<>();
      List<Field> listField = ImmutableList.of(
          Field.create(ImmutableMap.of("a", Field.create("aval"))),
          Field.create(ImmutableMap.of("b", Field.create("bval"))));
      map.put("list_field", Field.create(listField));
      map.put("copied", Field.create("rval"));
      Record record = RecordCreator.create("s", "s:1");
      record.set(Field.create(map));

      StageRunner.Output output = runner.runProcess(ImmutableList.of(record));
      Assert.assertEquals(2, output.getRecords().get("a").size());

      Field field = output.getRecords().get("a").get(0).get();
      Field newListField = field.getValueAsMap().get("op");
      Assert.assertTrue(field.getValue() instanceof Map);
      Assert.assertTrue(newListField.getValueAsMap().containsKey("a"));
      Assert.assertTrue(!newListField.getValueAsMap().containsKey("b"));
      Assert.assertEquals("rval", field.getValueAsMap().get("copied").getValueAsString());

      field = output.getRecords().get("a").get(1).get();
      newListField = field.getValueAsMap().get("op");
      Assert.assertTrue(newListField.getValueAsMap().containsKey("b"));
      Assert.assertTrue(!newListField.getValueAsMap().containsKey("a"));
      Assert.assertEquals("rval", field.getValueAsMap().get("copied").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }
}
