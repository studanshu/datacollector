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
package com.streamsets.pipeline.stage.processor.scripting;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;


/**
 * Common verification of scripting processor unit tests.
 * Processor test classes should define their appropriate test script.
 * Subsequently this utility can be called to run and verify the results of the scripts.
 */
public class ScriptingProcessorTestUtil {
  private ScriptingProcessorTestUtil() {}
  static final String JAVASCRIPT_CLASSNAME = "com.streamsets.pipeline.stage.processor.javascript.JavaScriptProcessor";

  public static <C extends Processor> void verifyWriteErrorRecord(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      record.set(Field.create("Hello"));
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertEquals("Hello", output.getRecords().get("lane").get(0).get().getValueAsString());
      Assert.assertEquals("Bye", output.getRecords().get("lane").get(1).get().getValueAsString());
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals("Error", runner.getErrorRecords().get(0).get().getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyMapAndArray(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(4, output.getRecords().get("lane").size());
      Record outRec = output.getRecords().get("lane").get(0);
      Assert.assertEquals(Field.create((String) null), outRec.get());
      outRec = output.getRecords().get("lane").get(1);
      Assert.assertEquals(Field.Type.STRING, outRec.get("/").getType());
      Assert.assertEquals("Hello", outRec.get("/").getValue());
      outRec = output.getRecords().get("lane").get(2);
      Assert.assertEquals(Field.Type.MAP, outRec.get("/").getType());
      Assert.assertEquals(Field.Type.STRING, outRec.get("/foo").getType());
      Assert.assertEquals("FOO", outRec.get("/foo").getValue());
      outRec = output.getRecords().get("lane").get(3);
      Assert.assertEquals(Field.Type.LIST, outRec.get("/").getType());
      // JavaScript only defines "Number" as a type which is a 64-bit float (double)
      if (System.getProperty("java.version").startsWith("1.7.")) {
        Assert.assertEquals(Field.Type.DOUBLE, outRec.get("[0]").getType());
        Assert.assertEquals(5.0, outRec.get("[0]").getValue());
      }
      // Java8's Nashorn engine however, will respect the original Java type of Integer.
      if (System.getProperty("java.version").startsWith("1.8")) {
        Assert.assertEquals(Field.Type.INTEGER, outRec.get("[0]").getType());
        Assert.assertEquals(5, outRec.get("[0]").getValue());
      }
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyMode(Class<C> clazz, Processor processor) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = Arrays.asList(record1, record2);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Assert.assertEquals("Hello", output.getRecords().get("lane").get(0).get().getValueAsString());
      Assert.assertEquals("Bye", output.getRecords().get("lane").get(1).get().getValueAsString());
      Assert.assertEquals(0, runner.getErrorRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyRecordModeOnErrorHandling(
      Class<C> clazz,
      Processor processor,
      OnRecordError onRecordError
  )
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .setOnRecordError(onRecordError)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = Arrays.asList(record1, record2);
      StageRunner.Output output = runner.runProcess(input);
      if (onRecordError == OnRecordError.DISCARD) {
        Assert.assertEquals(1, output.getRecords().get("lane").size());
        Assert.assertEquals("Bye", output.getRecords().get("lane").get(0).get().getValueAsString());
        Assert.assertEquals(0, runner.getErrorRecords().size());
      } else if (onRecordError == OnRecordError.TO_ERROR) {
        Assert.assertEquals(1, output.getRecords().get("lane").size());
        Assert.assertEquals("Bye", output.getRecords().get("lane").get(0).get().getValueAsString());
        Assert.assertEquals(1, runner.getErrorRecords().size());
        Assert.assertEquals("Hello", runner.getErrorRecords().get(0).get().getValueAsString());
      }
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyBatchModeOnErrorHandling(
      Class<C> clazz,
      Processor processor,
      OnRecordError onRecordError
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .setOnRecordError(onRecordError)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record1 = RecordCreator.create();
      record1.set(Field.create("Hello"));
      Record record2 = RecordCreator.create();
      record2.set(Field.create("Bye"));
      List<Record> input = Arrays.asList(record1, record2);
      runner.runProcess(input);
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyPrimitiveTypesPassthrough(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      List<Field> list = new ArrayList<>();
      list.add(Field.create(true));
      list.add(Field.create('a'));
      list.add(Field.create((byte)1));
      list.add(Field.create((short)2));
      list.add(Field.create(3)); //int
      list.add(Field.create((long)4));
      list.add(Field.create((float)5));
      list.add(Field.create((double)6));
      list.add(Field.createDate(new Date()));
      list.add(Field.create("string"));
      list.add(Field.create(new byte[]{1,2,3}));
      record.set(Field.create(list));
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals(record.get(), output.getRecords().get("lane").get(0).get());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyPrimitiveTypesFromScripting(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record = RecordCreator.create();
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(2, output.getRecords().get("lane").size());

      List<Field> list = new ArrayList<>();
      list.add(Field.create(1)); //int
      list.add(Field.create((long) 5));
      list.add(Field.create(0.5)); //double
      list.add(Field.create(true));
      list.add(Field.create("hello"));
      Field field = Field.create(list);
      Assert.assertEquals(field, output.getRecords().get("lane").get(0).get());

      Assert.assertEquals(Field.create((List) null), output.getRecords().get("lane").get(1).get());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyPreserveTypeForNullValue(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    Record record = RecordCreator.create();
    List<Field> list = new LinkedList<>();
    list.add(Field.create(Field.Type.INTEGER, null));
    list.add(Field.create(Field.Type.LONG, null));
    list.add(Field.create(Field.Type.DOUBLE, null));
    list.add(Field.create(Field.Type.FLOAT, null));
    list.add(Field.create(Field.Type.DECIMAL, null));
    list.add(Field.create(Field.Type.BOOLEAN, null));
    list.add(Field.create(Field.Type.STRING, null));
    list.add(Field.create(Field.Type.TIME, null));
    list.add(Field.createDate(null));

    Field field = Field.create(list);
    record.set(field);
    List<Record> input = Collections.singletonList(record);

    try {
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      List<Field> result = output.getRecords().get("lane").get(0).get().getValueAsList();
      for(int i = 0; i < list.size(); i++){
        Assert.assertEquals(list.get(i).getType(), result.get(i).getType());
      }
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyPrimitiveTypesFromScriptingJavaScript(
      Class<C> clazz,
      Processor processor
  ) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {

      Record record = RecordCreator.create();
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(2, output.getRecords().get("lane").size());

      List<Field> list = new ArrayList<>();
      // JavaScript only defines "Number" as a type which is a 64-bit float (double)
      if (System.getProperty("java.version").startsWith("1.7.")) {
        list.add(Field.create((double) 1)); //double
      }
      // Java8's Nashorn engine however, will respect the original Java type of Integer.
      if (System.getProperty("java.version").startsWith("1.8")) {
        list.add(Field.create(1)); // int
      }
      list.add(Field.create(0.5d));
      list.add(Field.create(true));
      list.add(Field.create("hello"));
      Field field = Field.create(list);
      Assert.assertEquals(field, output.getRecords().get("lane").get(0).get());

      Assert.assertEquals(Field.create((List) null), output.getRecords().get("lane").get(1).get());
    } finally {
      runner.runDestroy();
    }
  }

  /**
   * Incoming record has type and values, and script evaluator will modify the type
   * as well as actual values. This test checks the output record if types in all
   * fields are modified and have correct type.
   * @param clazz PythonProcessor, JavaScriptProcessor, or GroovyProcessor.
   * @param processor Processor
   * @param <C>
   */
  public static <C extends Processor> void verifyChangedTypeFromScripting(
      Class<C> clazz,
      Processor processor) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("int_long", Field.create(123));  // int
    map.put("long_bool", Field.create(12345L)); // long
    map.put("str_date", Field.create("today")); // String
    map.put("double_decimal", Field.create(123d)); // double
    record.set(Field.create(map));
    StageRunner.Output output;

    try {
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }

    Assert.assertEquals(1, output.getRecords().get("lane").size());
    Map<String, Field> outRec = output.getRecords().get("lane").get(0).get().getValueAsMap();
    if (!clazz.getName().equals(JAVASCRIPT_CLASSNAME)) {
      // JavaScript has ony "Number" type. No int, long, double, decimal types. So skip them.
      Assert.assertEquals(Field.Type.LONG, outRec.get("int_long").getType());
      Assert.assertEquals(Field.Type.DECIMAL, outRec.get("double_decimal").getType());
      // JavaScript fails this test because Date is Object type.
      Assert.assertEquals(Field.Type.DATE, outRec.get("str_date").getType());
    }
    Assert.assertEquals(Field.Type.BOOLEAN, outRec.get("long_bool").getType());
  }



  public static <C extends Processor> void verifyStateObject(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("count", Field.create(0));
      record.set(Field.create(map));
      List<Record> input = Collections.singletonList(record);
      runner.runProcess(input);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Assert.assertEquals(2, output.getRecords().get("lane").get(0).get("/count").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyStateObjectJavaScript(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Map<String, Field> map = new HashMap<>();
      map.put("count", Field.create(0));
      record.set(Field.create(map));
      List<Record> input = Collections.singletonList(record);
      runner.runProcess(input);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertEquals(1, output.getRecords().get("lane").size());
      // JavaScript only has a single number type, which is a double.
      Assert.assertEquals(2.0d, output.getRecords().get("lane").get(0).get("/count").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyListMap(Class<C> clazz, Processor processor) throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
      listMap.put("Hello", Field.create(1));

      Record record = RecordCreator.create();
      record.set(Field.createListMap(listMap));
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);

      Assert.assertEquals(2, output.getRecords().get("lane").size());
      Record outRec = output.getRecords().get("lane").get(0);
      Assert.assertEquals(Field.Type.LIST_MAP, outRec.get().getType());
      // In this case the type passthrough works because the scripting processor didn't modify this field.
      Assert.assertEquals(1, outRec.get("/Hello").getValue());
      Assert.assertEquals(1, outRec.get("[0]").getValue());
      outRec = output.getRecords().get("lane").get(1);
      Assert.assertEquals(Field.Type.LIST_MAP, outRec.get().getType());
      // JavaScript only defines "Number" as a type which is a 64-bit float (double)
      if (System.getProperty("java.version").startsWith("1.7.")) {
        Assert.assertEquals(2.0, outRec.get("/Hello").getValue());
        Assert.assertEquals(2.0, outRec.get("[0]").getValue());
      }
      // Java8's Nashorn engine however, will respect the original Java type of Integer.
      if (System.getProperty("java.version").startsWith("1.8")) {
        Assert.assertEquals(2, outRec.get("/Hello").getValue());
        Assert.assertEquals(2, outRec.get("[0]").getValue());
      }
    } finally {
      runner.runDestroy();
    }
  }

  public static <C extends Processor> void verifyListMapOrder(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    try {
      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
      for (int i = 0; i < 20; i++) {
        listMap.put("A" + i, Field.create(1));
      }

      Record record = RecordCreator.create();
      record.set(Field.createListMap(listMap));
      List<Record> input = Collections.singletonList(record);
      StageRunner.Output output = runner.runProcess(input);

      Assert.assertEquals(1, output.getRecords().get("lane").size());
      Record outRec = output.getRecords().get("lane").get(0);
      Assert.assertEquals(Field.Type.LIST_MAP, outRec.get().getType());

      Assert.assertEquals(20, outRec.get("/").getValueAsListMap().size());
      Assert.assertEquals(
          new ArrayList<>(listMap.keySet()),
          new ArrayList<>(outRec.get("/").getValueAsListMap().keySet())
      );
    } finally {
      runner.runDestroy();
    }
  }

  /**
   * Test for nested map. It changes the values in nested map and confirm that
   * the types are preserved.
   * @param clazz JythonEvaluator, JavaScripEvaluator or GroovyEvaluator.
   * @param processor Processor
   * @param <C>
   * @throws StageException
   */
  public static <C extends Processor> void verifyNestedMap(Class<C> clazz, Processor processor)
      throws StageException {
    ProcessorRunner runner = new ProcessorRunner.Builder(clazz, processor)
        .addOutputLane("lane")
        .build();
    runner.runInit();
    /*
       This is the setup. Test will change col1-col4 to null, and row2 to null.
       { row1: {
                 col1: true,  // boolean
                 col2: 10L,   // long
                 col3: today's date // Date
                 col4: 0.5d // double
              },
         row2 : {
                 col5: 0.5f  //float
              }
       }
     */
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    Map<String, Field> row1 = new HashMap<>();
    Map<String, Field> row2 = new HashMap<>();
    row1.put("col1", Field.create(true));
    row1.put("col2", Field.create(10L));
    row1.put("col3", Field.createDate(new Date()));
    row1.put("col4", Field.create(0.5d));
    row2.put("col5", Field.create(0.5f));
    map.put("row1", Field.create(row1));
    map.put("row2", Field.create(row2));

    record.set(Field.create(map));
    StageRunner.Output output;

    try {
      output = runner.runProcess(Collections.singletonList(record));
    } finally {
      runner.runDestroy();
    }

    Assert.assertEquals(1, output.getRecords().get("lane").size());
    Record outRec = output.getRecords().get("lane").get(0);
    Assert.assertEquals(Field.Type.MAP, outRec.get().getType());

    // All of the values in the "row1" map are null, but type should be preserved
    Map<String, Field> row1map = outRec.get().getValueAsMap().get("row1").getValueAsMap();
    for (Map.Entry<String, Field> r1 : row1map.entrySet()){
      Assert.assertEquals(r1.getValue().getType(), row1.get(r1.getKey()).getType());
    }
    // "row2" map is null, but the type should be preserved
    Assert.assertEquals(Field.Type.MAP, outRec.get().getValueAsMap().get("row2").getType());
    Assert.assertNull(outRec.get().getValueAsMap().get("row2").getValue());
  }
}
