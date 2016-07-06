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
package com.streamsets.pipeline.stage.processor.jython;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptingProcessorTestUtil;
import org.junit.Test;

public class TestJythonProcessor {

  @Test
  public void testOutErr() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.RECORD,
        "for record in records:\n" +
            "  output.write(record)\n" +
            "  record.value = 'Bye'\n" +
            "  output.write(record)\n" +
            "  record.value = 'Error'\n" +
            "  error.write(record, 'error')\n"
    );

    ScriptingProcessorTestUtil.verifyWriteErrorRecord(JythonDProcessor.class, processor);
  }

  @Test
  public void testJythonMapArray() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.RECORD,
        "output.write(records[0])\n" +
            "records[0].value = 'Hello'\n" +
            "output.write(records[0])\n" +
            "records[0].value = { 'foo' : 'FOO' };\n" +
            "output.write(records[0])\n" +
            "records[0].value = [ 5 ]\n" +
            "output.write(records[0])\n" +
            ""
    );

    ScriptingProcessorTestUtil.verifyMapAndArray(JythonDProcessor.class, processor);
  }

  private void testMode(ProcessingMode mode) throws Exception {
    Processor processor = new JythonProcessor(mode,
        "for record in records:\n" +
            "  output.write(record)");

    ScriptingProcessorTestUtil.verifyMode(JythonDProcessor.class, processor);
  }

  @Test
  public void testRecordMode() throws Exception {
    testMode(ProcessingMode.RECORD);
  }

  @Test
  public void testBatchMode() throws Exception {
    testMode(ProcessingMode.BATCH);
  }

  private void testRecordModeOnErrorHandling(OnRecordError onRecordError) throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.RECORD,
        "for record in records:\n" +
            "  if record.value == 'Hello':\n" +
            "    raise Exception()\n" +
            "  output.write(record)"
    );

    ScriptingProcessorTestUtil.verifyRecordModeOnErrorHandling(JythonDProcessor.class, processor, onRecordError);
  }


  @Test
  public void testRecordOnErrorDiscard() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.DISCARD);
  }

  @Test
  public void testRecordOnErrorToError() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.TO_ERROR);
  }

  @Test(expected = StageException.class)
  public void testRecordOnErrorStopPipeline() throws Exception {
    testRecordModeOnErrorHandling(OnRecordError.STOP_PIPELINE);
  }

  private void testBatchModeOnErrorHandling(OnRecordError onRecordError) throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.BATCH,
        "for record in records:\n" +
            "  if record.value == 'Hello':\n" +
            "    raise Exception()\n" +
            "  output.write(record)"
    );

    ScriptingProcessorTestUtil.verifyBatchModeOnErrorHandling(JythonDProcessor.class, processor, onRecordError);
  }


  @Test(expected = StageException.class)
  public void testBatchOnErrorDiscard() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.DISCARD);
  }

  @Test(expected = StageException.class)
  public void testBatchOnErrorToError() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.TO_ERROR);
  }

  @Test(expected = StageException.class)
  public void testBatchOnErrorStopPipeline() throws Exception {
    testBatchModeOnErrorHandling(OnRecordError.STOP_PIPELINE);
  }

  @Test
  public void testPrimitiveTypesPassthrough() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.RECORD,
        "import sys\n" +
            "from datetime import datetime\n" + // Verify that site.py was processed properly and std modules on path
            "for record in records:\n" +
            "  output.write(record)\n"
    );

    ScriptingProcessorTestUtil.verifyPrimitiveTypesPassthrough(JythonDProcessor.class, processor);
  }

  @Test
  public void testPrimitiveTypesFromScripting() throws Exception {
    Processor processor = new JythonProcessor(ProcessingMode.RECORD,
        "for record in records:\n" +
            "  record.value = [ 1, 5L, 0.5, True, 'hello' ]\n" +
            "  output.write(record)\n" +
            "  record.value = None\n" +
            "  output.write(record)\n" +
            "");
    ScriptingProcessorTestUtil.verifyPrimitiveTypesFromScripting(JythonDProcessor.class, processor);
  }

  @Test
  public void testStateObject() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.RECORD,
        "if not 'total_count' in state:\n" +
            "  state['total_count'] = 0\n" +
            "state['total_count'] = state['total_count'] + len(records)\n" +
            "for record in records:\n" +
            "  record.value['count'] = state['total_count']\n" +
            "  output.write(record)\n"
    );
    ScriptingProcessorTestUtil.verifyStateObject(JythonDProcessor.class, processor);
  }

  @Test
  public void testListMap() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.RECORD,
        "output.write(records[0])\n" +
            "records[0].value['Hello'] = 2\n" +
            "output.write(records[0])\n" +
            ""
    );
    ScriptingProcessorTestUtil.verifyListMap(JythonDProcessor.class, processor);
  }


  @Test
  public void testTypedNullPassThrough() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.BATCH,
        "for record in records:\n" +
            "  output.write(record)"
    );
    ScriptingProcessorTestUtil.verifyPreserveTypeForNullValue(JythonDProcessor.class, processor);
  }

  @Test
  public void testAssignNullToTypedField() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.BATCH,
        // record.value will be a list
        "for record in records:\n" +
            "  for r in record.value:\n" +
            "      r = None\n" +
            "  output.write(record)"
    );
    ScriptingProcessorTestUtil.verifyPreserveTypeForNullValue(JythonDProcessor.class, processor);
  }

  @Test
  public void testNestedMapWithNull() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.BATCH,
        "for record in records:\n" +
            "  for k in record.value['row1']:\n" +
            "      record.value['row1'][k] = None\n" +
            "  record.value['row2'] = None\n" +
            "  output.write(record)"
    );
    ScriptingProcessorTestUtil.verifyNestedMap(JythonDProcessor.class, processor);
  }

  @Test
  public void testChangeFieldTypeFromScripting() throws Exception {
    Processor processor = new JythonProcessor(
        ProcessingMode.BATCH,
        "from decimal import Decimal\n" +
        "from datetime import date\n" +
        "for record in records:\n" +
            "  record.value['int_long'] = 5L\n" +
            "  record.value['long_bool'] = True\n" +
            "  record.value['str_date'] = date.today()\n" +
            "  record.value['double_decimal'] = Decimal(1235.678)\n" +
            "  output.write(record)"
    );
    ScriptingProcessorTestUtil.verifyChangedTypeFromScripting(JythonDProcessor.class, processor);
  }

  @Test
  public void testListMapOrder() throws Exception {
    Processor processor = new JythonProcessor(ProcessingMode.RECORD,
        "records[0].value['A0'] = 0\n" +
            "records[0].value['A1'] = 1\n" +
            "records[0].value['A2'] = 2\n" +
            "records[0].value['A3'] = 3\n" +
            "records[0].value['A4'] = 4\n" +
            "records[0].value['A5'] = 5\n" +
            "records[0].value['A6'] = 6\n" +
            "records[0].value['A7'] = 7\n" +
            "records[0].value['A8'] = 8\n" +
            "records[0].value['A9'] = 9\n" +
            "records[0].value['A10'] = 10\n" +
            "records[0].value['A11'] = 11\n" +
            "records[0].value['A12'] = 12\n" +
            "records[0].value['A13'] = 13\n" +
            "records[0].value['A14'] = 14\n" +
            "records[0].value['A15'] = 15\n" +
            "records[0].value['A16'] = 16\n" +
            "records[0].value['A17'] = 17\n" +
            "records[0].value['A18'] = 18\n" +
            "records[0].value['A19'] = 19\n" +
            "output.write(records[0])\n" +
            "");
    ScriptingProcessorTestUtil.verifyListMapOrder(JythonDProcessor.class, processor);
  }
}
