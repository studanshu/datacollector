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
package com.streamsets.pipeline.lib.parser.delimited;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.apache.commons.csv.CSVFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class TestDelimitedCharDataParser {

  private Stage.Context getContext() {
    return ContextInfoCreator.createSourceContext("i", false, OnRecordError.TO_ERROR, Collections.EMPTY_LIST);
  }

  @Test
  public void testParseNoHeader() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, 0, CSVFormat.DEFAULT,
      CsvHeader.NO_HEADER, -1, CsvRecordType.LIST);
    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("4", parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseNoHeaderWithListMap() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, 0, CSVFormat.DEFAULT,
      CsvHeader.NO_HEADER, -1, CsvRecordType.LIST_MAP);
    Assert.assertEquals("0", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::0", record.getHeader().getSourceId());
    Assert.assertEquals("A", record.get().getValueAsListMap().get("0").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsListMap().get("1").getValueAsString());
    Assert.assertEquals("4", parser.getOffset());
    record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsListMap().get("0").getValueAsString());
    Assert.assertEquals("b", record.get().getValueAsListMap().get("1").getValueAsString());
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseIgnoreHeader() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, 0, CSVFormat.DEFAULT,
      CsvHeader.IGNORE_HEADER, -1, CsvRecordType.LIST);
    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseIgnoreHeaderWithListMap() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, 0, CSVFormat.DEFAULT,
      CsvHeader.IGNORE_HEADER, -1, CsvRecordType.LIST_MAP);
    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsListMap().get("0").getValueAsString());
    Assert.assertEquals("b", record.get().getValueAsListMap().get("1").getValueAsString());
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithHeader() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, 0, CSVFormat.DEFAULT,
      CsvHeader.WITH_HEADER, -1, CsvRecordType.LIST);
    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("header").getValueAsString());
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("header").getValueAsString());
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithHeaderWithListMap() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, 0, CSVFormat.DEFAULT,
      CsvHeader.WITH_HEADER, -1, CsvRecordType.LIST_MAP);
    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsListMap().get("A").getValueAsString());
    Assert.assertEquals("b", record.get().getValueAsListMap().get("B").getValueAsString());
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseNoHeaderWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 4, 0, CSVFormat.DEFAULT,
      CsvHeader.NO_HEADER, -1, CsvRecordType.LIST);
    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseIgnoreHeaderWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 4, 0, CSVFormat.DEFAULT,
      CsvHeader.IGNORE_HEADER, -1, CsvRecordType.LIST);
    Assert.assertEquals("4", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::4", record.getHeader().getSourceId());
    Assert.assertEquals("a", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("b", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("7", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseIgnoreHeaderWithOffset2() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b\ne,f"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 8, 0, CSVFormat.DEFAULT,
      CsvHeader.IGNORE_HEADER, -1, CsvRecordType.LIST);
    Assert.assertEquals("8", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::8", record.getHeader().getSourceId());
    Assert.assertEquals("e", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[0]/header"));
    Assert.assertEquals("f", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertFalse(record.has("[1]/header"));
    Assert.assertEquals("11", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }

  @Test
  public void testParseWithHeaderWithOffset() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b\ne,f"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 8, 0, CSVFormat.DEFAULT,
      CsvHeader.WITH_HEADER, -1, CsvRecordType.LIST);
    Assert.assertEquals("8", parser.getOffset());
    Record record = parser.parse();
    Assert.assertNotNull(record);
    Assert.assertEquals("id::8", record.getHeader().getSourceId());
    Assert.assertEquals("e", record.get().getValueAsList().get(0).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("A", record.get().getValueAsList().get(0).getValueAsMap().get("header").getValueAsString());
    Assert.assertEquals("f", record.get().getValueAsList().get(1).getValueAsMap().get("value").getValueAsString());
    Assert.assertEquals("B", record.get().getValueAsList().get(1).getValueAsMap().get("header").getValueAsString());
    Assert.assertEquals("11", parser.getOffset());
    record = parser.parse();
    Assert.assertNull(record);
    Assert.assertEquals("-1", parser.getOffset());
    parser.close();
  }


  @Test(expected = IOException.class)
  public void testClose() throws Exception {
    OverrunReader reader = new OverrunReader(new StringReader("A,B\na,b"), 1000, true, false);
    DataParser parser = new DelimitedCharDataParser(getContext(), "id", reader, 0, 0, CSVFormat.DEFAULT,
      CsvHeader.IGNORE_HEADER, -1, CsvRecordType.LIST);
    parser.close();
    parser.parse();
  }

}
