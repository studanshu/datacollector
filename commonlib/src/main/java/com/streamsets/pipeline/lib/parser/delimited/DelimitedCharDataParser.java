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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.lib.csv.OverrunCsvParser;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DelimitedCharDataParser extends AbstractDataParser {
  private final Stage.Context context;
  private final String readerId;
  private final OverrunCsvParser parser;
  private List<Field> headers;
  private boolean eof;
  private CsvRecordType recordType;

  public DelimitedCharDataParser(
      Stage.Context context,
      String readerId,
      OverrunReader reader,
      long readerOffset,
      int skipStartLines,
      CSVFormat format,
      CsvHeader header,
      int maxObjectLen,
      CsvRecordType recordType)
    throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.recordType = recordType;
    switch (header) {
      case WITH_HEADER:
        format = format.withHeader((String[])null).withSkipHeaderRecord(true);
        break;
      case IGNORE_HEADER:
        format = format.withHeader((String[])null).withSkipHeaderRecord(true);
        break;
      case NO_HEADER:
        format = format.withHeader((String[])null).withSkipHeaderRecord(false);
        break;
      default:
        throw new RuntimeException(Utils.format("Unknown header error: {}", header));
    }
    parser = new OverrunCsvParser(reader, format, readerOffset, skipStartLines, maxObjectLen);
    String[] hs = parser.getHeaders();
    if (header != CsvHeader.IGNORE_HEADER && hs != null) {
      headers = new ArrayList<>();
      for (String h : hs) {
        headers.add(Field.create(h));
      }
    }
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record = null;
    long offset = parser.getReaderPosition();
    String[] columns = parser.read();
    if (columns != null) {
      record = createRecord(offset, columns);
    } else {
      eof = true;
    }
    return record;
  }

  protected Record createRecord(long offset, String[] columns) throws DataParserException {
    Record record = context.createRecord(readerId + "::" + offset);

    if(recordType == CsvRecordType.LIST) {
      List<Field> row = new ArrayList<>();
      for (int i = 0; i < columns.length; i++) {
        Map<String, Field> cell = new HashMap<>();
        Field header = (headers != null) ? headers.get(i) : null;
        if (header != null) {
          cell.put("header", header);
        }
        Field value = Field.create(columns[i]);
        cell.put("value", value);
        row.add(Field.create(cell));
      }
      record.set(Field.create(row));
    } else {
      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
      for (int i = 0; i < columns.length; i++) {
        String key;
        Field header = (headers != null) ? headers.get(i) : null;
        if(header != null) {
          key = header.getValueAsString();
        } else {
          key = i + "";
        }
        listMap.put(key, Field.create(columns[i]));
      }
      record.set(Field.createListMap(listMap));
    }

    return record;
  }

  @Override
  public String getOffset() {
    return (eof) ? String.valueOf(-1) : String.valueOf(parser.getReaderPosition());
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
