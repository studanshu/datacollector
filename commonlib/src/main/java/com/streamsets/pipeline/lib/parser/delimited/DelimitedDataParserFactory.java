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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.util.DelimitedDataConstants;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.Set;

public class DelimitedDataParserFactory extends DataParserFactory {

  public static final Map<String, Object> CONFIGS =
      ImmutableMap.<String, Object>of(
          DelimitedDataConstants.DELIMITER_CONFIG, '|',
          DelimitedDataConstants.ESCAPE_CONFIG, '\\',
          DelimitedDataConstants.QUOTE_CONFIG, '"',
          DelimitedDataConstants.SKIP_START_LINES, 0
      );

  public static final Set<Class<? extends Enum>> MODES =
      ImmutableSet.of((Class<? extends Enum>) CsvMode.class, CsvHeader.class, CsvRecordType.class);

  public DelimitedDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return createParser(id, createReader(is), Long.parseLong(offset));
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    return createParser(id, createReader(reader), offset);
  }

  private DataParser createParser(String id, OverrunReader reader, long offset) throws DataParserException {
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
                                                         reader.getPos()));
    CSVFormat csvFormat = getSettings().getMode(CsvMode.class).getFormat();
    if (getSettings().getMode(CsvMode.class) == CsvMode.CUSTOM) {
      csvFormat = CSVFormat.DEFAULT.withDelimiter((char)getSettings().getConfig(DelimitedDataConstants.DELIMITER_CONFIG))
                                   .withEscape((char) getSettings().getConfig(DelimitedDataConstants.ESCAPE_CONFIG))
                                   .withQuote((char)getSettings().getConfig(DelimitedDataConstants.QUOTE_CONFIG));
    }
    try {
      return new DelimitedCharDataParser(
          getSettings().getContext(),
          id,
          reader,
          offset,
          (Integer) getSettings().getConfig(DelimitedDataConstants.SKIP_START_LINES),
          csvFormat,
          getSettings().getMode(CsvHeader.class),
          getSettings().getMaxRecordLen(),
          getSettings().getMode(CsvRecordType.class));
    } catch (IOException ex) {
      throw new DataParserException(Errors.DELIMITED_PARSER_00, id, offset, ex.toString(), ex);
    }
  }

}
