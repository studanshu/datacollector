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
package com.streamsets.pipeline.lib.parser.log;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexParser extends LogCharDataParser {

  private final Pattern pattern;
  private final Map<String, Integer> fieldToGroupMap;

  public RegexParser(Stage.Context context,
                     String readerId,
                     OverrunReader reader,
                     long readerOffset,
                     int maxObjectLen,
                     boolean retainOriginalText,
                     Pattern pattern,
                     Map<String, Integer> fieldToGroupMap,
                     StringBuilder currentLine,
                     StringBuilder previousLine
  ) throws IOException {
    super(context, readerId, reader, readerOffset, maxObjectLen, retainOriginalText, -1, currentLine, previousLine);
    this.fieldToGroupMap = fieldToGroupMap;
    this.pattern = pattern;
  }

  @Override
  protected Map<String, Field> parseLogLine(StringBuilder sb) throws DataParserException {
    Matcher m = pattern.matcher(sb.toString());
    if (!m.find()) {
      if (!m.find()) {
        throw new DataParserException(Errors.LOG_PARSER_03, sb.toString(), "Regular Expression - " + pattern.pattern());
      }
    }

    Map<String, Field> map = new HashMap<>();
    for(Map.Entry<String, Integer> e : fieldToGroupMap.entrySet()) {
      map.put(e.getKey(), Field.create(m.group(e.getValue())));
    }
    return map;
  }
}
