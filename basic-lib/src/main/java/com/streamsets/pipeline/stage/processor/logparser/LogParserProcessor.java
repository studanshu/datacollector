/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.logparser;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.OnParseError;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.lib.parser.log.LogDataFormatValidator;
import com.streamsets.pipeline.lib.parser.log.RegExConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogParserProcessor extends SingleLaneRecordProcessor {

  private final String fieldPathToParse;
  private final boolean removeCtrlChars;
  private final String parsedFieldPath;
  private final LogMode logMode;
  private final int logMaxObjectLen;
  private final String customLogFormat;
  private final String regex;
  private final List<RegExConfig> fieldPathsToGroupName;
  private final String grokPatternDefinition;
  private final String grokPattern;
  private final boolean enableLog4jCustomLogFormat;
  private final String log4jCustomLogFormat;
  private final OnParseError onParseError;
  private final int maxStackTraceLines;

  public LogParserProcessor(String fieldPathToParse, boolean removeCtrlChars, String parsedFieldPath, LogMode logMode,
      String customLogFormat,
      String regex, List<RegExConfig> fieldPathsToGroupName, String grokPatternDefinition,
      String grokPattern, boolean enableLog4jCustomLogFormat, String log4jCustomLogFormat) {
    this.fieldPathToParse = fieldPathToParse;
    this.removeCtrlChars = removeCtrlChars;
    this.parsedFieldPath = parsedFieldPath;
    this.logMode = logMode;
    this.logMaxObjectLen = -1;
    this.customLogFormat = customLogFormat;
    this.regex = regex;
    this.fieldPathsToGroupName = fieldPathsToGroupName;
    this.grokPatternDefinition = grokPatternDefinition;
    this.grokPattern = grokPattern;
    this.enableLog4jCustomLogFormat = enableLog4jCustomLogFormat;
    this.log4jCustomLogFormat = log4jCustomLogFormat;
    this.onParseError = OnParseError.ERROR;
    this.maxStackTraceLines = 0;
  }

  private LogDataFormatValidator logDataFormatValidator;
  private DataParserFactory parserFactory;

  @Override
  protected void init() throws StageException {
    super.init();
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();

    logDataFormatValidator = new LogDataFormatValidator(logMode, logMaxObjectLen,
      false, customLogFormat, regex, grokPatternDefinition, grokPattern,
      enableLog4jCustomLogFormat, log4jCustomLogFormat, onParseError, maxStackTraceLines,
      com.streamsets.pipeline.stage.origin.spooldir.Groups.LOG.name(),
      getFieldPathToGroupMap(fieldPathsToGroupName));
    logDataFormatValidator.validateLogFormatConfig(issues, getContext());

    DataParserFactoryBuilder builder = new DataParserFactoryBuilder(getContext(), DataParserFormat.LOG);
    builder.setOverRunLimit(1000000);
    builder.setRemoveCtrlChars(removeCtrlChars);
    logDataFormatValidator.populateBuilder(builder);

    try {
      parserFactory = builder.build();
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(null, null,
        com.streamsets.pipeline.stage.origin.spooldir.Errors.SPOOLDIR_24, ex.getMessage(), ex));
    }
    return issues;
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(fieldPathToParse);
    if (field == null) {
      throw new OnRecordErrorException(Errors.LOGP_00, record.getHeader().getSourceId(), fieldPathToParse);
    } else {
      String value = field.getValueAsString();
      if (value == null) {
        throw new OnRecordErrorException(Errors.LOGP_01, record.getHeader().getSourceId(), fieldPathToParse);
      }
      try (DataParser parser = parserFactory.getParser(record.getHeader().getSourceId(), value)){
        Record r = parser.parse();
        if(r != null) {
          Field parsed = r.get();
          record.set(parsedFieldPath, parsed);
        }
      } catch (IOException | DataParserException ex) {
        throw new OnRecordErrorException(Errors.LOGP_03, record.getHeader().getSourceId(), fieldPathToParse,
                                         ex.getMessage(), ex);
      }
      if (!record.has(parsedFieldPath)) {
        throw new OnRecordErrorException(Errors.LOGP_02, record.getHeader().getSourceId(), parsedFieldPath);
      }
      batchMaker.addRecord(record);
    }
  }

  private Map<String, Integer> getFieldPathToGroupMap(List<RegExConfig> fieldPathsToGroupName) {
    if(fieldPathsToGroupName == null) {
      return new HashMap<>();
    }
    Map<String, Integer> fieldPathToGroup = new HashMap<>();
    for(RegExConfig r : fieldPathsToGroupName) {
      fieldPathToGroup.put(r.fieldPath, r.group);
    }
    return fieldPathToGroup;
  }

}