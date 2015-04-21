/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.sdcrecord;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.common.SdcRecordDataFactoryUtil;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class SdcRecordDataParserFactory extends DataParserFactory {
  public static final Map<String, Object> CONFIGS = Collections.emptyMap();
  public static final Set<Class<? extends Enum>> MODES = Collections.emptySet();

  public SdcRecordDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, long offset) throws DataParserException {
    byte[] headerBytes = SdcRecordDataFactoryUtil.readHeader(is);
    byte formatNumber = headerBytes[0];
    switch(formatNumber) {
      case SdcRecordDataFactoryUtil.SDC_FORMAT_JSON_BYTE:
        return getJsonSdcRecordParser(id, is, offset);
      default:
        throw new DataParserException(Errors.SDC_RECORD_PARSER_02, formatNumber);
    }
  }

  private DataParser getJsonSdcRecordParser(String id, InputStream is, long offset) throws DataParserException {
    OverrunReader reader = createReader(is);
    Utils.checkState(reader.getPos() == 0, Utils.formatL("reader must be in position '0', it is at '{}'",
      reader.getPos()));
    try {
      return new JsonSdcRecordDataParser(getSettings().getContext(), reader, offset,
        getSettings().getMaxRecordLen());
    } catch (IOException ex) {
      throw new DataParserException(Errors.SDC_RECORD_PARSER_00, id, offset, ex.getMessage(), ex);
    }
  }
}