/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.ChooserValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CharsetChooserValues implements ChooserValues {
  private static final Logger LOG = LoggerFactory.getLogger(CharsetChooserValues.class);

  private static final List<String> VALUES;

  static {
    Set<String> set = new LinkedHashSet<>();
    set.add("UTF-8");
    set.add("US-ASCII");
    set.add("UTF-16");
    set.add("ISO-8859-1");
    set.add("IBM-500");
    set.add("GBK");
    Iterator<String> it = set.iterator();
    while (it.hasNext()) {
      String cs = it.next();
      if (!Charset.isSupported(cs)) {
        it.remove();
        LOG.warn("Charset '{}' is not supported", cs);
      }
    }
    for (Map.Entry<String, Charset> entry : Charset.availableCharsets().entrySet()) {
      set.add(entry.getKey());
    }
    VALUES = new ArrayList<>(set);
  }

  @Override
  public String getResourceBundle() {
    return null;
  }

  @Override
  public List<String> getValues() {
    return VALUES;
  }

  @Override
  public List<String> getLabels() {
    return VALUES;
  }
}