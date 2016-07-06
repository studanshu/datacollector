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
package com.streamsets.pipeline.stage.processor.groovy;

import com.streamsets.pipeline.stage.processor.scripting.AbstractScriptingProcessor;
import com.streamsets.pipeline.stage.processor.scripting.ProcessingMode;
import com.streamsets.pipeline.stage.processor.scripting.ScriptObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;

public class GroovyProcessor extends AbstractScriptingProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(GroovyProcessor.class);

  public static final String GROOVY_ENGINE = "groovy";

  public GroovyProcessor(ProcessingMode processingMode, String script) {
    super(LOG, GROOVY_ENGINE, Groups.GROOVY.name(), "script", processingMode, script);
  }

  @Override
  protected ScriptObjectFactory createScriptObjectFactory() {
    return new GroovyScriptObjectFactory(engine);
  }

  private static class GroovyScriptObjectFactory extends ScriptObjectFactory {
    public GroovyScriptObjectFactory(ScriptEngine engine) {
      super(engine);
    }
  }
}
