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
package com.streamsets.datacollector.el;

import org.junit.Assert;
import org.junit.Test;

import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.el.JvmEL;
import com.streamsets.datacollector.el.RuleELRegistry;

public class TestJvmEL {

  @Test
  public void testMaxMemory() throws Exception {
    ELEvaluator eval = new ELEvaluator("x", JvmEL.class);
    ELVariables variables = new ELVariables();
    Assert.assertTrue(eval.eval(variables, "${jvm:maxMemoryMB()}", Long.class) > 0);
  }

  @Test
  public void testJvmELAvailViaRuleELRegistry() throws Exception {
    ELEvaluator eval = new ELEvaluator("x", RuleELRegistry.getRuleELs(RuleELRegistry.GENERAL));
    ELVariables variables = new ELVariables();
    Assert.assertTrue(eval.eval(variables, "${jvm:maxMemoryMB()}", Long.class) > 0);
  }

}
