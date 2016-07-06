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

import com.streamsets.pipeline.lib.el.TimeNowEL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

public class TestTimeNewEL {

  ELEvaluator eval;
  ELVariables variables;
  Date date;

  @Before
  public void setUp() {
    date = new Date();
    eval = new ELEvaluator("test", TimeNowEL.class);
    variables = new ELVariables();
    variables.addContextVariable(TimeNowEL.TIME_NOW_CONTEXT_VAR, date);
  }

  @Test
  public void testNow() throws Exception {
    Assert.assertEquals(date, eval.eval(variables, "${time:now()}", Date.class));
  }

  @Test
  public void testTrimDate() throws Exception {
    Date output = eval.eval(variables, "${time:trimDate(time:now())}", Date.class);
    Assert.assertEquals(1900, output.getYear());
    Assert.assertEquals(0, output.getMonth());
    Assert.assertEquals(1, output.getDate());
  }

  @Test
  public void testTrimTime() throws Exception {
    Date output = eval.eval(variables, "${time:trimTime(time:now())}", Date.class);
    Assert.assertEquals(0, output.getHours());
    Assert.assertEquals(0, output.getMinutes());
    Assert.assertEquals(0, output.getSeconds());
  }

}
