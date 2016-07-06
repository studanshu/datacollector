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
package com.streamsets.datacollector.configupgrade;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.definition.StageLibraryDefinitionExtractor;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.base.BaseSource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestPipelineConfigurationUpgrader {
  private static int UPGRADE_CALLED;

  @Before
  public void setUp() {
    UPGRADE_CALLED = 0;
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  public static class Source1 extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  public static class Upgrader2 implements StageUpgrader {
    @Override
    public List<Config> upgrade(String library, String stageName, String stageInstance, int fromVersion, int toVersion,
        List<Config> configs) throws StageException {
      UPGRADE_CALLED++;
      configs.add(new Config("a", "A"));
      return configs;
    }
  }

  @StageDef(version = 1, label = "L", upgrader = Upgrader2.class, onlineHelpRefUrl = "")
  public static class Source2 extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  private static final StageLibraryDefinition LIBRARY_DEF =
      StageLibraryDefinitionExtractor.get().extract(TestPipelineConfigurationUpgrader.class.getClassLoader());

  private static final StageDefinition SOURCE1_DEF = StageDefinitionExtractor.get().extract(LIBRARY_DEF,
                                                                                            Source1.class, "");

  private static final StageDefinition SOURCE2_V1_DEF = StageDefinitionExtractor.get().extract(LIBRARY_DEF,
                                                                                               Source2.class, "");
  private static final StageDefinition SOURCE2_V2_DEF;

  static {
    SOURCE2_V2_DEF = Mockito.spy(SOURCE2_V1_DEF);
    Mockito.when(SOURCE2_V2_DEF.getVersion()).thenReturn(2);
  }

  private StageLibraryTask getLibrary(StageDefinition def) {
    StageLibraryTask library = Mockito.mock(StageLibraryTask.class);
    Mockito.when(library.getStage(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean())).thenReturn(def);
    return library;
  }

  @Test
  public void testNeedsUpgradeStage() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    StageConfiguration stageConf = new StageConfiguration("i", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                          SOURCE2_V1_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                          null, null);
    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(SOURCE2_V1_DEF, stageConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(SOURCE2_V2_DEF, stageConf, issues));
    Assert.assertTrue(issues.isEmpty());

    stageConf = new StageConfiguration("i", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                       SOURCE2_V2_DEF.getVersion(), Collections.EMPTY_LIST, null, null, null);

    // null def
    Assert.assertFalse(up.needsUpgrade(null, stageConf, issues));
    Assert.assertFalse(issues.isEmpty());

    // invalid downgrade
    issues.clear();
    Assert.assertFalse(up.needsUpgrade(SOURCE2_V1_DEF, stageConf, issues));
    Assert.assertFalse(issues.isEmpty());
  }

  public static class ForMockUpgrader extends PipelineConfigurationUpgrader {
  }

  ;

  public PipelineConfigurationUpgrader getPipelineV2Upgrader() {
    PipelineConfigurationUpgrader up = new ForMockUpgrader();
    StageDefinition pipelineDefV2 = Mockito.spy(up.getPipelineDefinition());
    Mockito.when(pipelineDefV2.getVersion()).thenReturn(PipelineConfigBean.VERSION + 1);
    Mockito.when(pipelineDefV2.getUpgrader()).thenReturn(new Upgrader2());
    up = Mockito.spy(up);
    Mockito.when(up.getPipelineDefinition()).thenReturn(pipelineDefV2);
    return up;
  }

  @Test
  public void testNeedsUpgradePipelineConfs() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    PipelineConfiguration pipelineConf = new PipelineConfiguration(1, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                                                   null, Collections.EMPTY_LIST, null,
                                                                   Collections.EMPTY_LIST, null, null);
    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE1_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(getPipelineV2Upgrader().needsUpgrade(getLibrary(SOURCE1_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    PipelineConfigurationUpgrader up0 = getPipelineV2Upgrader();
    StageDefinition pipelineDefV0 = Mockito.spy(up.getPipelineDefinition());
    Mockito.when(pipelineDefV0.getVersion()).thenReturn(0);
    Mockito.when(up0.getPipelineDefinition()).thenReturn(pipelineDefV0);

    Assert.assertFalse(up0.needsUpgrade(getLibrary(SOURCE1_DEF), pipelineConf, issues));
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testNeedsUpgradePipelineErrorStage() throws Exception {
    PipelineConfigurationUpgrader up = new ForMockUpgrader();

    StageConfiguration stageConf = new StageConfiguration("i", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                          SOURCE2_V1_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                          null, null);

    PipelineConfiguration pipelineConf = new PipelineConfiguration(1, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                                                   null, Collections.EMPTY_LIST, null,
                                                                   Collections.EMPTY_LIST, stageConf, null);
    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE2_V1_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    stageConf = new StageConfiguration("i", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                       SOURCE2_V2_DEF.getVersion(), Collections.EMPTY_LIST, null, null, null);
    pipelineConf = new PipelineConfiguration(1, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                             null, Collections.EMPTY_LIST, null,
                                             Collections.EMPTY_LIST, stageConf, null);
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE2_V1_DEF), pipelineConf, issues));
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testNeedsUpgradePipelineStage() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    StageConfiguration stageConf = new StageConfiguration("i", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                          SOURCE2_V1_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                          null, null);

    PipelineConfiguration pipelineConf = new PipelineConfiguration(1, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                                                   null, Collections.EMPTY_LIST, null,
                                                                   ImmutableList.of(stageConf), null, null);
    // no upgrade
    List<Issue> issues = new ArrayList<>();
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE2_V1_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    Assert.assertTrue(up.needsUpgrade(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // invalid downgrade
    stageConf = new StageConfiguration("i", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                       SOURCE2_V2_DEF.getVersion(), Collections.EMPTY_LIST, null, null, null);
    pipelineConf = new PipelineConfiguration(1, PipelineConfigBean.VERSION, UUID.randomUUID(),
                                             null, Collections.EMPTY_LIST, null,
                                             ImmutableList.of(stageConf), null, null);
    Assert.assertFalse(up.needsUpgrade(getLibrary(SOURCE2_V1_DEF), pipelineConf, issues));
    Assert.assertFalse(issues.isEmpty());

  }

  @Test
  public void testUpgradeStage() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    StageConfiguration stageConf = new StageConfiguration("i", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                          SOURCE2_V1_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                          null, null);
    List<Issue> issues = new ArrayList<>();

    Assert.assertTrue(up.needsUpgrade(SOURCE2_V2_DEF, stageConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    stageConf = up.upgrade(SOURCE2_V2_DEF, stageConf, issues);
    Assert.assertNotNull(stageConf);
    Assert.assertTrue(issues.isEmpty());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), stageConf.getStageVersion());
    Assert.assertEquals(1, UPGRADE_CALLED);
  }

  private PipelineConfiguration getPipelineUpToDate() {
    StageConfiguration stageConf1 = new StageConfiguration("i1", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                           SOURCE2_V2_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                           null, null);

    StageConfiguration stageConf2 = new StageConfiguration("i2", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                           SOURCE2_V2_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                           null, null);

    StageConfiguration errorConf = new StageConfiguration("e", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                          SOURCE2_V2_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                          null, null);

    return new PipelineConfiguration(1, PipelineConfigBean.VERSION, UUID.randomUUID(), null, Collections.EMPTY_LIST,
                                     null, ImmutableList.of(stageConf1, stageConf2), errorConf, null);
  }

  private PipelineConfiguration getPipelineToUpgrade() {
    StageConfiguration stageConf1 = new StageConfiguration("i1", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                           SOURCE2_V2_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                           Collections.EMPTY_LIST, null);

    StageConfiguration stageConf2 = new StageConfiguration("i2", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                           SOURCE2_V1_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                           Collections.EMPTY_LIST, null);

    StageConfiguration errorConf = new StageConfiguration("e", SOURCE2_V1_DEF.getLibrary(), SOURCE2_V1_DEF.getName(),
                                                          SOURCE2_V1_DEF.getVersion(), Collections.EMPTY_LIST, null,
                                                          Collections.EMPTY_LIST, null);

    return new PipelineConfiguration(1, PipelineConfigBean.VERSION, UUID.randomUUID(), null, Collections.EMPTY_LIST,
                                     null, ImmutableList.of(stageConf1, stageConf2), errorConf, null);
  }

  @Test
  public void testUpgradePipeline() throws Exception {
    PipelineConfigurationUpgrader up2 = getPipelineV2Upgrader();

    PipelineConfiguration pipelineConf = getPipelineToUpgrade();

    List<Issue> issues = new ArrayList<>();

    Assert.assertTrue(up2.needsUpgrade(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues));
    Assert.assertTrue(issues.isEmpty());

    // upgrade
    pipelineConf = up2.upgrade(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues);

    Assert.assertNotNull(pipelineConf);
    Assert.assertTrue(issues.isEmpty());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getErrorStage().getStageVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getStages().get(0).getStageVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getStages().get(1).getStageVersion());
    Assert.assertEquals(3, UPGRADE_CALLED);
    Assert.assertEquals(PipelineConfigBean.VERSION + 1, pipelineConf.getVersion());
    Assert.assertEquals("A", pipelineConf.getConfiguration("a").getValue());
    Assert.assertEquals(null, pipelineConf.getStages().get(0).getConfig("a"));
    Assert.assertEquals(1, pipelineConf.getStages().get(1).getConfiguration().size());
    Assert.assertEquals("A", pipelineConf.getStages().get(1).getConfig("a").getValue());
    Assert.assertEquals(1, pipelineConf.getErrorStage().getConfiguration().size());
    Assert.assertEquals("A", pipelineConf.getErrorStage().getConfig("a").getValue());
  }

  @Test
  public void testUpgradeIfNecessaryPipelineUpgrade() throws Exception {
    PipelineConfigurationUpgrader up2 = getPipelineV2Upgrader();

    PipelineConfiguration pipelineConf = getPipelineToUpgrade();

    List<Issue> issues = new ArrayList<>();

    pipelineConf = up2.upgradeIfNecessary(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues);

    Assert.assertNotNull(pipelineConf);
    Assert.assertTrue(issues.isEmpty());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getErrorStage().getStageVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getStages().get(0).getStageVersion());
    Assert.assertEquals(SOURCE2_V2_DEF.getVersion(), pipelineConf.getStages().get(1).getStageVersion());
    Assert.assertEquals(3, UPGRADE_CALLED);
    Assert.assertEquals(PipelineConfigBean.VERSION + 1, pipelineConf.getVersion());
    Assert.assertEquals("A", pipelineConf.getConfiguration("a").getValue());
    Assert.assertEquals(null, pipelineConf.getStages().get(0).getConfig("a"));
    Assert.assertEquals(1, pipelineConf.getStages().get(1).getConfiguration().size());
    Assert.assertEquals("A", pipelineConf.getStages().get(1).getConfig("a").getValue());
    Assert.assertEquals(1, pipelineConf.getErrorStage().getConfiguration().size());
    Assert.assertEquals("A", pipelineConf.getErrorStage().getConfig("a").getValue());
  }

  @Test
  public void testUpgradeIfNecessaryPipelineNoNeedTo() throws Exception {
    PipelineConfigurationUpgrader up = PipelineConfigurationUpgrader.get();

    List<Issue> issues = new ArrayList<>();
    PipelineConfiguration pipelineConf = getPipelineToUpgrade();
    PipelineConfiguration pipelineConf2 = up.upgradeIfNecessary(getLibrary(SOURCE2_V2_DEF), pipelineConf, issues);
    Assert.assertSame(pipelineConf, pipelineConf2);
  }

}
