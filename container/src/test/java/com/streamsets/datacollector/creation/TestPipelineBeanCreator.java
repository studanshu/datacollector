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
package com.streamsets.datacollector.creation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.stagelibrary.ClassLoaderReleaser;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.StatsAggregatorStage;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.MultiValueChooserModel;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.api.base.BaseTarget;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestPipelineBeanCreator {

  private StageDefinition getStageDef() {
    StageDefinition def = Mockito.mock(StageDefinition.class);
    Mockito.when(def.isErrorStage()).thenReturn(false);
    return def;
  }

  public enum E { A, B }

  public class MyConfigBean {
    public List<E> enums;
  }

  @Test
  public void testToEnum() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toEnum(E.class, "A", getStageDef(), "g", "s", "c", issues);
    Assert.assertEquals(E.A, v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toEnum(E.class, "x", getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToString() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toString("A", getStageDef(), "g", "s", "c", issues);
    Assert.assertEquals("A", v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toString(1, getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testChar() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toChar("A", getStageDef(), "g", "s", "c", issues);
    Assert.assertEquals('A', v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toChar(1, getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    v = PipelineBeanCreator.get().toChar("", getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    v = PipelineBeanCreator.get().toChar("abc", getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToBoolean() {
    List<Issue> issues = new ArrayList<>();
    Object v = PipelineBeanCreator.get().toBoolean(true, getStageDef(), "g", "s", "c", issues);
    Assert.assertEquals(true, v);
    Assert.assertTrue(issues.isEmpty());

    v = PipelineBeanCreator.get().toBoolean(1, getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  @Test
  public void testToNumber() {
    List<Issue> issues = new ArrayList<>();
    Assert.assertEquals((byte) 1, PipelineBeanCreator.get().toNumber(Byte.TYPE, (byte) 1, getStageDef(),
                                                                     "g", "s", "c", issues));
    Assert.assertEquals((short) 1, PipelineBeanCreator.get().toNumber(Short.TYPE, (short) 1, getStageDef(),
                                                                      "g", "s", "c", issues));
    Assert.assertEquals((int) 1, PipelineBeanCreator.get().toNumber(Integer.TYPE, (int) 1, getStageDef(),
                                                                    "g", "s", "c", issues));
    Assert.assertEquals((long) 1, PipelineBeanCreator.get().toNumber(Long.TYPE, (long) 1, getStageDef(),
                                                                     "g", "s", "c", issues));
    Assert.assertEquals((float) 1, PipelineBeanCreator.get().toNumber(Float.TYPE, (float) 1, getStageDef(),
                                                                      "g", "s", "c", issues));
    Assert.assertEquals((double) 1, PipelineBeanCreator.get().toNumber(Double.TYPE, (double) 1, getStageDef(),
                                                                       "g", "s", "c", issues));
    Assert.assertEquals((byte) 1, PipelineBeanCreator.get().toNumber(Byte.class, new Byte((byte)1), getStageDef(),
                                                                     "g", "s", "c", issues));
    Assert.assertEquals((short) 1, PipelineBeanCreator.get().toNumber(Short.class, new Short((short)1), getStageDef(),
                                                                      "g", "s", "c", issues));
    Assert.assertEquals((int) 1, PipelineBeanCreator.get().toNumber(Integer.class, new Integer(1), getStageDef(),
                                                                    "g", "s", "c", issues));
    Assert.assertEquals((long) 1, PipelineBeanCreator.get().toNumber(Long.class, new Long(1), getStageDef(),
                                                                     "g", "s", "c", issues));
    Assert.assertEquals((float) 1, PipelineBeanCreator.get().toNumber(Float.class, new Float(1), getStageDef(),
                                                                      "g", "s", "c", issues));
    Assert.assertEquals((double) 1, PipelineBeanCreator.get().toNumber(Double.class, new Double(1), getStageDef(),
                                                                       "g", "s", "c", issues));
    Assert.assertTrue(issues.isEmpty());

    Object v = PipelineBeanCreator.get().toNumber(Byte.class, 'a', getStageDef(), "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());
  }

  public static class SubBean {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "A",
        required = true
    )
    public E subBeanEnum;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        defaultValue = "B",
        required = false
    )
    public String subBeanString;


    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"3\"]",
        required = true
    )
    public List<String> subBeanList;

  }

  public static class Bean {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        defaultValue = "1",
        required = false
    )
    public int beanInt;

    @ConfigDefBean
    public SubBean beanSubBean;

  }

  public static class EValueChooser extends BaseEnumChooserValues<E> {
    public EValueChooser() {
      super(E.class);
    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  public static class MyTarget extends BaseTarget {

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"1\"]",
        required = true
    )
    public List<String> list;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.LIST,
        defaultValue = "[\"2\"]",
        required = true,
        evaluation = ConfigDef.Evaluation.EXPLICIT
    )
    public List<String> listExplicit;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MAP,
        defaultValue = "{\"a\" : \"1\"}",
        required = true
    )
    public Map<String, String> map;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MAP,
        defaultValue = "{\"a\" : \"2\"}",
        required = true,
        evaluation = ConfigDef.Evaluation.EXPLICIT
    )
    public Map<String, String> mapExplicit;

    @ConfigDefBean
    public Bean bean;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ListBeanModel
    public List<Bean> complexField;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.STRING,
        required = true
    )
    public String stringJavaDefault = "Hello";

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.NUMBER,
        required = true
    )
    public int intJavaDefault = 5;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ValueChooserModel(EValueChooser.class)
    public E enumSJavaDefault = E.B;

    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @MultiValueChooserModel(EValueChooser.class)
    public List<E> enumMJavaDefault = Arrays.asList(E.A);


    @ConfigDef(
        label = "L",
        type = ConfigDef.Type.MODEL,
        required = true
    )
    @ValueChooserModel(EValueChooser.class)
    public E enumSNoDefaultAlAll;

    @Override
    public void write(Batch batch) throws StageException {

    }
  }

  @StageDef(version = 1, label = "L", onlineHelpRefUrl = "")
  @ErrorStage
  public static class ErrorMyTarget extends MyTarget {
  }

  @StageDef(version = 1, label = "A", onlineHelpRefUrl = "")
  @StatsAggregatorStage
  public static class AggregatingMyTarget extends MyTarget {
  }

  @Test
  public void testToList() throws NoSuchFieldException {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    ConfigDefinition configDef = stageDef.getConfigDefinition("list");
    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", "A");
    List<Issue> issues = new ArrayList<>();
    Object value = ImmutableList.of("${a}");
    Object v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues, null);
    Assert.assertEquals(ImmutableList.of("A"), v);
    Assert.assertTrue(issues.isEmpty());

    value = ImmutableList.of("A");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues, null);
    Assert.assertEquals(ImmutableList.of("A"), v);
    Assert.assertTrue(issues.isEmpty());

    value = Arrays.asList(null, "A");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues, null);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = ImmutableList.of("${a");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues, null);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = "x";
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues, null);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    //explicit EL eval
    issues.clear();
    configDef = stageDef.getConfigDefinition("listExplicit");
    value = ImmutableList.of("${a}");
    v = PipelineBeanCreator.get().toList(value, stageDef, configDef, constants, "g", "s", "c", issues, null);
    Assert.assertEquals(ImmutableList.of("${a}"), v);
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testToEnumList() throws NoSuchFieldException {
    List<Issue> issues = new ArrayList<>();
    Object value = ImmutableList.of("A");
    Object v = PipelineBeanCreator.get().toList(value, Mockito.mock(StageDefinition.class),
      Mockito.mock(ConfigDefinition.class), null, "g", "s", "c", issues, MyConfigBean.class.getField("enums"));
    Assert.assertEquals(ImmutableList.of(E.A), v);
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testToMap() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    ConfigDefinition configDef = stageDef.getConfigDefinition("map");
    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();
    Object value = ImmutableList.of(ImmutableMap.of("key", "a", "value", 2));
    Object v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertEquals(ImmutableMap.of("a", 2), v);
    Assert.assertTrue(issues.isEmpty());


    value = ImmutableList.of(ImmutableMap.of("key", "a", "value", "${a}"));
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertEquals(ImmutableMap.of("a", "1"), v);
    Assert.assertTrue(issues.isEmpty());

    Map map = new HashMap();
    map.put("key", null);
    map.put("value", "A");
    value = ImmutableList.of(map);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    map.put("key", "a");
    map.put("value", null);
    value = ImmutableList.of(map);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = "x";
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    issues.clear();
    value = ImmutableList.of(1);
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNull(v);
    Assert.assertFalse(issues.isEmpty());

    //explicit EL eval
    issues.clear();
    configDef = stageDef.getConfigDefinition("mapExplicit");
    value = ImmutableList.of(ImmutableMap.of("key", "a", "value", "${a}"));
    v = PipelineBeanCreator.get().toMap(value, stageDef, configDef, constants, "g", "s", "c", issues);
    Assert.assertNotNull(v);
    Assert.assertEquals(ImmutableMap.of("a", "${a}"), v);
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testCreateAndInjectStageUsingDefaults() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");

    StageConfiguration stageConf = new StageConfiguration("i", "l", "n", 1, Collections.<Config>emptyList(),
                                                          Collections.<String, Object>emptyMap(),
                                                          Collections.<String>emptyList(),
                                                          Collections.<String>emptyList(),
                                                          Collections.<String>emptyList());

    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();

    StageBean bean = PipelineBeanCreator.get().createStage(stageDef, Mockito.mock(ClassLoaderReleaser.class), stageConf,
                                                           constants, issues);

    Assert.assertNotNull(bean);
    MyTarget stage = (MyTarget) bean.getStage();
    Assert.assertEquals(ImmutableList.of("1"), stage.list);
    Assert.assertEquals(ImmutableList.of("2"), stage.listExplicit);
    Assert.assertEquals(ImmutableMap.of("a", "1"), stage.map);
    Assert.assertEquals(ImmutableMap.of("a", "2"), stage.mapExplicit);
    Assert.assertEquals(1, stage.bean.beanInt);
    Assert.assertEquals(E.A, stage.bean.beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("3"), stage.bean.beanSubBean.subBeanList);
    Assert.assertEquals("B", stage.bean.beanSubBean.subBeanString);
    Assert.assertEquals(Collections.emptyList(), stage.complexField);
  }

  @Test
  public void testCreateAndInjectStageUsingMixOfDefaultsAndConfig() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");

    List<Config> configConfs = ImmutableList.of(
        new Config("list", ImmutableList.of("X")),
        new Config("map", ImmutableList.of(ImmutableMap.of("key", "a", "value", "AA"))),
        new Config("bean.beanInt", new Long(3)),
        new Config("bean.beanSubBean.subBeanEnum", "A"),
        new Config("bean.beanSubBean.subBeanString", "AA"),
        new Config("complexField", ImmutableList.of(ImmutableMap.of(
            "beanInt", 4,
            "beanSubBean.subBeanEnum", "A",
            "beanSubBean.subBeanList", ImmutableList.of("a", "b"),
            "beanSubBean.subBeanString", "X")))
    );
    StageConfiguration stageConf = new StageConfiguration("i", "l", "n", 1, configConfs,
                                                          Collections.<String, Object>emptyMap(),
                                                          Collections.<String>emptyList(),
                                                          Collections.<String>emptyList(),
                                                          Collections.<String>emptyList());

    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();

    StageBean bean = PipelineBeanCreator.get().createStage(stageDef, Mockito.mock(ClassLoaderReleaser.class), stageConf,
                                                           constants, issues);

    Assert.assertNotNull(bean);
    MyTarget stage = (MyTarget) bean.getStage();
    Assert.assertEquals(ImmutableList.of("X"), stage.list);
    Assert.assertEquals(ImmutableList.of("2"), stage.listExplicit);
    Assert.assertEquals(ImmutableMap.of("a", "AA"), stage.map);
    Assert.assertEquals(ImmutableMap.of("a", "2"), stage.mapExplicit);
    Assert.assertEquals(3, stage.bean.beanInt);
    Assert.assertEquals(E.A, stage.bean.beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("3"), stage.bean.beanSubBean.subBeanList);
    Assert.assertEquals("AA", stage.bean.beanSubBean.subBeanString);
    Assert.assertEquals(1, stage.complexField.size());
    Assert.assertEquals(4, stage.complexField.get(0).beanInt);
    Assert.assertEquals(E.A, stage.complexField.get(0).beanSubBean.subBeanEnum);
    Assert.assertEquals(ImmutableList.of("a", "b"), stage.complexField.get(0).beanSubBean.subBeanList);
    Assert.assertEquals("X", stage.complexField.get(0).beanSubBean.subBeanString);
  }

  @Test
  public void testCreatePipelineBean() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    StageDefinition errorStageDef = StageDefinitionExtractor.get().extract(libraryDef, ErrorMyTarget.class, "");
    StageDefinition aggStageDef = StageDefinitionExtractor.get().extract(libraryDef, AggregatingMyTarget.class, "");
    StageLibraryTask library = Mockito.mock(StageLibraryTask.class);
    Mockito.when(library.getStage(Mockito.eq("l"), Mockito.eq("s"), Mockito.eq(false)))
           .thenReturn(stageDef);
    Mockito.when(library.getStage(Mockito.eq("l"), Mockito.eq("e"), Mockito.eq(false)))
           .thenReturn(errorStageDef);
    Mockito.when(library.getStage(Mockito.eq("l"), Mockito.eq("a"), Mockito.eq(false)))
      .thenReturn(aggStageDef);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());

    List<Config> pipelineConfigs = ImmutableList.of(
        new Config("executionMode", ExecutionMode.CLUSTER_BATCH.name()),
        new Config("memoryLimit", 1000)
    );

    StageConfiguration stageConf = new StageConfiguration("si", "l", "s", 1,
        ImmutableList.of(new Config("list", ImmutableList.of("S"))),
        Collections.<String, Object>emptyMap(), Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList());
    StageConfiguration errorStageConf = new StageConfiguration("ei", "l", "e", 1,
        ImmutableList.of(new Config("list", ImmutableList.of("E"))),
        Collections.<String, Object>emptyMap(), Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList());
    StageConfiguration aggStageConf = new StageConfiguration("ai", "l", "a", 1,
      ImmutableList.of(new Config("list", ImmutableList.of("A"))),
      Collections.<String, Object>emptyMap(), Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList());
    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        UUID.randomUUID(),
        "D",
        pipelineConfigs,
        Collections.EMPTY_MAP,
        ImmutableList.of(stageConf),
        errorStageConf,
        aggStageConf
    );

    List<Issue> issues = new ArrayList<>();
    PipelineBean bean = PipelineBeanCreator.get().create(false, library, pipelineConf, issues);

    Assert.assertNotNull(bean);

    // pipeline configs
    Assert.assertEquals(ExecutionMode.CLUSTER_BATCH, bean.getConfig().executionMode);

    // stages
    Assert.assertEquals(1, bean.getStages().size());
    MyTarget stage = (MyTarget) bean.getStages().get(0).getStage();
    Assert.assertEquals(ImmutableList.of("S"), stage.list);

    // Aggregating stage
    AggregatingMyTarget aggregatingStage = (AggregatingMyTarget) bean.getStatsAggregatorStage().getStage();
    Assert.assertEquals(ImmutableList.of("A"), aggregatingStage.list);

    // error stage
    ErrorMyTarget errorStage = (ErrorMyTarget) bean.getErrorStage().getStage();
    Assert.assertEquals(ImmutableList.of("E"), errorStage.list);

  }

  @Test
  public void testStageBeanReleaseClassLoader() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Mockito.when(libraryDef.getClassLoader()).thenReturn(cl);
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");

    StageConfiguration stageConf =
        new StageConfiguration("i", "l", "n", 1, Collections.<Config>emptyList(),
                               Collections.<String, Object>emptyMap(),
                               Collections.<String>emptyList(),
                               Collections.<String>emptyList(),
                               Collections.<String>emptyList());

    Map<String, Object> constants = ImmutableMap.<String, Object>of("a", 1);
    List<Issue> issues = new ArrayList<>();

    ClassLoaderReleaser releaser = Mockito.mock(ClassLoaderReleaser.class);
    StageBean bean = PipelineBeanCreator.get().createStage(stageDef, releaser, stageConf, constants, issues);

    Mockito.verify(releaser, Mockito.never()).releaseStageClassLoader(Mockito.<ClassLoader>any());
    bean.releaseClassLoader();
    Mockito.verify(releaser, Mockito.times(1)).releaseStageClassLoader(cl);

  }


  @Test
  public void testConfigsWithJavaDefaults() {
    StageLibraryDefinition libraryDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    StageDefinition stageDef = StageDefinitionExtractor.get().extract(libraryDef, MyTarget.class, "");
    StageDefinition errorStageDef = StageDefinitionExtractor.get().extract(libraryDef, ErrorMyTarget.class, "");
    StageDefinition aggregatingStageDef = StageDefinitionExtractor.get().extract(libraryDef, AggregatingMyTarget.class, "");
    StageLibraryTask library = Mockito.mock(StageLibraryTask.class);
    Mockito.when(library.getStage(Mockito.eq("l"), Mockito.eq("s"), Mockito.eq(false)))
           .thenReturn(stageDef);
    Mockito.when(library.getStage(Mockito.eq("l"), Mockito.eq("e"), Mockito.eq(false)))
           .thenReturn(errorStageDef);
    Mockito.when(library.getStage(Mockito.eq("l"), Mockito.eq("a"), Mockito.eq(false)))
      .thenReturn(aggregatingStageDef);
    Mockito.when(libraryDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());

    List<Config> pipelineConfigs = ImmutableList.of(
        new Config("executionMode", ExecutionMode.CLUSTER_BATCH.name()),
        new Config("memoryLimit", 1000)
    );

    StageConfiguration stageConf = new StageConfiguration("si", "l", "s", 1,
        ImmutableList.of(new Config("list", ImmutableList.of("S"))),
        Collections.<String, Object>emptyMap(), Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList());
    StageConfiguration errorStageConf = new StageConfiguration("ei", "l", "e", 1,
         ImmutableList.of(new Config("list", ImmutableList.of("E"))),
         Collections.<String, Object>emptyMap(), Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList());
    StageConfiguration aggStageConf = new StageConfiguration("ai", "l", "a", 1,
      ImmutableList.of(new Config("list", ImmutableList.of("A"))),
      Collections.<String, Object>emptyMap(), Collections.<String>emptyList(), Collections.<String>emptyList(), Collections.<String>emptyList());
    PipelineConfiguration pipelineConf = new PipelineConfiguration(
        1,
        PipelineConfigBean.VERSION,
        UUID.randomUUID(),
        "D",
        pipelineConfigs,
        Collections.EMPTY_MAP,
        ImmutableList.of(stageConf),
        errorStageConf,
        aggStageConf
    );

    List<Issue> issues = new ArrayList<>();
    PipelineBean bean = PipelineBeanCreator.get().create(false, library, pipelineConf, issues);

    MyTarget target = (MyTarget) bean.getStages().get(0).getStage();

    Assert.assertEquals("Hello", target.stringJavaDefault);
    Assert.assertEquals(E.B, target.enumSJavaDefault);
    Assert.assertEquals(ImmutableList.of(E.A), target.enumMJavaDefault);
    Assert.assertEquals(5, target.intJavaDefault);
    Assert.assertEquals(E.A, target.enumSNoDefaultAlAll);
  }
}
