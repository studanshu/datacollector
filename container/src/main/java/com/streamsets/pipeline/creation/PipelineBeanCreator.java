/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.creation;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.ConfigConfiguration;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.ModelType;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.PipelineGroups;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageLibraryDefinition;
import com.streamsets.pipeline.definition.ConfigDefinitionExtractor;
import com.streamsets.pipeline.definition.ConfigValueExtractor;
import com.streamsets.pipeline.definition.StageDefinitionExtractor;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.ElUtil;
import com.streamsets.pipeline.validation.Issue;
import com.streamsets.pipeline.validation.IssueCreator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class PipelineBeanCreator {

  private static final PipelineBeanCreator CREATOR = new PipelineBeanCreator() {
  };

  public static PipelineBeanCreator get() {
    return CREATOR;
  }

  private static final StageDefinition PIPELINE_DEFINITION = getPipelineDefinition();

  static private StageDefinition getPipelineDefinition() {
    StageLibraryDefinition libraryDef = new StageLibraryDefinition(Thread.currentThread().getContextClassLoader(),
                                                                   "system", "System", new Properties());
    return StageDefinitionExtractor.get().extract(libraryDef, PipelineConfigBean.class, "Pipeline Config Definitions");
  }

  static private Map<String, ConfigDefinition> getSystemStageConfigDefMap() {
    Map<String, ConfigDefinition> map = new HashMap<>();
    for (ConfigDefinition def
        : ConfigDefinitionExtractor.get().extract(StageConfigBean.class, "System Stage Config Definitions")) {
      map.put(def.getName(), def);
    }
    return ImmutableMap.copyOf(map);
  }

  public PipelineConfigBean create(PipelineConfiguration pipelineConf, List<Issue> errors) {
    int priorErrors = errors.size();
    PipelineConfigBean pipelineConfigBean = createPipelineConfigs(pipelineConf, errors);
    return (errors.size() == priorErrors) ? pipelineConfigBean : null;
  }

  public PipelineBean create(StageLibraryTask library, PipelineConfiguration pipelineConf, List<Issue> errors) {
    int priorErrors = errors.size();
    PipelineConfigBean pipelineConfigBean = create(pipelineConf, errors);
    StageBean errorStageBean = null;
    List<StageBean> stages = new ArrayList<>();
    if (pipelineConfigBean != null && pipelineConfigBean.constants != null) {
      for (StageConfiguration stageConf : pipelineConf.getStages()) {
        StageBean stageBean = createStageBean(library, stageConf, false, pipelineConfigBean.constants, errors);
        if (stageBean != null) {
          stages.add(stageBean);
        }
      }
      StageConfiguration errorStageConf = pipelineConf.getErrorStage();
      if (errorStageConf != null) {
        errorStageBean = createStageBean(library, errorStageConf, true, pipelineConfigBean.constants, errors);
      } else {
        errors.add(IssueCreator.getPipeline().create(PipelineGroups.BAD_RECORDS.name(), "badRecordsHandling",
                                                     CreationError.CREATION_009));
      }
    }
    return (errors.size() == priorErrors) ? new PipelineBean(pipelineConfigBean, stages, errorStageBean) : null;
  }

  StageBean createStageBean(StageLibraryTask library, StageConfiguration stageConf, boolean errorStage,
      Map<String, Object> constants, List<Issue> errors) {
    IssueCreator issueCreator = (errorStage) ? IssueCreator.getErrorStage(stageConf.getInstanceName())
                                             : IssueCreator.getStage(stageConf.getInstanceName());
    StageBean bean = null;
    StageDefinition stageDef = library.getStage(stageConf.getLibrary(), stageConf.getStageName(),
                                                stageConf.getStageVersion());
    if (stageDef != null) {
      if (stageDef.isErrorStage() != errorStage) {
        if (stageDef.isErrorStage()) {
          errors.add(issueCreator.create(CreationError.CREATION_007, stageConf.getLibrary(), stageConf.getStageName(),
                                         stageConf.getStageVersion()));
        } else {
          errors.add(issueCreator.create(CreationError.CREATION_008, stageConf.getLibrary(), stageConf.getStageName(),
                                         stageConf.getStageVersion()));
        }
      }
      bean = createStage(stageDef, stageConf, constants, errors);
    } else {
      errors.add(issueCreator.create(CreationError.CREATION_006, stageConf.getLibrary(), stageConf.getStageName(),
                                     stageConf.getStageVersion()));
    }
    return bean;
  }

  @SuppressWarnings("unchecked")
  StageConfiguration getPipelineConfAsStageConf(PipelineConfiguration pipelineConf) {
    return new StageConfiguration(null, "none", "pipeline", "1.0.0", pipelineConf.getConfiguration(),
                                  Collections.EMPTY_MAP, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
  }

  @SuppressWarnings("unchecked")
  PipelineConfigBean createPipelineConfigs(PipelineConfiguration pipelineConf, List<Issue> errors) {
    PipelineConfigBean pipelineConfigBean = new PipelineConfigBean();
    if (createConfigBeans(pipelineConfigBean, "", PIPELINE_DEFINITION, "pipeline", errors)) {
      injectConfigs(pipelineConfigBean, "", PIPELINE_DEFINITION.getConfigDefinitionsMap(), PIPELINE_DEFINITION,
                    getPipelineConfAsStageConf(pipelineConf), Collections.EMPTY_MAP, errors);
    }
    return pipelineConfigBean;
  }

  // if not null it is OK. if null there was at least one error, check errors for the details
  StageBean createStage(StageDefinition stageDef, StageConfiguration stageConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    Stage stage;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(stageDef.getStageClassLoader());
      stage = createStageInstance(stageDef, stageConf.getInstanceName(), errors);
      if (stage != null) {
        injectStageConfigs(stage, stageDef, stageConf, pipelineConstants, errors);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    StageConfigBean stageConfigBean = createAndInjectStageBeanConfigs(stageDef, stageConf, pipelineConstants, errors);
    return (errors.isEmpty()) ? new StageBean(stageDef, stageConf, stageConfigBean, stage) : null;
  }

  Stage createStageInstance(StageDefinition stageDef, String stageName, List<Issue> errors) {
    Stage stage = null;
    try {
      stage = (Stage) stageDef.getStageClass().newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                            : IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(CreationError.CREATION_000, stageDef.getLabel(), ex.getMessage()));
    }
    return stage;
  }

  Stage injectStageConfigs(Stage stage, StageDefinition stageDef, StageConfiguration stageConf,
      Map<String, Object> pipelineConstants, List<Issue> errors) {
    if (createConfigBeans(stage, "", stageDef, stageConf.getInstanceName(), errors)) {
      injectConfigs(stage, "", stageDef.getConfigDefinitionsMap(), stageDef, stageConf, pipelineConstants, errors);
    }
    return stage;
  }

  StageConfigBean createAndInjectStageBeanConfigs(StageDefinition stageDef, StageConfiguration stageConf,
      Map<String, Object> pipelineConstants, List<Issue> errors) {
    StageConfigBean stageConfigBean = new StageConfigBean();
    if (createConfigBeans(stageConfigBean, "", stageDef, stageConf.getInstanceName(), errors)) {
      //we use the stageDef configdefs because they may hide system configs
      injectConfigs(stageConfigBean, "", stageDef.getConfigDefinitionsMap(), stageDef, stageConf, pipelineConstants,
                    errors);
    }
    return stageConfigBean;
  }

  boolean createConfigBeans(Object obj, String configPrefix, StageDefinition stageDef, String stageName,
      List<Issue> errors) {
    boolean ok = true;
    Class klass = obj.getClass();
    for (Field field : klass.getFields()) {
      String configName = configPrefix + field.getName();
      if (field.getAnnotation(ConfigDefBean.class) != null) {
        try {
          Object bean = field.getType().newInstance();
          if (createConfigBeans(bean, configName + ".", stageDef, stageName, errors)) {
            field.set(obj, bean);
          }
        } catch (InstantiationException | IllegalAccessException ex) {
          ok = false;
          IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                                : IssueCreator.getStage(stageName);
          errors.add(issueCreator.create(CreationError.CREATION_001, field.getType().getSimpleName(), ex.getMessage()));
        }
      }
    }
    return ok;
  }

  void injectConfigs(Object obj, Map<String, Object> valueMap, String configPrefix,
      Map<String, ConfigDefinition> configDefMap, StageDefinition stageDef, StageConfiguration stageConf,
      Map<String, Object> pipelineConstants, List<Issue> errors) {
    String stageName = stageConf.getInstanceName();
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    for (Field field : obj.getClass().getFields()) {
      String configName = configPrefix + field.getName();
      if (field.getAnnotation(ConfigDef.class) != null) {
        ConfigDefinition configDef = configDefMap.get(configName);
        if (configDef == null) {
          errors.add(issueCreator.create(configDef.getGroup(), configName, CreationError.CREATION_002, configName));
        } else {
          Object value = valueMap.get(configName);
          if (value == null) {
            //TODO LOG WARNING missing config in state config
            injectDefaultValue(obj, field, stageDef, stageConf, configDef, pipelineConstants, stageName, errors);
          } else {
            injectConfigValue(obj, field, value, stageDef, stageConf, configDef, null, pipelineConstants, errors);
          }
        }
      } else if (field.getAnnotation(ConfigDefBean.class) != null) {
        try {
          injectConfigs(field.get(obj), valueMap, configName + ".", configDefMap, stageDef, stageConf,
                        pipelineConstants, errors);
        } catch (IllegalArgumentException | IllegalAccessException ex) {
          errors.add(issueCreator.create(CreationError.CREATION_003, ex.getMessage()));
        }
      }
    }

  }

  void injectConfigs(Object obj, String configPrefix, Map<String, ConfigDefinition> configDefMap,
      StageDefinition stageDef, StageConfiguration stageConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    String stageName = stageConf.getInstanceName();
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    for (Field field : obj.getClass().getFields()) {
      String configName = configPrefix + field.getName();
      if (field.getAnnotation(ConfigDef.class) != null) {
        ConfigDefinition configDef = configDefMap.get(configName);
        // if there is no config def, we ignore it, it can be the case when the config is a @HideConfig
        if (configDef != null) {
          ConfigConfiguration configConf = stageConf.getConfig(configName);
          if (configConf == null) {
            //TODO LOG WARNING missing config in state config
            injectDefaultValue(obj, field, stageDef, stageConf, configDef, pipelineConstants, stageName, errors);
          } else {
            injectConfigValue(obj, field, stageDef, stageConf, configDef, configConf, pipelineConstants, errors);
          }
        }
      } else if (field.getAnnotation(ConfigDefBean.class) != null) {
        try {
          injectConfigs(field.get(obj), configName + ".", configDefMap, stageDef, stageConf, pipelineConstants, errors);
        } catch (IllegalArgumentException | IllegalAccessException ex) {
          errors.add(issueCreator.create(CreationError.CREATION_003, ex.getMessage()));
        }
      }
    }
  }

  void injectDefaultValue(Object obj, Field field, StageDefinition stageDef, StageConfiguration stageConf,
      ConfigDefinition configDef, Map<String, Object> pipelineConstants, String stageName, List<Issue> errors) {
    Object value = configDef.getDefaultValue();
    if (value != null) {
      injectConfigValue(obj, field, value, stageDef, stageConf, configDef, null, pipelineConstants, errors);
    }
  }

  Object toEnum(Class klass, Object value, StageDefinition stageDef, String stageName, String groupName,
      String configName, List<Issue> errors) {
    try {
      value = Enum.valueOf(klass, value.toString());
    } catch (IllegalArgumentException ex) {
      IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                            : IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_010, value, klass.getSimpleName(),
                                     ex.getMessage()));
      value = null;
    }
    return value;
  }

  Object toString(Object value, StageDefinition stageDef, String stageName, String groupName, String configName,
      List<Issue> errors) {
    if (!(value instanceof String)) {
      IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                            : IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_011, value,
                                     value.getClass().getSimpleName()));
      value = null;
    }
    return value;
  }

  Object toChar(Object value, StageDefinition stageDef, String stageName, String groupName, String configName,
      List<Issue> errors) {
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    if (!(value instanceof String)) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_012, value));
      value = null;
    } else {
      String strValue = value.toString();
      if (strValue.isEmpty() || strValue.length() > 1) {
        errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_012, value));
        value = null;
      } else {
        value = strValue.charAt(0);
      }
    }
    return value;
  }

  Object toBoolean(Object value, StageDefinition stageDef, String stageName, String groupName, String configName,
      List<Issue> errors) {
    if (!(value instanceof Boolean)) {
      IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                            : IssueCreator.getStage(stageName);
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_013, value));
      value = null;
    }
    return value;
  }

  private static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPER_MAP
      = new ImmutableMap.Builder<Class<?>, Class<?>>()
      .put(byte.class, Byte.class)
      .put(short.class, Short.class)
      .put(int.class, Integer.class)
      .put(long.class, Long.class)
      .put(float.class, Float.class)
      .put(double.class, Double.class)
      .build();

  private static final Map<Class<?>, Method> WRAPPERS_VALUE_OF_MAP = new HashMap<>();

  @SuppressWarnings("unchecked")
  private static Method getValueOfMethod(Class klass) {
    try {
      return klass.getMethod("valueOf", String.class);
    } catch (Exception ex)  {
      throw new RuntimeException(ex);
    }
  }

  static {
    for (Class klass : PRIMITIVE_WRAPPER_MAP.values()) {
      WRAPPERS_VALUE_OF_MAP.put(klass, getValueOfMethod(klass));
    }
  }

  Object toNumber(Class numberType, Object value, StageDefinition stageDef, String stageName, String groupName,
      String configName, List<Issue> errors) {
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    if (!ConfigValueExtractor.NUMBER_TYPES.contains(value.getClass())) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_014, value));
      value = null;
    } else {
      try {
        if (PRIMITIVE_WRAPPER_MAP.containsKey(numberType)) {
          numberType = PRIMITIVE_WRAPPER_MAP.get(numberType);
        }
        value = WRAPPERS_VALUE_OF_MAP.get(numberType).invoke(null, value.toString());
      } catch (Exception ex) {
        errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_015, value,
                                       numberType.getSimpleName(), ex.getMessage()));
        value = null;
      }
    }
    return value;
  }

  Object toList(Object value, StageDefinition stageDef, ConfigDefinition configDef,
      Map<String, Object> pipelineConstants, String stageName, String groupName, String configName,
      List<Issue> errors) {
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    if (!(value instanceof List)) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_020));
      value = null;
    } else {
      boolean error = false;
      List<Object> list = new ArrayList<>();
      for (Object element : (List) value) {
        if (element == null) {
          errors.add(issueCreator.create(groupName, configName,  CreationError.CREATION_021));
          error = true;
        } else {
          element = resolveIfImplicitEL(element, stageDef, configDef, pipelineConstants, stageName, errors);
          if (element != null) {
            list.add(element);
          } else {
            error = true;
          }
        }
      }
      value = (error) ? null : list;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  Object toMap(Object value, StageDefinition stageDef, ConfigDefinition configDef,
      Map<String, Object> pipelineConstants, String stageName, String groupName, String configName,
      List<Issue> errors) {
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    if (!(value instanceof List)) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_030));
      value = null;
    } else {
      boolean error = false;
      Map map = new LinkedHashMap();
      for (Object entry : (List) value) {
        if (!(entry instanceof Map)) {
          error = true;
          errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_031,
                                         entry.getClass().getSimpleName()));
        } else {

          Object k = ((Map)entry).get("key");
          if (k == null) {
            errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_032));
          }

          Object v = ((Map)entry).get("value");
          if (v == null) {
            errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_033));
          } else {
            v = resolveIfImplicitEL(v, stageDef, configDef, pipelineConstants, stageName, errors);
          }

          if (k != null && v != null) {
            map.put(k, v);
          } else {
            error = true;
          }
        }
      }
      value = (error) ? null : map;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  Object toComplexField(Object value, StageDefinition stageDef, StageConfiguration stageConf,
      ConfigDefinition configDef, ConfigConfiguration configConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    String stageName = stageConf.getInstanceName();
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    if (!(value instanceof List)) {
      errors.add(issueCreator.create(configDef.getGroup(), configDef.getName(), CreationError.CREATION_040,
                           value.getClass().getSimpleName()));
      value = null;
    } else {
      boolean error = false;
      List<Object> list = new ArrayList<>();
      Class klass = configDef.getModel().getComplexFieldClass();
      List listValue = (List) value;
      for (int i = 0; i < listValue.size(); i++) {
        Map<String, Object> configElement;
        try {
          configElement = (Map<String, Object>) listValue.get(i);
          try {
            Object element = klass.newInstance();
            if (createConfigBeans(element, configDef.getName() + ".", stageDef, stageConf.getInstanceName(), errors)) {
              injectConfigs(element, configElement, "", configDef.getModel().getConfigDefinitionsAsMap(), stageDef,
                            stageConf, pipelineConstants, errors);
              list.add(element);
            }
          } catch (InstantiationException | IllegalAccessException ex) {
            errors.add(issueCreator.create(configDef.getGroup(), Utils.format("{}[{}]", configConf.getName(), i),
                                 CreationError.CREATION_041, klass.getSimpleName(), ex.getMessage()));
            error = true;
            break;
          }
        } catch (ClassCastException ex) {
          errors.add(issueCreator.create(configDef.getGroup(), Utils.format("{}[{}]", configConf.getName(), i),
                               CreationError.CREATION_042, ex.getMessage()));
        }
      }
      value = (error) ? null : list;
    }
    return value;
  }

  Object resolveIfImplicitEL(Object value, StageDefinition stageDef, ConfigDefinition configDef,
      Map<String, Object> pipelineConstants, String stageName, List<Issue> errors) {
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    if (configDef.getEvaluation() == ConfigDef.Evaluation.IMPLICIT && value instanceof String &&
        ElUtil.isElString(value)) {
      try {
        value = ElUtil.evaluate(value, stageDef, configDef, pipelineConstants);
      } catch (ELEvalException ex) {
        errors.add(issueCreator.create(configDef.getGroup(), configDef.getName(), CreationError.CREATION_005,
                                       value, ex.getMessage()));
        value = null;
      }
    }
    return value;
  }

  void injectConfigValue(Object obj, Field field, StageDefinition stageDef, StageConfiguration stageConf,
      ConfigDefinition configDef, ConfigConfiguration configConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    Object value = configConf.getValue();
    if (value == null) {
      value = configDef.getDefaultValue();
    }
    injectConfigValue(obj, field, value, stageDef, stageConf, configDef, configConf, pipelineConstants, errors);
  }


  void injectConfigValue(Object obj, Field field, Object value, StageDefinition stageDef, StageConfiguration stageConf,
      ConfigDefinition configDef, ConfigConfiguration configConf, Map<String, Object> pipelineConstants,
      List<Issue> errors) {
    String stageName = stageConf.getInstanceName();
    IssueCreator issueCreator = (stageDef.isErrorStage()) ? IssueCreator.getErrorStage(stageName)
                                                          : IssueCreator.getStage(stageName);
    String groupName = configDef.getGroup();
    String configName = configDef.getName();
    if (value == null) {
      errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_050));
    } else {
      if (configDef.getModel() != null && configDef.getModel().getModelType() == ModelType.COMPLEX_FIELD) {
        value = toComplexField(value, stageDef, stageConf, configDef, configConf, pipelineConstants, errors);
      } else if (List.class.isAssignableFrom(field.getType())) {
        value = toList(value, stageDef, configDef, pipelineConstants, stageName, groupName, configName, errors);
      } else if (Map.class.isAssignableFrom(field.getType())) {
        value = toMap(value, stageDef, configDef, pipelineConstants, stageName, groupName, configName, errors);
      } else {
        value = resolveIfImplicitEL(value, stageDef, configDef, pipelineConstants, stageName, errors);
        if (field.getType().isEnum()) {
          value = toEnum(field.getType(), value, stageDef, stageName, groupName, configName, errors);
        } else if (field.getType() == String.class) {
          value = toString(value, stageDef, stageName, groupName, configName, errors);
        } else if (List.class.isAssignableFrom(field.getType())) {
          value = toList(value, stageDef, configDef, pipelineConstants, stageName, groupName, configName, errors);
        } else if (Map.class.isAssignableFrom(field.getType())) {
          value = toMap(value, stageDef, configDef, pipelineConstants, stageName, groupName, configName, errors);
        } else if (ConfigValueExtractor.CHARACTER_TYPES.contains(field.getType())) {
          value = toChar(value, stageDef, stageName, groupName, configName, errors);
        } else if (ConfigValueExtractor.BOOLEAN_TYPES.contains(field.getType())) {
          value = toBoolean(value, stageDef, stageName, groupName, configName, errors);
        } else if (ConfigValueExtractor.NUMBER_TYPES.contains(field.getType())) {
          value = toNumber(field.getType(), value, stageDef, stageName, groupName, configName, errors);
        } else {
          errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_051,
                                         field.getType().getSimpleName()));
          value = null;
        }
      }
      if (value != null) {
        try {
          field.set(obj, value);
        } catch (IllegalAccessException ex) {
          errors.add(issueCreator.create(groupName, configName, CreationError.CREATION_060, value, ex.getMessage()));
        }
      }
    }
  }

}