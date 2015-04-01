/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ElConstantDefinition;
import com.streamsets.pipeline.el.ElFunctionDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PipelineDefinition {
  /*The config definitions of the pipeline*/
  private List<ConfigDefinition> configDefinitions;
  private ConfigGroupDefinition groupDefinition;

  public static PipelineDefinition getPipelineDef() {
    return new PipelineDefinition().localize();
  }

  public PipelineDefinition localize() {
    ClassLoader classLoader = getClass().getClassLoader();

    // stage configs
    List<ConfigDefinition> configDefs = new ArrayList<>();
    for (ConfigDefinition configDef : getConfigDefinitions()) {
      configDefs.add(configDef.localize(classLoader, PipelineDefConfigs.class.getName() + "-bundle"));
    }

    // stage groups
    ConfigGroupDefinition groupDefs = StageDefinition.localizeConfigGroupDefinition(classLoader,
                                                                                    getConfigGroupDefinition());
    return new PipelineDefinition(configDefs, groupDefs);
  }

  private PipelineDefinition(List<ConfigDefinition> configDefs, ConfigGroupDefinition groupDef) {
    configDefinitions = configDefs;
    groupDefinition = groupDef;
  }

  @VisibleForTesting
  PipelineDefinition() {
    this(ImmutableList.of(createDeliveryGuaranteeOption(), createBadRecordsHandlingConfigs()),
         createConfigGroupDefinition());
  }

  /*Need this API for Jackson to serialize*/
  public List<ConfigDefinition> getConfigDefinitions() {
    return configDefinitions;
  }

  public ConfigGroupDefinition getConfigGroupDefinition() {
    return groupDefinition;
  }

  @Override
  public String toString() {
    return Utils.format("PipelineDefinition[configDefinitions='{}']", configDefinitions);
  }

  /**************************************************************/
  /********************** Private methods ***********************/
  /**************************************************************/

  private static ConfigGroupDefinition createConfigGroupDefinition() {
    Map<String, List<String>> classNameToGroupsMap = new HashMap<>();
    List<String> groupsInEnum = new ArrayList<>();
    List<Map<String, String>> groups = new ArrayList<>();
    for (PipelineDefConfigs.Groups group : PipelineDefConfigs.Groups.values()) {
      groupsInEnum.add(group.name());
      groups.add(ImmutableMap.of("name", group.name(), "label", group.getLabel()));
    }
    classNameToGroupsMap.put(PipelineDefConfigs.Groups.class.getName(), groupsInEnum);
    return new ConfigGroupDefinition(classNameToGroupsMap, groups);
  }

  private static ConfigDefinition createDeliveryGuaranteeOption() {

    ChooserValues valueChooser = new DeliveryGuaranteeChooserValues();
    ModelDefinition model = new ModelDefinition(ModelType.VALUE_CHOOSER, valueChooser.getClass().getName(),
                                                valueChooser.getValues(), valueChooser.getLabels(), null);

    return new ConfigDefinition(
      PipelineDefConfigs.DELIVERY_GUARANTEE_CONFIG,
      ConfigDef.Type.MODEL,
      PipelineDefConfigs.DELIVERY_GUARANTEE_LABEL,
      PipelineDefConfigs.DELIVERY_GUARANTEE_DESCRIPTION,
      DeliveryGuarantee.AT_LEAST_ONCE.name(),
      true,
      "",
      PipelineDefConfigs.DELIVERY_GUARANTEE_CONFIG,
      model,
      "",
      new ArrayList<>(),
      0,
      Collections.<ElFunctionDefinition> emptyList(),
      Collections.<ElConstantDefinition> emptyList(),
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      "",
      0);
  }

  private static ConfigDefinition createBadRecordsHandlingConfigs() {
    ChooserValues valueChooser = new ErrorHandlingChooserValues();
    ModelDefinition model = new ModelDefinition(ModelType.VALUE_CHOOSER, valueChooser.getClass().getName(),
                                                valueChooser.getValues(), valueChooser.getLabels(), null);
    return new ConfigDefinition(
        PipelineDefConfigs.ERROR_RECORDS_CONFIG,
        ConfigDef.Type.MODEL,
        PipelineDefConfigs.ERROR_RECORDS_LABEL,
        PipelineDefConfigs.ERROR_RECORDS_DESCRIPTION,
        "",
        true,
        PipelineDefConfigs.Groups.BAD_RECORDS.name(),
        PipelineDefConfigs.ERROR_RECORDS_CONFIG,
        model,
        "",
        new ArrayList<>(),
        10,
        Collections.<ElFunctionDefinition> emptyList(),
        Collections.<ElConstantDefinition> emptyList(),
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        "",
        0);
  }

}