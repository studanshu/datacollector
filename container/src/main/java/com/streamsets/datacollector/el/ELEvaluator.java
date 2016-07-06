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

import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.CommonError;
import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ELEvaluator extends ELEval {
  private static final Logger LOG = LoggerFactory.getLogger(ELEvaluator.class);
  private final String configName;
  private final Map<String, Object> constants;
  private final Map<String, Map<String,Method>> functionsByNamespace;
  private final FunctionMapperImpl functionMapper;
  private final List<ElFunctionDefinition> elFunctionDefinitions;
  private final List<ElConstantDefinition> elConstantDefinitions;

  // ExpressionEvaluatorImpl can be used as a singleton
  private static final ExpressionEvaluatorImpl EVALUATOR = new ExpressionEvaluatorImpl();

  public ELEvaluator(String configName, Map<String, Object> constants, List<Class> elFuncConstDefClasses) {
    this(configName, constants, elFuncConstDefClasses.toArray(new Class[elFuncConstDefClasses.size()]));
  }

  public ELEvaluator(String configName, Map<String, Object> constants, Class<?>... elFuncConstDefClasses) {
    this.configName = configName;
    this.constants = new HashMap<>(constants);
    functionsByNamespace = new HashMap<>();
    elFunctionDefinitions = new ArrayList<>();
    elConstantDefinitions = new ArrayList<>();
    populateConstantsAndFunctions(elFuncConstDefClasses);
    this.functionMapper = new FunctionMapperImpl();
  }

  public ELEvaluator(String configName, Class<?>... elFuncConstDefClasses) {
    this(configName, new HashMap<String, Object>(), elFuncConstDefClasses);
  }

  private void populateConstantsAndFunctions(Class<?>... elFuncConstDefClasses) {
    if(elFuncConstDefClasses != null) {
      for (ElFunctionDefinition function : ELDefinitionExtractor.get().extractFunctions(elFuncConstDefClasses, "")) {
        elFunctionDefinitions.add(function);
        registerFunction(function);
      }
      for (ElConstantDefinition constant : ELDefinitionExtractor.get().extractConstants(elFuncConstDefClasses, "")) {
        elConstantDefinitions.add(constant);
        constants.put(constant.getName(), constant.getValue());
      }
    }
  }

  // stuff a function into the namespace/name dual level map structure
  private void registerFunction(ElFunctionDefinition function) {
    String namespace;
    String functionName;
    if (function.getName().contains(":")) {
      String[] tokens = function.getName().split(":");
      namespace = tokens[0];
      functionName = tokens[1];
    } else {
      namespace = "";
      functionName = function.getName();
    }

    Map<String, Method> namespaceFunctions = functionsByNamespace.get(namespace);
    if (namespaceFunctions == null) {
      namespaceFunctions = new HashMap<>();
      functionsByNamespace.put(namespace, namespaceFunctions);
    }
    namespaceFunctions.put(functionName, function.getMethod());
  }

  @Override
  public String getConfigName() {
    return configName;
  }

  @Override
  public ELVars createVariables() {
    return new ELVariables(constants);
  }

  public static void parseEL(String el) throws ELEvalException {
    try {
      EVALUATOR.parseExpressionString(el);
    } catch (ELException e) {
      LOG.debug("Error parsering EL '{}': {}", el, e.toString(), e);
      throw new ELEvalException(CommonError.CMN_0105, el, e.toString(), e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T evaluate (final ELVars vars, String expression, Class<T> returnType) throws ELEvalException {
    VariableResolver variableResolver = new VariableResolver() {

      @Override
      public Object resolveVariable(String name) throws ELException {
        Object value = constants.get(name);
        if (!vars.hasVariable(name)) {
          if (value == null && !constants.containsKey(name)) {
            throw new ELException(Utils.format("Constants/Variable '{}' cannot be resolved", name));
          }
        } else {
          value = vars.getVariable(name);
        }
        return value;
      }
    };
    try {
      return (T) EVALUATOR.evaluate(expression, returnType, variableResolver, functionMapper);
    } catch (ELException e) {
      // Apache evaluator is not using the getCause exception chaining that is available in Java but rather a custom
      // chaining mechanism. This doesn't work well for us as we're effectively swallowing the cause that is not
      // available in log, ...
      Throwable t = e;
      if(e.getRootCause() != null) {
        t = e.getRootCause();
        if(e.getCause() == null) {
          e.initCause(t);
        }
      }
      LOG.debug("Error valuating EL '{}': {}", expression, e.toString(), e);
      throw new ELEvalException(CommonError.CMN_0104, expression, t.toString(), e);
    }
  }

  private class FunctionMapperImpl implements FunctionMapper {

    @Override
    public Method resolveFunction(String functionNamespace, String functionName) {
      Map<String, Method> namespaceFunctions = functionsByNamespace.get(functionNamespace);
      if (namespaceFunctions == null) {
        return null;
      }
      return namespaceFunctions.get(functionName);
    }
  }

  public List<ElFunctionDefinition> getElFunctionDefinitions() {
    return elFunctionDefinitions;
  }

  public List<ElConstantDefinition> getElConstantDefinitions() {
    return elConstantDefinitions;
  }
}
