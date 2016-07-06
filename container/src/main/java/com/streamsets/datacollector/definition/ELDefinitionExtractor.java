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
package com.streamsets.datacollector.definition;

import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionArgumentDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.el.JvmEL;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.el.VaultEL;
import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.Base64EL;
import com.streamsets.pipeline.lib.el.MathEL;
import com.streamsets.pipeline.lib.el.StringEL;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public abstract class ELDefinitionExtractor {
  static final Class[] DEFAULT_EL_DEFS = {
      Base64EL.class, JvmEL.class, MathEL.class, RuntimeEL.class, StringEL.class, VaultEL.class
  };

  private static final ELDefinitionExtractor EXTRACTOR = new ELDefinitionExtractor() {};

  public static ELDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public List<ErrorMessage> validateConstants(Class[] classes, Object contextMsg) {
    return validateConstants(ImmutableSet.<Class>builder().add(DEFAULT_EL_DEFS).add(classes).build(), contextMsg);
  }

  public List<ElConstantDefinition> extractConstants(Class[] classes, Object contextMsg) {
    return extractConstants(ImmutableSet.<Class>builder().add(DEFAULT_EL_DEFS).add(classes).build(), contextMsg);
  }

  public List<ErrorMessage> validateFunctions(Class[] classes, Object contextMsg) {
    return validateFunctions(ImmutableSet.<Class>builder().add(DEFAULT_EL_DEFS).add(classes).build(), contextMsg);
  }

  public List<ElFunctionDefinition> extractFunctions(Class[] classes, Object contextMsg) {
    return extractFunctions(ImmutableSet.<Class>builder().add(DEFAULT_EL_DEFS).add(classes).build(), contextMsg);
  }

  public Map<String, ElFunctionDefinition> getElFunctionsCatalog() {
    return elFunctionsIdx;
  }

  public Map<String, ElConstantDefinition> getELConstantsCatalog() {
    return elConstantsIdx;
  }

  private AtomicInteger indexCounter;
  private final Map<Method, ElFunctionDefinition> elFunctions;
  private final Map<Field, ElConstantDefinition> elConstants;
  private final Map<String, ElFunctionDefinition> elFunctionsIdx;
  private final Map<String, ElConstantDefinition> elConstantsIdx;

  private ELDefinitionExtractor() {
    indexCounter = new AtomicInteger();
    elFunctions = new ConcurrentHashMap<>();
    elConstants = new ConcurrentHashMap<>();
    elFunctionsIdx = new ConcurrentHashMap<>();
    elConstantsIdx = new ConcurrentHashMap<>();
  }

  private final static Pattern VALID_NAME = Pattern.compile("\\w*");

  private List<ErrorMessage> validateFunctions(Set<Class> augmentedClasses, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    for (Class<?> klass : augmentedClasses) {
      for (Method method : klass.getDeclaredMethods()) {
        if (method.getAnnotation(ElFunction.class) != null) {
          if (!Modifier.isPublic(method.getModifiers())) {
            errors.add(new ErrorMessage(DefinitionError.DEF_050, contextMsg, klass.getSimpleName(), method.getName()));
          }
        }
      }
      for (Method method : klass.getMethods()) {
        ElFunctionDefinition fDef = elFunctions.get(method);
        if (fDef == null) {
          ElFunction fAnnotation = method.getAnnotation(ElFunction.class);
          if (fAnnotation != null) {
            String fName = fAnnotation.name();
            if (fName.isEmpty()) {
              errors.add(new ErrorMessage(DefinitionError.DEF_051, contextMsg, klass.getSimpleName(), method.getName()));
            }
            if (!VALID_NAME.matcher(fName).matches()) {
              errors.add(new ErrorMessage(DefinitionError.DEF_053, contextMsg, klass.getSimpleName(), method.getName(),
                                          fName));
            }
            String fPrefix = fAnnotation.prefix();
            if (!VALID_NAME.matcher(fPrefix).matches()) {
              errors.add(new ErrorMessage(DefinitionError.DEF_054, contextMsg, klass.getSimpleName(), method.getName(),
                                          fPrefix));
            }
            if (!fPrefix.isEmpty()) {
              fName = fPrefix + ":" + fName;
            }
            if (!Modifier.isStatic(method.getModifiers())) {
              errors.add(new ErrorMessage(DefinitionError.DEF_052, contextMsg, klass.getSimpleName(), fName));
            }
            Annotation[][] pAnnotations = method.getParameterAnnotations();
            Class<?>[] pTypes = method.getParameterTypes();
            for (int i = 0; i < pTypes.length; i++) {
              if (getParamAnnotation(pAnnotations[i]) == null) {
                errors.add(new ErrorMessage(DefinitionError.DEF_055, contextMsg, klass.getSimpleName(), fName, i));
              }
            }
          }
        }
      }
    }
    return errors;
  }

  private ElParam getParamAnnotation(Annotation[] paramAnnotations) {
    for (Annotation annotation : paramAnnotations) {
      if (annotation instanceof ElParam) {
        return (ElParam) annotation;
      }
    }
    return null;
  }

  List<ElFunctionDefinition> extractFunctions(Set<Class> augmentedClasses, Object contextMsg) {
    List<ErrorMessage> errors = validateFunctions(augmentedClasses, contextMsg);
    if (errors.isEmpty()) {
      List<ElFunctionDefinition> fDefs = new ArrayList<>();
      for (Class<?> klass : augmentedClasses) {
        for (Method method : klass.getMethods()) {
          ElFunctionDefinition fDef = elFunctions.get(method);
          if (fDef == null) {
            ElFunction fAnnotation = method.getAnnotation(ElFunction.class);
            if (fAnnotation != null) {
              String fName = fAnnotation.name();
              if (!fAnnotation.prefix().isEmpty()) {
                fName = fAnnotation.prefix() + ":" + fName;
              }
              Annotation[][] pAnnotations = method.getParameterAnnotations();
              Class<?>[] pTypes = method.getParameterTypes();
              List<ElFunctionArgumentDefinition> fArgDefs = new ArrayList<>(pTypes.length);
              for (int i = 0; i < pTypes.length; i++) {
                fArgDefs.add(new ElFunctionArgumentDefinition(getParamAnnotation(pAnnotations[i]).value(),
                                                              pTypes[i].getSimpleName()));
              }
              fDef = new ElFunctionDefinition(Integer.toString(indexCounter.incrementAndGet()), fAnnotation.prefix(),
                                              fName, fAnnotation.description(), fArgDefs,
                                              method.getReturnType().getSimpleName(), method);
              elFunctionsIdx.put(fDef.getIndex(), fDef);
              elFunctions.put(method, fDef);
            }
          }
          if (fDef != null) {
            fDefs.add(fDef);
          }
        }
      }
      return fDefs;
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid EL functions definition: {}", errors));
    }
  }

  private List<ErrorMessage> validateConstants(Set<Class> augmentedClasses, Object contextMsg) {
    List<ErrorMessage> errors = new ArrayList<>();
    for (Class<?> klass : augmentedClasses) {
      for (Field field : klass.getDeclaredFields()) {
        if (field.getAnnotation(ElConstant.class) != null) {
          if (!Modifier.isPublic(field.getModifiers())) {
            errors.add(new ErrorMessage(DefinitionError.DEF_060, contextMsg, klass.getSimpleName(), field.getName()));
          }
        }
      }
      for (Field field : klass.getFields()) {
        ElConstantDefinition cDef = elConstants.get(field);
        if (cDef == null) {
          ElConstant cAnnotation = field.getAnnotation(ElConstant.class);
          if (cAnnotation != null) {
            String cName = cAnnotation.name();
            if (cName.isEmpty()) {
              errors.add(new ErrorMessage(DefinitionError.DEF_061, contextMsg, klass.getSimpleName(), field.getName()));
            }
            if (!VALID_NAME.matcher(cName).matches()) {
              errors.add(new ErrorMessage(DefinitionError.DEF_063, contextMsg, klass.getSimpleName(), field.getName(),
                                          cName));
            }
            if (!Modifier.isStatic(field.getModifiers())) {
              errors.add(new ErrorMessage(DefinitionError.DEF_062, contextMsg, klass.getSimpleName(), cName));
            }
          }
        }
      }
    }
    return errors;
  }

  List<ElConstantDefinition> extractConstants(Set<Class> augmentedClasses, Object contextMsg) {
    List<ErrorMessage> errors = validateConstants(augmentedClasses, contextMsg);
    if (errors.isEmpty()) {
      List<ElConstantDefinition> cDefs = new ArrayList<>();
      for (Class<?> klass : augmentedClasses) {
        for (Field field : klass.getFields()) {
          ElConstantDefinition cDef = elConstants.get(field);
          if (cDef == null) {
            ElConstant cAnnotation = field.getAnnotation(ElConstant.class);
            if (cAnnotation != null) {
              String cName = cAnnotation.name();
              Object value;
              try {
                value = field.get(null);
              } catch (IllegalAccessException ex) {
                throw new IllegalArgumentException(Utils.format("{}, could not retrieve constant '{}' value: {}",
                                                                contextMsg, cName, ex.toString(), ex));
              }
              cDef = new ElConstantDefinition(Integer.toString(indexCounter.incrementAndGet()), cName,
                                              cAnnotation.description(), field.getType().getSimpleName(), value);
              elConstantsIdx.put(cDef.getIndex(), cDef);
              elConstants.put(field, cDef);
            }
          }
          if (cDef != null) {
            cDefs.add(cDef);
          }
        }
      }
      return cDefs;
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid EL constants definition: {}", errors));
    }
  }

}
