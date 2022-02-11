/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.request.validation;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Registry that loads and stores the request validators to be applied by
 * a service.
 */
public class ValidatorRegistry {

  private
      EnumMap<
          ValidationCondition, EnumMap<Type, Pair<List<Method>, List<Method>>>>
      validators = new EnumMap<>(ValidationCondition.class);

  /**
   * Creates a {@link ValidatorRegistry} instance that discovers validation
   * methods in the provided package and its sub-packages.
   * A validation method is recognized by the {@link RequestFeatureValidator}
   * annotation that contains important information about how and when to use
   * the validator.
   * @param validatorPackage the main package inside which validatiors should
   *                         be discovered.
   */
  ValidatorRegistry(String validatorPackage) {
    initMaps(validatorPackage);
    System.out.println(validators.entrySet());
  }

  /**
   * Get the validators that has to be run in the given list of
   * {@link ValidationCondition}s, for the given requestType and
   * {@link RequestProcessingPhase}.
   *
   * @param conditions conditions that are present for the request
   * @param requestType the type of the protocol message
   * @param phase the request processing phase
   * @return the list of validation methods that has to run.
   */
  List<Method> validationsFor(
      List<ValidationCondition> conditions,
      Type requestType,
      RequestProcessingPhase phase) {

    if (conditions.isEmpty() || validators.isEmpty()) {
      return Collections.emptyList();
    }

    List<Method> returnValue =
        validationsFor(conditions.get(0), requestType, phase);

    for (int i = 1; i < conditions.size(); i++) {
      returnValue.addAll(validationsFor(conditions.get(i), requestType, phase));
    }
    return returnValue;
  }

  /**
   * Grabs validations for one particular condition.
   *
   * @param condition conditions that are present for the request
   * @param requestType the type of the protocol message
   * @param phase the request processing phase
   * @return the list of validation methods that has to run.
   */
  private List<Method> validationsFor(
      ValidationCondition condition,
      Type requestType,
      RequestProcessingPhase phase) {


    EnumMap<Type, Pair<List<Method>, List<Method>>> requestTypeMap =
        validators.get(condition);
    if (requestTypeMap == null || requestTypeMap.isEmpty()) {
      return Collections.emptyList();
    }

    Pair<List<Method>, List<Method>> phases = requestTypeMap.get(requestType);
    if (phases == null ||
        (phases.getLeft().isEmpty() && phases.getRight().isEmpty())) {
      return Collections.emptyList();
    }

    List<Method> returnValue = new LinkedList<>();
    if (phase.equals(RequestProcessingPhase.PRE_PROCESS)) {
      returnValue = phases.getLeft();
    } else if (phase.equals(RequestProcessingPhase.POST_PROCESS)) {
      returnValue = phases.getRight();
    }
    return returnValue;
  }

  /**
   * Initializes the internal request validator store.
   * The requests are stored in the following structure:
   * - An EnumMap with the {@link ValidationCondition} as the key, and in which
   *   - values are an EnumMap with the request type as the key, and in which
   *     - values are Pair of lists, in which
   *       - left side is the pre-processing validations list
   *       - right side is the post-processing validations list
   * @param validatorPackage the package in which the methods annotated with
   *                         {@link RequestFeatureValidator} are gathered.
   */
  void initMaps(String validatorPackage) {
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(ClasspathHelper.forPackage(validatorPackage))
        .setScanners(new MethodAnnotationsScanner())
        .useParallelExecutor()
    );

    Set<Method> describedValidators =
        reflections.getMethodsAnnotatedWith(RequestFeatureValidator.class);

    for (Method m : describedValidators) {
      RequestFeatureValidator descriptor =
          m.getAnnotation(RequestFeatureValidator.class);

      for (ValidationCondition condition : descriptor.conditions()) {
        EnumMap<Type, Pair<List<Method>, List<Method>>> requestTypeMap =
            getAndInitialize(condition, newTypeMap(), validators);
        Pair<List<Method>, List<Method>> phases = getAndInitialize(
            descriptor.requestType(), newListPair(), requestTypeMap);
        if (isPreProcessValidator(descriptor)) {
          phases.getLeft().add(m);
        } else if (isPostProcessValidator(descriptor)) {
          phases.getRight().add(m);
        }
      }
    }
  }

  private EnumMap<Type, Pair<List<Method>, List<Method>>> newTypeMap() {
    return new EnumMap<>(Type.class);
  }

  private <K, V> V getAndInitialize(K key, V defaultValue, Map<K, V> from) {
    if (defaultValue == null) {
      throw new NullPointerException(
          "Entry can not be initialized with null value.");
    }
    V inMapValue = from.get(key);
    if (inMapValue == null || !from.containsKey(key)) {
      from.put(key, defaultValue);
      return defaultValue;
    }
    return inMapValue;
  }

  private boolean isPreProcessValidator(RequestFeatureValidator descriptor) {
    return descriptor.processingPhase()
        .equals(RequestProcessingPhase.PRE_PROCESS);
  }

  private boolean isPostProcessValidator(RequestFeatureValidator descriptor) {
    return descriptor.processingPhase()
        .equals(RequestProcessingPhase.POST_PROCESS);
  }

  private Pair<List<Method>, List<Method>> newListPair() {
    return Pair.of(new ArrayList<>(), new ArrayList<>());
  }

  static ValidatorRegistry emptyRegistry() {
    return new ValidatorRegistry("") {
      @Override
      List<Method> validationsFor(List<ValidationCondition> conditions,
          Type requestType, RequestProcessingPhase phase) {
        return Collections.emptyList();
      }

      @Override
      void initMaps(String validatorPackage) { }
    };
  }

}
