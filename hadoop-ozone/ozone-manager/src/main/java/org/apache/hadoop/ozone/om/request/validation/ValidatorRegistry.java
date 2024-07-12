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
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase.PRE_PROCESS;

/**
 * Registry that loads and stores the request validators to be applied by
 * a service.
 */
public class ValidatorRegistry {

  private final EnumMap<ValidationCondition,
      EnumMap<Type, EnumMap<RequestProcessingPhase, List<Method>>>>
      validators = new EnumMap<>(ValidationCondition.class);
  private final Map<Pair<Type, RequestProcessingPhase>, Pair<List<Method>, TreeMap<Integer, Integer>>>
      maxAllowedVersionValidatorMap = new HashMap<>(Type.values().length * RequestProcessingPhase.values().length,
      1.0f);

  /**
   * Creates a {@link ValidatorRegistry} instance that discovers validation
   * methods in the provided package and the packages in the same resource.
   * A validation method is recognized by the {@link RequestFeatureValidator}
   * annotation that contains important information about how and when to use
   * the validator.
   * @param validatorPackage the main package inside which validatiors should
   *                         be discovered.
   */
  ValidatorRegistry(String validatorPackage) {
    this(ClasspathHelper.forPackage(validatorPackage));
  }

  /**
   * Creates a {@link ValidatorRegistry} instance that discovers validation
   * methods under the provided URL.
   * A validation method is recognized by the {@link RequestFeatureValidator}
   * annotation that contains important information about how and when to use
   * the validator.
   * @param searchUrls the path in which the annotated methods are searched.
   */
  ValidatorRegistry(Collection<URL> searchUrls) {
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(searchUrls)
        .setScanners(new MethodAnnotationsScanner())
        .setParallel(true)
    );

    Set<Method> describedValidators =
        reflections.getMethodsAnnotatedWith(RequestFeatureValidator.class);
    initMaps(describedValidators);
  }

  /**
   * Get the validators that has to be run in the given list of
   * {@link ValidationCondition}s, for the given requestType and
   * {@link RequestProcessingPhase}.
   *
   * @param conditions conditions that are present for the request
   * @param requestType the type of the protocol message
   * @param phase the request processing phase
   * @param requestClientVersion the client version of the protocol message.
   * @return the list of validation methods that has to run.
   */
  List<Method> validationsFor(List<ValidationCondition> conditions,
                              Type requestType,
                              RequestProcessingPhase phase,
                              int requestClientVersion) {
    Set<Method> methodsToRun = new HashSet<>(validationsFor(conditions, requestType, phase));
    methodsToRun.addAll(validationsFor(requestType, phase, requestClientVersion));
    return new ArrayList<>(methodsToRun);
  }

  /**
   * Get the validators that has to run
   * that require {@Link ClientVersion}. minimum client version newer than the request client version, for the given
   * requestType and {@link RequestProcessingPhase}.
   *
   * @param requestType the type of the protocol message
   * @param phase the request processing phase
   * @param requestClientVersion the client version of the protocol message.
   * @return the list of validation methods that has to run.
   */
  private List<Method> validationsFor(Type requestType,
                                      RequestProcessingPhase phase,
                                      int requestClientVersion) {
    Pair<Type, RequestProcessingPhase> key = Pair.of(requestType, phase);
    if (this.maxAllowedVersionValidatorMap.containsKey(key)) {
      Pair<List<Method>, TreeMap<Integer, Integer>> value = this.maxAllowedVersionValidatorMap.get(key);
      return Optional.ofNullable(value.getRight().ceilingEntry(requestClientVersion))
          .map(Map.Entry::getValue)
          .map(startIndex -> value.getKey().subList(startIndex, value.getKey().size()))
          .orElse(Collections.emptyList());
    }
    return Collections.emptyList();
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
  private Set<Method> validationsFor(
      List<ValidationCondition> conditions,
      Type requestType,
      RequestProcessingPhase phase) {

    if (conditions.isEmpty() || validators.isEmpty()) {
      return Collections.emptySet();
    }

    Set<Method> returnValue = new HashSet<>();

    for (ValidationCondition condition: conditions) {
      returnValue.addAll(validationsFor(condition, requestType, phase));
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

    EnumMap<Type, EnumMap<RequestProcessingPhase, List<Method>>>
        requestTypeMap = validators.get(condition);
    if (requestTypeMap == null || requestTypeMap.isEmpty()) {
      return Collections.emptyList();
    }

    EnumMap<RequestProcessingPhase, List<Method>> phases =
        requestTypeMap.get(requestType);
    if (phases == null) {
      return Collections.emptyList();
    }

    List<Method> validatorsForPhase = phases.get(phase);
    if (validatorsForPhase == null) {
      return Collections.emptyList();
    }
    return validatorsForPhase;
  }

  private int getMaxAllowedClientVersion(Method method) {
    RequestFeatureValidator descriptor = method.getAnnotation(RequestFeatureValidator.class);
    ClientVersion maxAllowedClientVersion = descriptor.maxClientVersion();
    return maxAllowedClientVersion == null ? (ClientVersion.DEFAULT_VERSION.toProtoValue() - 1) :
        maxAllowedClientVersion.toProtoValue();
  }

  /**
   * Initializes the internal request validator store.
   * The requests are stored in the following structure:
   * - An EnumMap with the {@link ValidationCondition} as the key, and in which
   *   - values are an EnumMap with the request type as the key, and in which
   *     - values are Pair of lists, in which
   *       - left side is the pre-processing validations list
   *       - right side is the post-processing validations list
   * @param describedValidators collection of the annotated methods to process.
   */
  void initMaps(Collection<Method> describedValidators) {
    List<Method> sortedMethodsByMaxVersion = describedValidators.stream()
        .sorted((method1, method2) -> Integer.compare(getMaxAllowedClientVersion(method1),
            getMaxAllowedClientVersion(method2)))
        .collect(Collectors.toList());
    for (Method m : sortedMethodsByMaxVersion) {
      RequestFeatureValidator descriptor = m.getAnnotation(RequestFeatureValidator.class);
      m.setAccessible(true);
      Type requestType = descriptor.requestType();

      for (ValidationCondition condition : descriptor.conditions()) {
        EnumMap<Type, EnumMap<RequestProcessingPhase, List<Method>>>
            requestTypeMap = getAndInitialize(
            condition, this::newTypeMap, validators);
        EnumMap<RequestProcessingPhase, List<Method>> phases = getAndInitialize(
            requestType, this::newPhaseMap, requestTypeMap);
        if (isPreProcessValidator(descriptor)) {
          getAndInitialize(PRE_PROCESS, ArrayList::new, phases).add(m);
        } else if (isPostProcessValidator(descriptor)) {
          getAndInitialize(POST_PROCESS, ArrayList::new, phases).add(m);
        }
      }
      Pair<Type, RequestProcessingPhase> indexKey = Pair.of(descriptor.requestType(), descriptor.processingPhase());
      int maxAllowedClientVersion = this.getMaxAllowedClientVersion(m);
      Pair<List<Method>, TreeMap<Integer, Integer>> value =
          getAndInitialize(indexKey, () -> Pair.of(new ArrayList<>(), new TreeMap<>()), maxAllowedVersionValidatorMap);
      List<Method> methods = value.getKey();
      value.getRight().putIfAbsent(maxAllowedClientVersion, methods.size());
      methods.add(m);
    }
  }

  private EnumMap<Type,
      EnumMap<RequestProcessingPhase, List<Method>>> newTypeMap() {
    return new EnumMap<>(Type.class);
  }

  private EnumMap<RequestProcessingPhase, List<Method>> newPhaseMap() {
    return new EnumMap<>(RequestProcessingPhase.class);
  }

  private <K, V> V getAndInitialize(K key, Supplier<V> defaultValue, Map<K, V> from) {
    return from.compute(key, (k, v) -> v == null ? defaultValue.get() : v);
  }

  private boolean isPreProcessValidator(RequestFeatureValidator descriptor) {
    return descriptor.processingPhase()
        .equals(PRE_PROCESS);
  }

  private boolean isPostProcessValidator(RequestFeatureValidator descriptor) {
    return descriptor.processingPhase()
        .equals(POST_PROCESS);
  }

}
