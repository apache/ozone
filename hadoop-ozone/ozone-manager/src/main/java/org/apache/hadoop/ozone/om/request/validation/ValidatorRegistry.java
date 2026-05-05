/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.validation;

import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.PRE_PROCESS;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

/**
 * Registry that loads and stores the request validators to be applied by
 * a service.
 */
public class ValidatorRegistry {

  private final EnumMap<ValidationCondition,
      EnumMap<Type, EnumMap<RequestProcessingPhase, List<Method>>>>
      validators = new EnumMap<>(ValidationCondition.class);

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
        .setScanners(Scanners.MethodsAnnotated)
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
   * @return the list of validation methods that has to run.
   */
  List<Method> validationsFor(
      List<ValidationCondition> conditions,
      Type requestType,
      RequestProcessingPhase phase) {

    if (conditions.isEmpty() || validators.isEmpty()) {
      return Collections.emptyList();
    }

    Set<Method> returnValue = new HashSet<>();

    for (ValidationCondition condition: conditions) {
      returnValue.addAll(validationsFor(condition, requestType, phase));
    }
    return new ArrayList<>(returnValue);
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
    for (Method m : describedValidators) {
      RequestFeatureValidator descriptor =
          m.getAnnotation(RequestFeatureValidator.class);
      m.setAccessible(true);

      for (ValidationCondition condition : descriptor.conditions()) {
        EnumMap<Type, EnumMap<RequestProcessingPhase, List<Method>>>
            requestTypeMap = getAndInitialize(
                condition, this::newTypeMap, validators);
        EnumMap<RequestProcessingPhase, List<Method>> phases = getAndInitialize(
            descriptor.requestType(), this::newPhaseMap, requestTypeMap);
        if (isPreProcessValidator(descriptor)) {
          getAndInitialize(PRE_PROCESS, ArrayList::new, phases).add(m);
        } else if (isPostProcessValidator(descriptor)) {
          getAndInitialize(POST_PROCESS, ArrayList::new, phases).add(m);
        }
      }
    }
  }

  private EnumMap<Type,
      EnumMap<RequestProcessingPhase, List<Method>>> newTypeMap() {
    return new EnumMap<>(Type.class);
  }

  private EnumMap<RequestProcessingPhase, List<Method>> newPhaseMap() {
    return new EnumMap<>(RequestProcessingPhase.class);
  }

  private <K, V> V getAndInitialize(K key, Supplier<V> defaultSupplier, Map<K, V> from) {
    return from.computeIfAbsent(key, k -> defaultSupplier.get());
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
