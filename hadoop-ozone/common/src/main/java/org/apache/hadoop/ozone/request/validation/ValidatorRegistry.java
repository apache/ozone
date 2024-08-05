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
package org.apache.hadoop.ozone.request.validation;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.Version;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Registry that loads and stores the request validators to be applied by
 * a service.
 */
public class ValidatorRegistry<RequestType extends Enum<RequestType>> {

  private final Map<Class<? extends Version>, EnumMap<RequestType,
      EnumMap<RequestProcessingPhase, IndexedItems<Method, Integer>>>> indexedValidatorMap;

  /**
   * Creates a {@link ValidatorRegistry} instance that discovers validation
   * methods in the provided package and the packages in the same resource.
   * A validation method is recognized by all the annotations classes which
   * are annotated by {@link RegisterValidator} annotation that contains
   * important information about how and when to use the validator.
   * @param validatorPackage the main package inside which validatiors should
   *                         be discovered.
   */
  public ValidatorRegistry(Class<RequestType> requestType,
                    String validatorPackage,
                    Set<Class<? extends Version>> allowedVersionTypes,
                    Set<RequestProcessingPhase> allowedProcessingPhases) {
    this(requestType, ClasspathHelper.forPackage(validatorPackage), allowedVersionTypes, allowedProcessingPhases);
  }

  private Class<?> getReturnTypeOfAnnotationMethod(Class<? extends Annotation> clzz, String methodName) {
    try {
      return clzz.getMethod(methodName).getReturnType();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Method " + methodName + " not found in class:" + clzz.getCanonicalName());
    }
  }

  /**
   * Creates a {@link ValidatorRegistry} instance that discovers validation
   * methods under the provided URL.
   * A validation method is recognized by all annotations having the annotated by {@link RegisterValidator}
   * annotation that contains important information about how and when to use
   * the validator.
   * @param searchUrls the path in which the annotated methods are searched.
   */
  public ValidatorRegistry(Class<RequestType> requestType,
                    Collection<URL> searchUrls,
                    Set<Class<? extends Version>> allowedVersionTypes,
                    Set<RequestProcessingPhase> allowedProcessingPhases) {
    Set<Class<? extends Annotation>> validatorsToBeRegistered =
        new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(""))
            .setScanners(Scanners.TypesAnnotated)
            .setParallel(true)).getTypesAnnotatedWith(RegisterValidator.class).stream()
            .filter(annotationClass -> getReturnTypeOfAnnotationMethod((Class<? extends Annotation>) annotationClass,
                RegisterValidator.REQUEST_TYPE_METHOD_NAME)
                .equals(requestType))
            .filter(annotationClass -> allowedVersionTypes.contains(getReturnTypeOfAnnotationMethod(
                (Class<? extends Annotation>) annotationClass,
                    RegisterValidator.MAX_VERSION_METHOD_NAME)))
            .map(annotationClass -> (Class<? extends Annotation>) annotationClass)
            .collect(Collectors.toSet());
    this.indexedValidatorMap = allowedVersionTypes.stream().collect(ImmutableMap.toImmutableMap(Function.identity(),
        versionClass -> new EnumMap<>(requestType)));
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(searchUrls)
        .setScanners(Scanners.MethodsAnnotated)
        .setParallel(true)
    );
    initMaps(requestType, allowedProcessingPhases, validatorsToBeRegistered, reflections);
  }

  /**
   * Get the validators that has to be run in the given list of,
   * for the given requestType and for the given request versions.
   * {@link RequestProcessingPhase}.
   *
   * @param requestType the type of the protocol message
   * @param phase the request processing phase
   * @param requestVersions different versions extracted from the request.
   * @return the list of validation methods that has to run.
   */
  public List<Method> validationsFor(RequestType requestType,
                                     RequestProcessingPhase phase,
                                     List<? extends Version> requestVersions) {
    return requestVersions.stream()
        .flatMap(requestVersion -> this.validationsFor(requestType, phase, requestVersion).stream())
        .distinct().collect(Collectors.toList());
  }

  /**
   * Get the validators that has to be run in the given list of,
   * for the given requestType and for the given request versions.
   * {@link RequestProcessingPhase}.
   *
   * @param requestType the type of the protocol message
   * @param phase the request processing phase
   * @param requestVersion version extracted corresponding to the request.
   * @return the list of validation methods that has to run.
   */
  public <V extends Version> List<Method> validationsFor(RequestType requestType,
                                                         RequestProcessingPhase phase,
                                                         V requestVersion) {
    return Optional.ofNullable(this.indexedValidatorMap.get(requestVersion.getClass()))
        .map(requestTypeMap -> requestTypeMap.get(requestType)).map(phaseMap -> phaseMap.get(phase))
        .map(indexedMethods -> indexedMethods.getItemsGreaterThanIdx(requestVersion.getVersion()))
        .orElse(Collections.emptyList());

  }

  /**
   * Calls a specified method on the validator.
   * @Throws IllegalArgumentException when the specified method in the validator is invalid.
   */
  private <ReturnValue, Validator extends Annotation> ReturnValue callAnnotationMethod(
      Validator validator, String methodName, Class<ReturnValue> returnValueType) {
    try {
      return (ReturnValue) validator.getClass().getMethod(methodName).invoke(validator);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Method " + methodName + " not found in class:" +
          validator.getClass().getCanonicalName(), e);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new IllegalArgumentException("Error while invoking Method " + methodName + " from " +
          validator.getClass().getCanonicalName(), e);
    }
  }

  private <Validator extends Annotation> Version getMaxVersion(Validator validator) {
    return callAnnotationMethod(validator, RegisterValidator.MAX_VERSION_METHOD_NAME, Version.class);
  }

  private <Validator extends Annotation> RequestProcessingPhase getRequestPhase(Validator validator) {
    return callAnnotationMethod(validator, RegisterValidator.PROCESSING_PHASE_METHOD_NAME,
        RequestProcessingPhase.class);
  }

  private <Validator extends Annotation> RequestType getRequestType(Validator validator,
                                                                    Class<RequestType> requestType) {
    return callAnnotationMethod(validator, RegisterValidator.REQUEST_TYPE_METHOD_NAME, requestType);
  }


  private <V> void checkAllowedAnnotationValues(Set<V> values, V value, String valueName, String methodName) {
    if (!values.contains(value)) {
      throw new IllegalArgumentException(
          String.format("Invalid %1$s defined at annotation defined for method : %2$s, Annotation value : %3$s " +
                  "Allowed versionType: %4$s", valueName, methodName, value.toString(), values));
    }
  }

  /**
   * Initializes the internal request validator store.
   * The requests are stored in the following structure:
   * - An EnumMap with the RequestType as the key, and in which
   *   - values are an EnumMap with the request processing phase as the key, and in which
   *     - values is an {@link IndexedItems } containing the validation list
   * @param validatorsToBeRegistered collection of the annotated validtors to process.
   */
  private void initMaps(Class<RequestType> requestType,
                        Set<RequestProcessingPhase> allowedPhases,
                        Collection<Class<? extends Annotation>> validatorsToBeRegistered,
                        Reflections reflections) {
    for (Class<? extends Annotation> validator : validatorsToBeRegistered) {
      registerValidator(requestType, allowedPhases, validator, reflections);
    }
  }



  private void registerValidator(Class<RequestType> requestType,
                                 Set<RequestProcessingPhase> allowedPhases,
                                 Class<? extends Annotation> validatorToBeRegistered,
                                 Reflections reflections) {
    Collection<Method> methods =  reflections.getMethodsAnnotatedWith(validatorToBeRegistered);
    Class<? extends Version> versionClass = (Class<? extends Version>)
        this.getReturnTypeOfAnnotationMethod(validatorToBeRegistered, RegisterValidator.MAX_VERSION_METHOD_NAME);
    List<Pair<? extends Annotation, Method>> sortedMethodsByMaxVersion = methods.stream()
        .map(method -> Pair.of(method.getAnnotation(validatorToBeRegistered), method))
        .sorted((validatorMethodPair1, validatorMethodPair2) ->
            Integer.compare(
                this.getMaxVersion(validatorMethodPair1.getKey()).getVersion(),
                this.getMaxVersion(validatorMethodPair2.getKey()).getVersion()))
        .collect(Collectors.toList());
    for (Pair<? extends Annotation, Method> validatorMethodPair : sortedMethodsByMaxVersion) {
      Annotation validator = validatorMethodPair.getKey();
      Method method = validatorMethodPair.getValue();
      Version maxVersion = this.getMaxVersion(validator);
      RequestProcessingPhase phase = this.getRequestPhase(validator);
      checkAllowedAnnotationValues(allowedPhases, phase, RegisterValidator.PROCESSING_PHASE_METHOD_NAME,
          method.getName());
      RequestType type = this.getRequestType(validator, requestType);
      method.setAccessible(true);

      EnumMap<RequestType, EnumMap<RequestProcessingPhase, IndexedItems<Method, Integer>>> requestMap =
          this.indexedValidatorMap.get(versionClass);
      EnumMap<RequestProcessingPhase, IndexedItems<Method, Integer>> phaseMap =
          this.getAndInitialize(type, () -> new EnumMap<>(RequestProcessingPhase.class), requestMap);
      this.getAndInitialize(phase, IndexedItems::new, phaseMap).add(method, maxVersion.getVersion());
    }

  }

  private <K, V> V getAndInitialize(K key, Supplier<V> defaultSupplier, Map<K, V> from) {
    return from.computeIfAbsent(key, k -> defaultSupplier.get());
  }

  static final class IndexedItems<T, IDX extends Comparable<IDX>> {
    private final List<T> items;
    private final TreeMap<IDX, Integer> indexMap;

    private IndexedItems() {
      this.items = new ArrayList<>();
      this.indexMap = new TreeMap<>();
    }

    /**
     * Add an item to the collection and update index if required. The order of items added should have their index
     * sorted in increasing order.
     * @param item
     * @param idx
     */
    public void add(T item, IDX idx) {
      indexMap.putIfAbsent(idx, items.size());
      items.add(item);
    }

    /**
     *
     * @param indexValue Given index value.
     * @return All the items which has an index value greater than given index value.
     */
    public List<T> getItemsGreaterThanIdx(IDX indexValue) {
      return Optional.ofNullable(indexMap.higherEntry(indexValue))
          .map(Map.Entry::getValue)
          .map(startIndex -> items.subList(startIndex, items.size())).orElse(Collections.emptyList());
    }

  }

}
