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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.Versioned;
import org.apache.hadoop.ozone.request.validation.RegisterValidator;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

/**
 * Registry that loads and stores the request validators to be applied by
 * a service.
 */
public class ValidatorRegistry<RequestType extends Enum<RequestType>> {

  /**
   * A validator registered should have the following parameters:
   * applyBeforeVersion: Enum extending Version
   * RequestType: Enum signifying the type of request.
   * RequestProcessingPhase: Signifying if the validator is supposed to run pre or post submitting the request.
   * Based on the afforementioned parameters a complete map is built which stores the validators in a sorted order of
   * the applyBeforeVersion value of the validator method.
   * Thus when a request comes with a certain version value, all validators containing `applyBeforeVersion` parameter
   * greater than the request versions get triggered.
   * {@link #validationsFor(Enum, RequestProcessingPhase, Class, Versioned)}
   */
  private final Map<Class<? extends Annotation>, EnumMap<RequestType,
      EnumMap<RequestProcessingPhase, IndexedItems<Method, Versioned>>>> indexedValidatorMap;

  /**
   * Creates a {@link ValidatorRegistry} instance that discovers validation
   * methods in the provided package and the packages in the same resource.
   * A validation method is recognized by all the annotations classes which
   * are annotated by {@link RegisterValidator} annotation that contains
   * important information about how and when to use the validator.
   *
   * @param requestType             class of request type enum.
   * @param validatorPackage        the main package inside which validatiors should
   *                                be discovered.
   * @param allowedValidators       a set containing the various types of version allowed to be registered.
   * @param allowedProcessingPhases set of request processing phases which would be allowed to be registered to
   *                                registry.
   */
  public ValidatorRegistry(Class<RequestType> requestType, String validatorPackage,
      Set<Class<? extends Annotation>> allowedValidators, Set<RequestProcessingPhase> allowedProcessingPhases) {
    this(requestType, ClasspathHelper.forPackage(validatorPackage), allowedValidators, allowedProcessingPhases);
  }

  /**
   * Creates a {@link ValidatorRegistry} instance that discovers validation
   * methods under the provided URL.
   * A validation method is recognized by all annotations annotated by the {@link RegisterValidator}
   * annotation that contains important information about how and when to use
   * the validator.
   *
   * @param requestType             class of request type enum.
   * @param searchUrls              the path in which the annotated methods are searched.
   * @param allowedValidators       a set containing the various types of validator annotation allowed to be registered.
   * @param allowedProcessingPhases set of request processing phases which would be allowed to be registered to
   *                                the registry.
   */
  ValidatorRegistry(Class<RequestType> requestType, Collection<URL> searchUrls,
      Set<Class<? extends Annotation>> allowedValidators,
      Set<RequestProcessingPhase> allowedProcessingPhases) {
    Class<RequestType[]> requestArrayClass = (Class<RequestType[]>) Array.newInstance(requestType, 0)
        .getClass();
    Set<Class<? extends Annotation>> validatorsToBeRegistered =
        new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("org.apache.hadoop"))
            .setScanners(Scanners.TypesAnnotated)
            .setParallel(true)).getTypesAnnotatedWith(RegisterValidator.class).stream()
            .filter(allowedValidators::contains)
            .filter(annotationClass -> getReturnTypeOfAnnotationMethod((Class<? extends Annotation>) annotationClass,
                RegisterValidator.REQUEST_TYPE_METHOD_NAME)
                .equals(requestArrayClass))
            .map(annotationClass -> (Class<? extends Annotation>) annotationClass)
            .collect(Collectors.toSet());
    this.indexedValidatorMap = allowedValidators.stream().collect(ImmutableMap.toImmutableMap(Function.identity(),
        validatorClass -> new EnumMap<>(requestType)));
    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(searchUrls)
        .setScanners(Scanners.MethodsAnnotated)
        .setParallel(true)
    );
    initMaps(requestArrayClass, allowedProcessingPhases, validatorsToBeRegistered, reflections);
  }

  /**
   * Get the validators that has to be run in the given list of,
   * for the given requestType and for the given request versions.
   * {@link RequestProcessingPhase}.
   *
   * @param requestType     the type of the protocol message
   * @param phase           the request processing phase
   * @param requestVersions different versions extracted from the request.
   * @return the list of validation methods that has to run.
   */
  public List<Method> validationsFor(RequestType requestType, RequestProcessingPhase phase,
      Map<Class<? extends Annotation>, ? extends Versioned> requestVersions) {
    return requestVersions.entrySet().stream()
        .flatMap(requestVersion -> this.validationsFor(requestType, phase, requestVersion.getKey(),
            requestVersion.getValue()).stream())
        .distinct().collect(Collectors.toList());
  }

  /**
   * Get the validators that has to be run in the given list of,
   * for the given requestType and for the given request versions.
   * {@link RequestProcessingPhase}.
   *
   * @param requestType    the type of the protocol message
   * @param phase          the request processing phase
   * @param validatorClass annotation class of the validator
   * @param requestVersion version extracted corresponding to the request.
   * @return the list of validation methods that has to run.
   */
  public <V extends Versioned> List<Method> validationsFor(RequestType requestType,
      RequestProcessingPhase phase, Class<? extends Annotation> validatorClass, V requestVersion) {

    return Optional.ofNullable(this.indexedValidatorMap.get(validatorClass))
        .map(requestTypeMap -> requestTypeMap.get(requestType))
        .map(phaseMap -> phaseMap.get(phase))
        .map(indexedMethods -> requestVersion.version() < 0 ?
            indexedMethods.getItemsEqualToIdx(requestVersion) :
            indexedMethods.getItemsGreaterThanIdx(requestVersion))
        .orElse(Collections.emptyList());

  }

  /**
   * Calls a specified method on the validator.
   * @throws IllegalArgumentException when the specified method in the validator is invalid.
   */
  private static <ReturnValue, Validator extends Annotation> ReturnValue callAnnotationMethod(
      Validator validator, String methodName, Class<ReturnValue> returnValueType) {
    try {
      return returnValueType.cast(validator.getClass().getMethod(methodName).invoke(validator));
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Method " + methodName + " not found in class:" +
          validator.getClass().getCanonicalName(), e);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new IllegalArgumentException("Error while invoking Method " + methodName + " from " +
          validator.getClass().getCanonicalName(), e);
    }
  }

  private static Class<?> getReturnTypeOfAnnotationMethod(Class<? extends Annotation> clzz, String methodName) {
    try {
      return clzz.getMethod(methodName).getReturnType();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Method " + methodName + " not found in class:" + clzz.getCanonicalName());
    }
  }

  private static <Validator extends Annotation> Versioned getApplyBeforeVersion(Validator validator) {
    return callAnnotationMethod(validator, RegisterValidator.APPLY_BEFORE_METHOD_NAME, Versioned.class);
  }

  private static <Validator extends Annotation> RequestProcessingPhase getRequestPhase(Validator validator) {
    return callAnnotationMethod(validator, RegisterValidator.PROCESSING_PHASE_METHOD_NAME,
        RequestProcessingPhase.class);
  }

  private <Validator extends Annotation> RequestType[] getRequestType(Validator validator,
      Class<RequestType[]> requestType) {
    return callAnnotationMethod(validator, RegisterValidator.REQUEST_TYPE_METHOD_NAME, requestType);
  }

  private static <V> void checkAllowedAnnotationValues(Set<V> values, V value, String valueName, String methodName) {
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
   * - values are an EnumMap with the request processing phase as the key, and in which
   * - values is an {@link IndexedItems } containing the validation list
   *
   * @param validatorsToBeRegistered collection of the annotated validtors to process.
   */
  private void initMaps(Class<RequestType[]> requestType,
      Set<RequestProcessingPhase> allowedPhases,
      Collection<Class<? extends Annotation>> validatorsToBeRegistered,
      Reflections reflections) {
    for (Class<? extends Annotation> validator : validatorsToBeRegistered) {
      registerValidator(requestType, allowedPhases, validator, reflections);
    }
  }

  private void registerValidator(Class<RequestType[]> requestType,
      Set<RequestProcessingPhase> allowedPhases,
      Class<? extends Annotation> validatorToBeRegistered,
      Reflections reflections) {
    Collection<Method> methods = reflections.getMethodsAnnotatedWith(validatorToBeRegistered);
    List<Pair<? extends Annotation, Method>> sortedMethodsByApplyBeforeVersion = methods.stream()
        .map(method -> Pair.of(method.getAnnotation(validatorToBeRegistered), method))
        .sorted(Comparator.comparing(validatorMethodPair -> getApplyBeforeVersion(validatorMethodPair.getKey()),
            Versioned.versionComparator()))
        .collect(Collectors.toList());
    for (Pair<? extends Annotation, Method> validatorMethodPair : sortedMethodsByApplyBeforeVersion) {
      Annotation validator = validatorMethodPair.getKey();
      Method method = validatorMethodPair.getValue();
      Versioned applyBeforeVersion = this.getApplyBeforeVersion(validator);
      RequestProcessingPhase phase = this.getRequestPhase(validator);
      checkAllowedAnnotationValues(allowedPhases, phase, RegisterValidator.PROCESSING_PHASE_METHOD_NAME,
          method.getName());
      Set<RequestType> types = Sets.newHashSet(this.getRequestType(validator, requestType));
      method.setAccessible(true);
      for (RequestType type : types) {
        EnumMap<RequestType, EnumMap<RequestProcessingPhase, IndexedItems<Method, Versioned>>> requestMap =
            this.indexedValidatorMap.get(validatorToBeRegistered);
        EnumMap<RequestProcessingPhase, IndexedItems<Method, Versioned>> phaseMap =
            requestMap.computeIfAbsent(type, k -> new EnumMap<>(RequestProcessingPhase.class));
        phaseMap.computeIfAbsent(phase, k -> new IndexedItems<>(Versioned.versionComparator()))
            .add(method, applyBeforeVersion);
      }
    }
  }

  /**
   * Class responsible for maintaining indexs of items. Here each item should have an index corresponding to it.
   * The class implements functions for efficiently fetching range gets on the items added to the data structure.
   *
   * @param <T>   Refers to the Type of the item in the data structure
   * @param <IDX> Type of the index of an item added in the data structure. It is important that the index is
   *              comparable to each other.
   */
  private static final class IndexedItems<T, IDX> {
    private final List<T> items;
    private final NavigableMap<IDX, Integer> indexMap;

    private IndexedItems(Comparator<IDX> comparator) {
      this.items = new ArrayList<>();
      this.indexMap = new TreeMap<>(comparator);
    }

    /**
     * Add an item to the collection and update index if required. The order of items added should have their index
     * sorted in increasing order.
     *
     * @param item
     * @param idx
     */
    public void add(T item, IDX idx) {
      indexMap.putIfAbsent(idx, items.size());
      items.add(item);
    }

    /**
     * @param indexValue Given index value.
     * @return All the items which has an index value greater than given index value.
     */
    public List<T> getItemsGreaterThanIdx(IDX indexValue) {
      return Optional.ofNullable(indexMap.higherEntry(indexValue))
          .map(Map.Entry::getValue)
          .map(startIndex -> items.subList(startIndex, items.size())).orElse(Collections.emptyList());
    }

    /**
     * @param indexValue Given index value.
     * @return All the items that have an index value equal to the given index value.
     */
    public List<T> getItemsEqualToIdx(IDX indexValue) {
      return Optional.ofNullable(indexMap.get(indexValue))
          .map(startIndex -> items.subList(startIndex, Optional.ofNullable(indexMap.higherEntry(indexValue))
              .map(Map.Entry::getValue).orElse(items.size())))
          .orElse(Collections.emptyList());
    }
  }

}
