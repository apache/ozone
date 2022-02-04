package org.apache.hadoop.ozone.om.request.validation;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.jetbrains.annotations.NotNull;
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

public class ValidatorRegistry {

  EnumMap<ValidationCondition, EnumMap<Type, Pair<List<Method>, List<Method>>>>
      validators = new EnumMap<>(ValidationCondition.class);

  public static void main(String[] args) {
    new ValidatorRegistry("org.apache.hadoop.ozone");
  }

  public ValidatorRegistry(String validatorPackage) {
    initMaps(validatorPackage);
    System.out.println(validators.entrySet());
  }

  public List<Method> validationsFor(
      List<ValidationCondition> conditions,
      Type requestType,
      RequestProcessingPhase phase) {

    if (conditions.isEmpty() || validators.isEmpty()) {
      return Collections.emptyList();
    }

    List<Method> returnValue =
        validationsFor(conditions.get(0), requestType, phase);

    for (int i=1; i < conditions.size(); i++) {
      returnValue.addAll(validationsFor(conditions.get(i), requestType, phase));
    }
    return returnValue;
  }

  private List<Method> validationsFor(
      ValidationCondition condition,
      Type requestType,
      RequestProcessingPhase phase) {

    List<Method> returnValue = new LinkedList<>();

    EnumMap<Type, Pair<List<Method>, List<Method>>> requestTypeMap =
        validators.get(condition);
    if (requestTypeMap == null || requestTypeMap.isEmpty()) {
      returnValue = Collections.emptyList();
    }

    Pair<List<Method>, List<Method>> phases = requestTypeMap.get(requestType);
    if (phases == null ||
        (phases.getLeft().isEmpty() && phases.getRight().isEmpty())) {
      returnValue = Collections.emptyList();
    }

    if (phase.equals(RequestProcessingPhase.PRE_PROCESS)) {
      returnValue = phases.getLeft();
    } else if (phase.equals(RequestProcessingPhase.POST_PROCESS)) {
      returnValue = phases.getRight();
    }
    return returnValue;
  }

  private void initMaps(String validatorPackage) {
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
        List<Method> validationMethods = null;
        if (isPreProcessValidator(descriptor)) {
          validationMethods = phases.getLeft();
        } else if (isPostProcessValidator(descriptor)) {
          validationMethods = phases.getRight();
        }
        validationMethods.add(m);
      }
    }
  }

  @NotNull
  private EnumMap<Type, Pair<List<Method>, List<Method>>> newTypeMap() {
    return new EnumMap<>(Type.class);
  }

  <K, V> V getAndInitialize(K key, V defaultValue, Map<K,V> from) {
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

}
