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
package org.apache.ozone.annotations;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.AnnotationValueVisitor;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ExecutableType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleAnnotationValueVisitor8;
import javax.tools.Diagnostic;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This class is an annotation processor that is hooked into the java compiler
 * and is used to validate the RequestFeatureValidator annotations in the
 * codebase, to ensure that the annotated methods have the proper signature and
 * return type.
 *
 * The module is compiled in a different execution via Maven before anything
 * else is compiled, and then javac picks this class up as an annotation
 * processor from the classpath via a ServiceLoader, based on the
 * META-INF/services/javax.annotation.processing.Processor file in the module's
 * resources folder.
 */
@SupportedAnnotationTypes(
    "org.apache.hadoop.ozone.om.request.validation.RequestFeatureValidator")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class RequestFeatureValidatorProcessor extends AbstractProcessor {

  public static final String ERROR_CONDITION_IS_EMPTY =
      "RequestFeatureValidator has an empty condition list. Please define the"
          + " ValidationCondition in which the validator has to be applied.";
  public static final String ERROR_ANNOTATED_ELEMENT_IS_NOT_A_METHOD =
      "RequestFeatureValidator annotation is not applied to a method.";
  public static final String ERROR_VALIDATOR_METHOD_HAS_TO_BE_STATIC =
      "Only static methods can be annotated with the RequestFeatureValidator"
          + " annotation.";
  public static final String ERROR_UNEXPECTED_PARAMETER_COUNT =
      "Unexpected parameter count. Expected: %d; found: %d.";
  public static final String ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMREQUEST =
      "Pre-processing validator methods annotated with RequestFeatureValidator"
          + " annotation has to return an OMRequest object.";
  public static final String ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMRESPONSE =
      "Post-processing validator methods annotated with RequestFeatureValidator"
          + " annotation has to return an OMResponse object.";
  public static final String ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST =
      "First parameter of a RequestFeatureValidator method has to be an"
          + " OMRequest object.";
  public static final String ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT =
      "Last parameter of a RequestFeatureValidator method has to be"
          + " ValidationContext object.";
  public static final String ERROR_SECOND_PARAM_HAS_TO_BE_OMRESPONSE =
      "Second parameter of a RequestFeatureValidator method has to be an"
          + " OMResponse object.";

  public static final String OM_REQUEST_CLASS_NAME =
      "org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos"
          + ".OMRequest";
  public static final String OM_RESPONSE_CLASS_NAME =
      "org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos"
          + ".OMResponse";
  public static final String VALIDATION_CONTEXT_CLASS_NAME =
      "org.apache.hadoop.ozone.om.request.validation.ValidationContext";

  public static final String ANNOTATION_SIMPLE_NAME = "RequestFeatureValidator";
  public static final String ANNOTATION_CONDITIONS_PROPERTY_NAME = "conditions";
  public static final String ANNOTATION_PROCESSING_PHASE_PROPERTY_NAME =
      "processingPhase";

  public static final String PROCESSING_PHASE_PRE_PROCESS = "PRE_PROCESS";
  public static final String PROCESSING_PHASE_POST_PROCESS = "POST_PROCESS";
  public static final String ERROR_NO_PROCESSING_PHASE_DEFINED =
      "RequestFeatureValidator has an invalid ProcessingPhase defined.";

  @Override
  public boolean process(Set<? extends TypeElement> annotations,
      RoundEnvironment roundEnv) {
    for (TypeElement annotation : annotations) {
      if (!annotation.getSimpleName().contentEquals(ANNOTATION_SIMPLE_NAME)) {
        continue;
      }
      processElements(roundEnv.getElementsAnnotatedWith(annotation));
    }
    return false;
  }

  private void processElements(Set<? extends Element> annotatedElements) {
    for (Element elem : annotatedElements) {
      for (AnnotationMirror methodAnnotation : elem.getAnnotationMirrors()) {
        validateAnnotatedMethod(elem, methodAnnotation);
      }
    }
  }

  private void validateAnnotatedMethod(
      Element elem, AnnotationMirror methodAnnotation) {
    boolean isPreprocessor = checkAndEvaluateAnnotation(methodAnnotation);

    checkMethodIsAnnotated(elem);
    ensureAnnotatedMethodIsStatic(elem);
    ensurePreProcessorReturnsOMReqest((ExecutableElement) elem, isPreprocessor);
    ensurePostProcessorReturnsOMResponse(
        (ExecutableElement) elem, isPreprocessor);
    ensureMethodParameters(elem, isPreprocessor);
  }

  private void ensureMethodParameters(Element elem, boolean isPreprocessor) {
    List<? extends TypeMirror> paramTypes =
        ((ExecutableType) elem.asType()).getParameterTypes();
    ensureParameterCount(isPreprocessor, paramTypes);
    ensureParameterRequirements(paramTypes, 0, OM_REQUEST_CLASS_NAME,
        ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST);
    if (!isPreprocessor) {
      ensureParameterRequirements(paramTypes, 1, OM_RESPONSE_CLASS_NAME,
          ERROR_SECOND_PARAM_HAS_TO_BE_OMRESPONSE);
    }
    int contextOrder = isPreprocessor ? 1 : 2;
    ensureParameterRequirements(paramTypes, contextOrder,
        VALIDATION_CONTEXT_CLASS_NAME,
        ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT);
  }

  private void ensureParameterCount(boolean isPreprocessor,
      List<? extends TypeMirror> paramTypes) {
    int realParamCount = paramTypes.size();
    int expectedParamCount = isPreprocessor ? 2 : 3;
    if (realParamCount != expectedParamCount) {
      emitErrorMsg(String.format(ERROR_UNEXPECTED_PARAMETER_COUNT,
          expectedParamCount, realParamCount));
    }
  }

  private void ensureParameterRequirements(
      List<? extends TypeMirror> paramTypes,
      int index, String validationContextClassName,
      String errorLastParamHasToBeValidationContext) {
    if (paramTypes.size() >= index + 1 &&
        !paramTypes.get(index).toString().equals(validationContextClassName)) {
      emitErrorMsg(errorLastParamHasToBeValidationContext);
    }
  }

  private void ensurePostProcessorReturnsOMResponse(
      ExecutableElement elem, boolean isPreprocessor) {
    if (!isPreprocessor && !elem.getReturnType().toString()
        .equals(OM_RESPONSE_CLASS_NAME)) {
      emitErrorMsg(ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMRESPONSE);
    }
  }

  private void ensurePreProcessorReturnsOMReqest(
      ExecutableElement elem, boolean isPreprocessor) {
    if (isPreprocessor && !elem.getReturnType().toString()
        .equals(OM_REQUEST_CLASS_NAME)) {
      emitErrorMsg(ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMREQUEST);
    }
  }

  private void ensureAnnotatedMethodIsStatic(Element elem) {
    if (!elem.getModifiers().contains(Modifier.STATIC)) {
      emitErrorMsg(ERROR_VALIDATOR_METHOD_HAS_TO_BE_STATIC);
    }
  }

  private void checkMethodIsAnnotated(Element elem) {
    if (elem.getKind() != ElementKind.METHOD) {
      emitErrorMsg(ERROR_ANNOTATED_ELEMENT_IS_NOT_A_METHOD);
    }
  }

  private boolean checkAndEvaluateAnnotation(
      AnnotationMirror methodAnnotation) {
    boolean isPreprocessor = false;
    for (Entry<? extends ExecutableElement, ? extends AnnotationValue>
        entry : methodAnnotation.getElementValues().entrySet()) {
      
      if (hasInvalidValidationCondition(entry)) {
        emitErrorMsg(ERROR_CONDITION_IS_EMPTY);
      }
      if (isProcessingPhaseValue(entry)) {
        isPreprocessor = evaluateProcessingPhase(entry);
      }
    }
    return isPreprocessor;
  }

  private boolean evaluateProcessingPhase(
      Entry<? extends ExecutableElement, ? extends AnnotationValue> entry) {
    String procPhase = visit(entry, new ProcessingPhaseVisitor());
    if (procPhase.equals(PROCESSING_PHASE_PRE_PROCESS)) {
      return true;
    } else if (procPhase.equals(PROCESSING_PHASE_POST_PROCESS)) {
      return false;
    }
    return false;
  }

  private boolean isProcessingPhaseValue(
      Entry<? extends ExecutableElement, ? extends AnnotationValue> entry) {
    return isPropertyNamedAs(entry, ANNOTATION_PROCESSING_PHASE_PROPERTY_NAME);
  }

  private boolean hasInvalidValidationCondition(
      Entry<? extends ExecutableElement, ? extends AnnotationValue> entry) {
    return isPropertyNamedAs(entry, ANNOTATION_CONDITIONS_PROPERTY_NAME)
        && !visit(entry, new ConditionValidator());
  }

  private boolean isPropertyNamedAs(
      Entry<? extends ExecutableElement, ? extends AnnotationValue> entry,
      String simpleName) {
    return entry.getKey().getSimpleName().contentEquals(simpleName);
  }

  private <T> T visit(
      Entry<? extends ExecutableElement, ? extends AnnotationValue> entry,
      AnnotationValueVisitor<T, Void> visitor) {
    return entry.getValue().accept(visitor, null);
  }

  private static class ConditionValidator
      extends SimpleAnnotationValueVisitor8<Boolean, Void> {

    ConditionValidator() {
      super(Boolean.TRUE);
    }

    @Override
    public Boolean visitArray(List<? extends AnnotationValue> vals,
        Void unused) {
      if (vals.isEmpty()) {
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    }

  }

  private static class ProcessingPhaseVisitor
      extends SimpleAnnotationValueVisitor8<String, Void> {

    ProcessingPhaseVisitor() {
      super("UNKNOWN");
    }

    @Override
    public String visitEnumConstant(VariableElement c, Void unused) {
      if (c.getSimpleName().contentEquals(PROCESSING_PHASE_PRE_PROCESS)) {
        return PROCESSING_PHASE_PRE_PROCESS;
      }
      if (c.getSimpleName().contentEquals(PROCESSING_PHASE_POST_PROCESS)) {
        return PROCESSING_PHASE_POST_PROCESS;
      }
      throw new IllegalStateException(ERROR_NO_PROCESSING_PHASE_DEFINED);
    }
  }

  private void emitErrorMsg(String s) {
    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, s);
  }
}
