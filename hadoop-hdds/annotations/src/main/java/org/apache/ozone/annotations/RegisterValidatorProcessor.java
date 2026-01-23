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

package org.apache.ozone.annotations;

import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

/**
 * This class is an annotation processor that is hooked into the java compiler
 * and is used to validate the Registered Validators annotations in the
 * codebase, to ensure that the annotated annotation classes have the proper required annotation methods and
 * appropriate return type for these methods.
 *
 * applyBefore : Returns an enum which implement
 * requestType: Returns an Enum value.
 * processingPhase: Returns an Enum of type RequestProcessingPhase.
 *
 * The module is compiled in a different execution via Maven before anything
 * else is compiled, and then javac picks this class up as an annotation
 * processor from the classpath via a ServiceLoader, based on the
 * META-INF/services/javax.annotation.processing.Processor file in the module's
 * resources folder.
 */
@SupportedAnnotationTypes("org.apache.hadoop.ozone.request.validation.RegisterValidator")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class RegisterValidatorProcessor extends AbstractProcessor {

  public static final String ANNOTATION_SIMPLE_NAME = "RegisterValidator";
  public static final String VERSION_CLASS_NAME = "org.apache.hadoop.ozone.Version";
  public static final String REQUEST_PROCESSING_PHASE_CLASS_NAME = "org.apache.hadoop.ozone.om.request.validation" +
      ".RequestProcessingPhase";
  public static final String APPLY_BEFORE_METHOD_NAME = "applyBefore";
  public static final String REQUEST_TYPE_METHOD_NAME = "requestType";
  public static final String PROCESSING_PHASE_METHOD_NAME = "processingPhase";

  public static final String MAX_VERSION_NOT_FOUND_ERROR_MESSAGE = "Method " + APPLY_BEFORE_METHOD_NAME +
      " returning an enum implementing " + VERSION_CLASS_NAME + " not found";
  public static final String REQUEST_TYPE_NOT_FOUND_ERROR_MESSAGE = "Method " + REQUEST_TYPE_METHOD_NAME +
      " returning an enum not found";
  public static final String PROCESSING_PHASE_NOT_FOUND_ERROR_MESSAGE = "Method " + PROCESSING_PHASE_METHOD_NAME
      + " returning an enum implementing " + REQUEST_PROCESSING_PHASE_CLASS_NAME + " not found";

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    for (TypeElement annotation : annotations) {
      if (!annotation.getSimpleName().contentEquals(ANNOTATION_SIMPLE_NAME)) {
        continue;
      }
      processElements(roundEnv.getElementsAnnotatedWith(annotation));
    }
    return false;
  }

  private boolean validateArrayMethod(ExecutableElement method, String expectedMethodName,
                                      ElementKind expectedReturnType,
                                      String expectedReturnClass) {
    Elements elementUtils = processingEnv.getElementUtils();
    Types types = processingEnv.getTypeUtils();
    TypeElement expectedReturnInterface = expectedReturnClass == null || expectedReturnClass.equals("") ? null :
        elementUtils.getTypeElement(expectedReturnClass);
    return method.getSimpleName().toString().equals(expectedMethodName) && (expectedReturnType == null ||
        TypeKind.ARRAY.equals(method.getReturnType().getKind()) &&
            types.asElement(((ArrayType)method.getReturnType()).getComponentType()).getKind() == expectedReturnType) &&
        (expectedReturnInterface == null ||
            types.isAssignable(types.asElement(method.getReturnType()).asType(), expectedReturnInterface.asType()));
  }

  private boolean validateMethod(ExecutableElement method, String expectedMethodName, ElementKind expectedReturnType,
                                 String expectedReturnClass) {
    Elements elementUtils = processingEnv.getElementUtils();
    Types types = processingEnv.getTypeUtils();
    TypeElement expectedReturnInterface = expectedReturnClass == null || expectedReturnClass.equals("") ? null :
        elementUtils.getTypeElement(expectedReturnClass);
    return method.getSimpleName().toString().equals(expectedMethodName) && (expectedReturnType == null ||
        types.asElement(method.getReturnType()) != null &&
            types.asElement(method.getReturnType()).getKind() == expectedReturnType) &&
        (expectedReturnInterface == null ||
            types.isAssignable(types.asElement(method.getReturnType()).asType(), expectedReturnInterface.asType()));
  }

  private void processElements(Set<? extends Element> annotatedElements) {
    for (Element element : annotatedElements) {
      if (element.getKind().equals(ElementKind.ANNOTATION_TYPE)) {
        boolean hasApplyBeforeMethod = false;
        boolean hasRequestType = false;
        boolean hasRequestProcessPhase =  false;
        for (Element enclosedElement : element.getEnclosedElements()) {
          // Check if the annotation has a method called "validatorName" returning a String
          if (enclosedElement instanceof ExecutableElement) {
            ExecutableElement method = (ExecutableElement) enclosedElement;
            hasApplyBeforeMethod = hasApplyBeforeMethod || validateMethod(method, APPLY_BEFORE_METHOD_NAME,
                ElementKind.ENUM, VERSION_CLASS_NAME);
            hasRequestType = hasRequestType || validateArrayMethod(method, REQUEST_TYPE_METHOD_NAME, ElementKind.ENUM,
                null);
            hasRequestProcessPhase = hasRequestProcessPhase || validateMethod(method, PROCESSING_PHASE_METHOD_NAME,
                ElementKind.ENUM, REQUEST_PROCESSING_PHASE_CLASS_NAME);
          }
        }
        if (!hasApplyBeforeMethod) {
          emitErrorMsg(MAX_VERSION_NOT_FOUND_ERROR_MESSAGE + " for " +
              element.getSimpleName().toString());
        }
        if (!hasRequestType) {
          emitErrorMsg(REQUEST_TYPE_NOT_FOUND_ERROR_MESSAGE + " for " +
              element.getSimpleName().toString());
        }
        if (!hasRequestProcessPhase) {
          emitErrorMsg(PROCESSING_PHASE_NOT_FOUND_ERROR_MESSAGE + " for " +
              element.getSimpleName().toString());
        }
      }
    }
  }

  private void emitErrorMsg(String s) {
    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, s);
  }
}
