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

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.SimpleAnnotationValueVisitor8;
import javax.tools.Diagnostic;

/**
 * Validates that picocli options use the preferred Ozone CLI option style.
 */
@SupportedAnnotationTypes(CliOptionStyleProcessor.OPTION_ANNOTATION)
public class CliOptionStyleProcessor extends AbstractProcessor {

  static final String OPTION_ANNOTATION = "picocli.CommandLine.Option";
  private static final String NAMES_ATTRIBUTE = "names";
  private static final Pattern CAMEL_CASE = Pattern.compile("--.*[A-Z].*");
  private static final Pattern UNDER_SCORE = Pattern.compile("--.*_.*");

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latestSupported();
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations,
      RoundEnvironment roundEnv) {
    for (TypeElement annotation : annotations) {
      if (OPTION_ANNOTATION.contentEquals(annotation.getQualifiedName())) {
        roundEnv.getElementsAnnotatedWith(annotation)
            .forEach(this::checkOptionNames);
      }
    }
    return false;
  }

  private void checkOptionNames(Element element) {
    for (AnnotationMirror annotation : element.getAnnotationMirrors()) {
      if (isOptionAnnotation(annotation)) {
        checkOptionNames(element, annotation);
      }
    }
  }

  private boolean isOptionAnnotation(AnnotationMirror annotation) {
    return OPTION_ANNOTATION.contentEquals(
        annotation.getAnnotationType().asElement().toString());
  }

  private void checkOptionNames(Element element, AnnotationMirror annotation) {
    for (Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
        annotation.getElementValues().entrySet()) {
      if (entry.getKey().getSimpleName().contentEquals(NAMES_ATTRIBUTE)) {
        checkOptionNameValues(element, annotation, entry.getValue());
      }
    }
  }

  private void checkOptionNameValues(Element element, AnnotationMirror annotation,
      AnnotationValue value) {
    value.accept(new SimpleAnnotationValueVisitor8<Void, Void>() {
      @Override
      public Void visitArray(List<? extends AnnotationValue> values,
          Void unused) {
        values.forEach(v -> checkOptionNameValues(element, annotation, v));
        return null;
      }

      @Override
      public Void visitString(String option, Void unused) {
        checkOptionName(option, element, annotation, value);
        return null;
      }
    }, null);
  }

  private void checkOptionName(String option, Element element,
      AnnotationMirror annotation, AnnotationValue value) {
    if (hasDeprecatedStyle(option)) {
      processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
          String.format("CLI option '%s' uses a deprecated style. New options "
              + "should use --dash-separated-style long names or "
              + "single-character short names.", option),
          element, annotation, value);
    }
  }

  private static boolean hasDeprecatedStyle(String option) {
    if (option.startsWith("--")) {
      return CAMEL_CASE.matcher(option).matches()
          || UNDER_SCORE.matcher(option).matches();
    }
    return option.startsWith("-") && option.length() > 2;
  }

}
