/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
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
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic.Kind;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Annotation Processor that verifies if the methods that are marked with
 * Replicate annotation have proper method signature which throws
 * TimeoutException.
 */
@SupportedAnnotationTypes(ReplicateAnnotationProcessor.ANNOTATION_NAME)
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class ReplicateAnnotationProcessor extends AbstractProcessor {

  static final String ANNOTATION_NAME =
      "org.apache.hadoop.hdds.scm.metadata.Replicate";
  private static final String REQUIRED_EXCEPTION =
      "org.apache.hadoop.hdds.scm.exceptions.SCMException";

  @Override
  public boolean process(Set<? extends TypeElement> annotations,
                         RoundEnvironment roundEnv) {
    for (TypeElement annotation : annotations) {
      if (ANNOTATION_NAME.contentEquals(annotation.getQualifiedName())) {
        roundEnv.getElementsAnnotatedWith(annotation)
            .forEach(this::checkMethodSignature);
      }
    }
    return false;
  }

  /**
   * Checks if the method signature is correct.
   *
   * @param element the method element to be checked.
   */
  private void checkMethodSignature(Element element) {
    if (!(element instanceof ExecutableElement)) {
      processingEnv.getMessager().printMessage(Kind.ERROR,
          "Replicate annotation should be on method.");
      return;
    }
    final ExecutableElement executableElement = (ExecutableElement) element;
    final Set<String> exceptions = executableElement.getThrownTypes().stream()
        .map(TypeMirror::toString)
        .collect(toSet());

    if (!exceptions.contains(REQUIRED_EXCEPTION)) {

      processingEnv.getMessager().printMessage(Kind.ERROR,
          "Method with Replicate annotation should declare throwing " +
              REQUIRED_EXCEPTION, executableElement);
    }
  }
}
