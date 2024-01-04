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

import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ozone.annotations.RequestFeatureValidatorProcessor;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static com.google.testing.compile.Compiler.javac;
import static org.apache.ozone.annotations.RequestFeatureValidatorProcessor.ERROR_CONDITION_IS_EMPTY;
import static org.apache.ozone.annotations.RequestFeatureValidatorProcessor.ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST;
import static org.apache.ozone.annotations.RequestFeatureValidatorProcessor.ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT;
import static org.apache.ozone.annotations.RequestFeatureValidatorProcessor.ERROR_SECOND_PARAM_HAS_TO_BE_OMRESPONSE;
import static org.apache.ozone.annotations.RequestFeatureValidatorProcessor.ERROR_UNEXPECTED_PARAMETER_COUNT;
import static org.apache.ozone.annotations.RequestFeatureValidatorProcessor.ERROR_VALIDATOR_METHOD_HAS_TO_BE_STATIC;
import static org.apache.ozone.annotations.RequestFeatureValidatorProcessor.ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMREQUEST;
import static org.apache.ozone.annotations.RequestFeatureValidatorProcessor.ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMRESPONSE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Compile tests against the annotation processor for the
 * {@link RequestFeatureValidator} annotation.
 *
 * The processor should ensure the method signatures and return values, based
 * on annotation arguments provided.
 */
public class TestRequestFeatureValidatorProcessor {

  private static final String CLASSNAME = "Validation";

  @Test
  public void testAnnotationCanOnlyBeAppliedOnMethods() {
    Class<RequestFeatureValidator> c = RequestFeatureValidator.class;
    for (Annotation a : c.getAnnotations()) {
      if (a instanceof Target) {
        assertEquals(1, ((Target) a).value().length);
        assertSame(((Target) a).value()[0], ElementType.METHOD);
      }
    }
  }

  @Test
  public void testACorrectAnnotationSetupForPreProcessCompiles() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions("ServiceException"));

    assertThat(compile(source)).succeeded();
  }

  @Test
  public void testACorrectAnnotationSetupForPostProcessCompiles() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), postProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions("ServiceException"));

    assertThat(compile(source)).succeeded();
  }

  @Test
  public void testValidatorDoesNotNecessarilyThrowsExceptions() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source)).succeeded();
  }

  @Test
  public void testNonStaticValidatorDoesNotCompile() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(ERROR_VALIDATOR_METHOD_HAS_TO_BE_STATIC);
  }

  @Test
  public void testValidatorMethodCanBeFinal() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public", "static", "final"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source)).succeeded();
  }

  @Test
  public void testValidatorMethodCanBePrivate() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("private", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source)).succeeded();
  }

  @Test
  public void testValidatorMethodCanBeDefaultVisible() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source)).succeeded();
  }

  @Test
  public void testValidatorMethodCanBeProtected() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("protected", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source)).succeeded();
  }

  @Test
  public void testEmptyValidationConditionListDoesNotCompile() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(emptyConditions(), preProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source)).hadErrorContaining(ERROR_CONDITION_IS_EMPTY);
  }

  @Test
  public void testNotEnoughParametersForPreProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(
            String.format(ERROR_UNEXPECTED_PARAMETER_COUNT, 2, 1));
  }

  @Test
  public void testTooManyParametersForPreProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(
            String.format(ERROR_UNEXPECTED_PARAMETER_COUNT, 2, 3));
  }

  @Test
  public void testNotEnoughParametersForPostProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), postProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(
            String.format(ERROR_UNEXPECTED_PARAMETER_COUNT, 3, 2));
  }

  @Test
  public void testTooManyParametersForPostProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), postProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx",
            "String name"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(
            String.format(ERROR_UNEXPECTED_PARAMETER_COUNT, 3, 4));
  }

  @Test
  public void testWrongReturnValueForPreProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("String"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMREQUEST);
  }

  @Test
  public void testWrongReturnValueForPostProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), postProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("String"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMRESPONSE);
  }

  @Test
  public void testWrongFirstArgumentForPreProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("String rq", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST);
  }

  @Test
  public void testWrongFirstArgumentForPostProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), postProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("String rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST);
  }

  @Test
  public void testWrongSecondArgumentForPreProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), preProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "String ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT);
  }

  @Test
  public void testWrongSecondArgumentForPostProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), postProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "String rp", "ValidationContext ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(ERROR_SECOND_PARAM_HAS_TO_BE_OMRESPONSE);
  }

  @Test
  public void testWrongThirdArgumentForPostProcess() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), postProcess(), aReqType()),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "String ctx"),
        exceptions());

    assertThat(compile(source))
        .hadErrorContaining(ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT);
  }
  
  @Test
  public void testInvalidProcessingPhase() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(someConditions(), "INVALID", aReqType()),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions("ServiceException"));

    assertThat(compile(source)).failed();
  }

  @Test
  public void testMultipleErrorMessages() {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(emptyConditions(), postProcess(), aReqType()),
        modifiers(),
        returnValue("String"),
        parameters("String rq", "int rp", "String ctx"),
        exceptions());

    Compilation compilation = compile(source);
    assertThat(compilation).hadErrorContaining(ERROR_CONDITION_IS_EMPTY);
    assertThat(compilation)
        .hadErrorContaining(ERROR_VALIDATOR_METHOD_HAS_TO_BE_STATIC);
    assertThat(compilation)
        .hadErrorContaining(ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMRESPONSE);
    assertThat(compilation)
        .hadErrorContaining(ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST);
    assertThat(compilation)
        .hadErrorContaining(ERROR_SECOND_PARAM_HAS_TO_BE_OMRESPONSE);
    assertThat(compilation)
        .hadErrorContaining(ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT);
  }

  private Compilation compile(List<String> source) {
    Compilation c = javac()
        .withProcessors(new RequestFeatureValidatorProcessor())
        .compile(JavaFileObjects.forSourceLines(CLASSNAME, source));
    c.diagnostics().forEach(System.out::println);
    return c;
  }

  private ValidationCondition[] someConditions() {
    return
        new ValidationCondition[] {ValidationCondition.OLDER_CLIENT_REQUESTS};
  }

  private ValidationCondition[] emptyConditions() {
    return new ValidationCondition[] {};
  }

  private RequestProcessingPhase preProcess() {
    return RequestProcessingPhase.PRE_PROCESS;
  }

  private RequestProcessingPhase postProcess() {
    return RequestProcessingPhase.POST_PROCESS;
  }

  private Type aReqType() {
    return Type.CreateVolume;
  }

  private String returnValue(String retVal) {
    return retVal;
  }

  private String[] parameters(String... params) {
    return params;
  }

  private String[] modifiers(String... modifiers) {
    return modifiers;
  }

  private String[] exceptions(String... exceptions) {
    return exceptions;
  }

  private List<String> generateSourceOfValidatorMethodWith(
      String annotation,
      String[] modifiers,
      String returnType,
      String[] paramspecs,
      String[] exceptions) {
    List<String> lines = new ArrayList<>(allImports());
    lines.add("");
    lines.add("public class " + CLASSNAME + " {");
    lines.add("");
    lines.add("  " + annotation);
    StringBuilder signature =
        buildMethodSignature(modifiers, returnType, paramspecs, exceptions);
    lines.add(signature.toString());
    lines.add("    return null;");
    lines.add("  }");
    lines.add("}");
    lines.add("");
    lines.stream()
        .filter(s -> !s.startsWith("import"))
        .forEach(System.out::println);
    return lines;
  }

  private String annotationOf(
      ValidationCondition[] conditions,
      RequestProcessingPhase phase,
      Type reqType) {
    return annotationOf(conditions, phase.name(), reqType);
  }

  private String annotationOf(
      ValidationCondition[] conditions,
      String phase,
      Type reqType) {
    StringBuilder annotation = new StringBuilder();
    annotation.append("@RequestFeatureValidator(");
    StringBuilder conditionsArray = new StringBuilder();
    conditionsArray.append("conditions = { ");
    if (conditions.length > 0) {
      for (ValidationCondition condition : conditions) {
        conditionsArray.append(condition.name()).append(", ");
      }
      annotation
          .append(conditionsArray.substring(0, conditionsArray.length() - 2));
    } else {
      annotation.append(conditionsArray);
    }
    annotation.append(" }");
    annotation.append(", processingPhase = ").append(phase);
    annotation.append(", requestType = ").append(reqType.name());
    annotation.append(" )");
    return annotation.toString();
  }

  private List<String> allImports() {
    List<String> imports = new ArrayList<>();
    imports.add("import org.apache.hadoop.ozone.om.request.validation"
        + ".RequestFeatureValidator;");
    imports.add("import org.apache.hadoop.ozone.protocol.proto"
        + ".OzoneManagerProtocolProtos.OMRequest;");
    imports.add("import org.apache.hadoop.ozone.protocol.proto"
        + ".OzoneManagerProtocolProtos.OMResponse;");
    imports.add("import org.apache.hadoop.ozone.om.request.validation"
        + ".ValidationContext;");
    imports.add("import com.google.protobuf.ServiceException;");
    for (ValidationCondition condition : ValidationCondition.values()) {
      imports.add("import static org.apache.hadoop.ozone.om.request.validation"
          + ".ValidationCondition." + condition.name() + ";");
    }
    for (RequestProcessingPhase phase : RequestProcessingPhase.values()) {
      imports.add("import static org.apache.hadoop.ozone.om.request.validation"
          + ".RequestProcessingPhase." + phase.name() + ";");
    }
    for (Type reqType : Type.values()) {
      imports.add("import static org.apache.hadoop.ozone.protocol.proto"
          + ".OzoneManagerProtocolProtos.Type." + reqType.name() + ";");
    }
    return imports;
  }

  private StringBuilder buildMethodSignature(
      String[] modifiers, String returnType,
      String[] paramspecs, String[] exceptions) {
    StringBuilder signature = new StringBuilder();
    signature.append("  ");
    for (String modifier : modifiers) {
      signature.append(modifier).append(" ");
    }
    signature.append(returnType).append(" ");
    signature.append("validatorMethod(");
    signature.append(createParameterList(paramspecs));
    signature.append(") ");
    signature.append(createThrowsClause(exceptions));
    return signature.append(" {");
  }

  private String createParameterList(String[] paramSpecs) {
    if (paramSpecs == null || paramSpecs.length == 0) {
      return "";
    }
    StringBuilder parameters = new StringBuilder();
    for (String paramSpec : paramSpecs) {
      parameters.append(paramSpec).append(", ");
    }
    return parameters.substring(0, parameters.length() - 2);
  }

  private String createThrowsClause(String[] exceptions) {
    StringBuilder throwsClause = new StringBuilder();
    if (exceptions != null && exceptions.length > 0) {
      throwsClause.append(" throws ");
      StringBuilder exceptionList = new StringBuilder();
      for (String exception : exceptions) {
        exceptionList.append(exception).append(", ");
      }
      throwsClause
          .append(exceptionList.substring(0, exceptionList.length() - 2));
    }
    return throwsClause.toString();
  }
}
