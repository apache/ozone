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

import static com.google.testing.compile.CompilationSubject.assertThat;
import static com.google.testing.compile.Compiler.javac;
import static org.apache.ozone.annotations.OmRequestFeatureValidatorProcessor.ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST;
import static org.apache.ozone.annotations.OmRequestFeatureValidatorProcessor.ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT;
import static org.apache.ozone.annotations.OmRequestFeatureValidatorProcessor.ERROR_SECOND_PARAM_HAS_TO_BE_OMRESPONSE;
import static org.apache.ozone.annotations.OmRequestFeatureValidatorProcessor.ERROR_UNEXPECTED_PARAMETER_COUNT;
import static org.apache.ozone.annotations.OmRequestFeatureValidatorProcessor.ERROR_VALIDATOR_METHOD_HAS_TO_BE_STATIC;
import static org.apache.ozone.annotations.OmRequestFeatureValidatorProcessor.ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMREQUEST;
import static org.apache.ozone.annotations.OmRequestFeatureValidatorProcessor.ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMRESPONSE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.Versioned;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RegisterValidator;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;
import org.apache.ozone.annotations.OmRequestFeatureValidatorProcessor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

/**
 * Compile tests against the annotation processor for all validators annotated with
 * {@link RegisterValidator} annotation.
 *
 * The processor should ensure the method signatures and return values, based
 * on annotation arguments provided.
 */
public class TestOMValidatorProcessor {

  private static final String CLASSNAME = "Validation";
  private static final Map<Class<?>, Class<?>> ANNOTATION_VERSION_CLASS_MAP = new Reflections(
      new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("")).setScanners(Scanners.TypesAnnotated)
          .setParallel(true)).getTypesAnnotatedWith(RegisterValidator.class).stream().filter(annotationClass -> {
            try {
              return annotationClass.getMethod(RegisterValidator.REQUEST_TYPE_METHOD_NAME).getReturnType()
                  .equals(Type[].class);
            } catch (NoSuchMethodException e) {
              throw new RuntimeException(e);
            }
          })
          .collect(Collectors.toMap(Function.identity(), annotationClass -> {
            try {
              return annotationClass.getMethod(RegisterValidator.APPLY_BEFORE_METHOD_NAME).getReturnType();
            } catch (NoSuchMethodException e) {
              throw new RuntimeException(e);
            }
          }));

  private static Stream<Arguments> annotatedClasses() {
    return ANNOTATION_VERSION_CLASS_MAP.entrySet().stream().flatMap(e ->
        Arrays.stream(e.getValue().getEnumConstants()).map(version -> Arguments.of(e.getKey(), version)));
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testAnnotationCanOnlyBeAppliedOnMethods(Class<?> annotationClass,
                                                                                    V version) {
    for (Annotation a : annotationClass.getAnnotations()) {
      if (a instanceof Target) {
        assertEquals(1, ((Target) a).value().length);
        assertSame(((Target) a).value()[0], ElementType.METHOD);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testACorrectAnnotationSetupForPreProcessCompiles(Class<?> annotationClass,
                                                                                             V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions("ServiceException"), annotationClass);

    assertThat(compile(source)).succeeded();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testACorrectAnnotationSetupForPostProcessCompiles(
      Class<?> annotationClass, V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(postProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions("ServiceException"), annotationClass);

    assertThat(compile(source)).succeeded();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testValidatorDoesNotNecessarilyThrowsExceptions(Class<?> annotationClass,
                                                                                            V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source)).succeeded();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testNonStaticValidatorDoesNotCompile(Class<?> annotationClass,
                                                                                 V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(ERROR_VALIDATOR_METHOD_HAS_TO_BE_STATIC);
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testValidatorMethodCanBeFinal(Class<?> annotationClass,
                                                                          V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static", "final"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source)).succeeded();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testValidatorMethodCanBePrivate(Class<?> annotationClass,
                                                                            V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("private", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source)).succeeded();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testValidatorMethodCanBeDefaultVisible(Class<?> annotationClass,
                                                                                   V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source)).succeeded();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testValidatorMethodCanBeProtected(Class<?> annotationClass,
                                                                              V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("protected", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source)).succeeded();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testNotEnoughParametersForPreProcess(Class<?> annotationClass,
                                                                                 V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(
            String.format(ERROR_UNEXPECTED_PARAMETER_COUNT, 2, 1));
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testTooManyParametersForPreProcess(Class<?> annotationClass,
                                                                               V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(
            String.format(ERROR_UNEXPECTED_PARAMETER_COUNT, 2, 3));
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testNotEnoughParametersForPostProcess(Class<?> annotationClass,
                                                                                  V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(postProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(
            String.format(ERROR_UNEXPECTED_PARAMETER_COUNT, 3, 2));
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testTooManyParametersForPostProcess(Class<?> annotationClass,
                                                                                V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(postProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx",
            "String name"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(
            String.format(ERROR_UNEXPECTED_PARAMETER_COUNT, 3, 4));
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testWrongReturnValueForPreProcess(Class<?> annotationClass,
                                                                              V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("String"),
        parameters("OMRequest rq", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMREQUEST);
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testWrongReturnValueForPostProcess(Class<?> annotationClass,
                                                                               V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(postProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("String"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(ERROR_VALIDATOR_METHOD_HAS_TO_RETURN_OMRESPONSE);
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testWrongFirstArgumentForPreProcess(Class<?> annotationClass,
                                                                                V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("String rq", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST);
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testWrongFirstArgumentForPostProcess(Class<?> annotationClass,
                                                                                 V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(postProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("String rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(ERROR_FIRST_PARAM_HAS_TO_BE_OMREQUEST);
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testWrongSecondArgumentForPreProcess(Class<?> annotationClass,
                                                                                 V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(preProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMRequest"),
        parameters("OMRequest rq", "String ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT);
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testWrongSecondArgumentForPostProcess(Class<?> annotationClass,
                                                                                  V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(postProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "String rp", "ValidationContext ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(ERROR_SECOND_PARAM_HAS_TO_BE_OMRESPONSE);
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testWrongThirdArgumentForPostProcess(Class<?> annotationClass,
                                                                                 V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(postProcess(), aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "String ctx"),
        exceptions(), annotationClass);

    assertThat(compile(source))
        .hadErrorContaining(ERROR_LAST_PARAM_HAS_TO_BE_VALIDATION_CONTEXT);
  }
  
  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testInvalidProcessingPhase(Class<?> annotationClass,
                                                                       V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf("INVALID", aReqType(), annotationClass, version),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions("ServiceException"), annotationClass);

    assertThat(compile(source)).failed();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testInvalidClientVersion(Class<?> annotationClass,
                                                                     V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(RequestProcessingPhase.PRE_PROCESS, aReqType(), annotationClass, null),
        modifiers("public", "static"),
        returnValue("OMResponse"),
        parameters("OMRequest rq", "OMResponse rp", "ValidationContext ctx"),
        exceptions("ServiceException"), annotationClass);

    assertThat(compile(source)).failed();
  }

  @ParameterizedTest
  @MethodSource("annotatedClasses")
  public <V extends Enum<V> & Versioned> void testMultipleErrorMessages(Class<?> annotationClass,
                                                                      V version) {
    List<String> source = generateSourceOfValidatorMethodWith(
        annotationOf(postProcess(), aReqType(), annotationClass, version),
        modifiers(),
        returnValue("String"),
        parameters("String rq", "int rp", "String ctx"),
        exceptions(), annotationClass);

    Compilation compilation = compile(source);
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
        .withProcessors(new OmRequestFeatureValidatorProcessor())
        .compile(JavaFileObjects.forSourceLines(CLASSNAME, source));
    c.diagnostics().forEach(System.out::println);
    return c;
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
      String[] exceptions,
      Class<?> annotationClass) {
    List<String> lines = new ArrayList<>(allImports(annotationClass));
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
    lines.stream().forEach(System.out::println);
    return lines;
  }

  private <V extends Enum<V> & Versioned> String annotationOf(
      RequestProcessingPhase phase, Type reqType, Class<?> annotationClass, V applyBeforeVersion) {
    return annotationOf(phase.name(), reqType, annotationClass, applyBeforeVersion);
  }

  private <V extends Enum<V> & Versioned> String annotationOf(
      String phase,
      Type reqType,
      Class<?> annotationClass,
      V applyBeforeVersion) {
    StringBuilder annotation = new StringBuilder();
    annotation.append('@').append(annotationClass.getName()).append('(');
    annotation.append("processingPhase = ").append(phase);
    annotation.append(", requestType = ").append(reqType.name());
    if (applyBeforeVersion != null) {
      annotation.append(", applyBefore = ").append(applyBeforeVersion.name());
    }
    annotation.append(" )");
    return annotation.toString();
  }

  private List<String> allImports(Class<?> annotationClass) {
    List<String> imports = new ArrayList<>();
    imports.add("import org.apache.hadoop.ozone.protocol.proto"
        + ".OzoneManagerProtocolProtos.OMRequest;");
    imports.add("import org.apache.hadoop.ozone.protocol.proto"
        + ".OzoneManagerProtocolProtos.OMResponse;");
    imports.add("import org.apache.hadoop.ozone.om.request.validation"
        + ".ValidationContext;");
    imports.add("import com.google.protobuf.ServiceException;");
    for (RequestProcessingPhase phase : RequestProcessingPhase.values()) {
      imports.add("import static org.apache.hadoop.ozone.request.validation"
          + ".RequestProcessingPhase." + phase.name() + ";");
    }
    for (Type reqType : Type.values()) {
      imports.add("import static org.apache.hadoop.ozone.protocol.proto"
          + ".OzoneManagerProtocolProtos.Type." + reqType.name() + ";");
    }
    imports.add("import " + annotationClass.getName() + ";");

    for (Object enumConstant : ANNOTATION_VERSION_CLASS_MAP.get(annotationClass).getEnumConstants()) {
      imports.add("import static " + ANNOTATION_VERSION_CLASS_MAP.get(annotationClass).getCanonicalName() + "." +
          ((Enum)enumConstant).name() + ";");
    }
    return imports;
  }

  private StringBuilder buildMethodSignature(
      String[] modifiers, String returnType,
      String[] paramspecs, String[] exceptions) {
    StringBuilder signature = new StringBuilder();
    signature.append("  ");
    for (String modifier : modifiers) {
      signature.append(modifier).append(' ');
    }
    signature.append(returnType).append(' ');
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
