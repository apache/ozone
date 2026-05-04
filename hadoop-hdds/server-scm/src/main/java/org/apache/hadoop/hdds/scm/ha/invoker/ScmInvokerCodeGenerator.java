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

package org.apache.hadoop.hdds.scm.ha.invoker;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * Generate code for {@link ScmInvoker} implementations.
 * Step 1. Create the target java file in {@link #DIR}.  It will be used as an input for license header and imports.
 * Step 2. Add main method to the API interface.
 * Step 3. Manually fix imports.
 * <p>
 * Below is an example for generating the API interface FinalizationStateManager:
 * Step 1. Copy FinalizationStateManager.java to DIR/FinalizationStateManagerInvoker.java
 * Step 2. //FinalizationStateManager
 *         static void main(String[] args) {
 *           ScmInvokerCodeGenerator.generate(FinalizationStateManager.class, true);
 *         }
 * Step 3. Manually fix imports.
 */
public final class ScmInvokerCodeGenerator {
  static final String DIR = "hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/ha/invoker/";
  static final int LINE_LENGTH = 120;

  static final DeclaredMethod INVOKE_LOCAL = new DeclaredMethod("invokeLocal",
      new Class[]{String.class, Object[].class},
      new String[]{"methodName", "p"},
      Message.class,
      new Class<?>[]{Exception.class});

  private final Class<?> api;
  private final String apiName;
  private final String invokerClassName;

  private final StringWriter lineBuffer = new StringWriter();
  private final StringWriter out = new StringWriter();
  private String indentation = "";

  private ScmInvokerCodeGenerator(Class<?> api) {
    this.api = api;
    this.apiName = api.getSimpleName();
    this.invokerClassName = apiName + "Invoker";

  }

  void printf(String format, Object... args) {
    printf(true, format, args);
  }

  void printf(boolean indent, String format, Object... args) {
    if (indent) {
      lineBuffer.append(indentation);
    }
    lineBuffer.append(String.format(format, args));
  }

  void println() {
    println(false, "");
  }

  void println(String format, Object... args) {
    println(true, format, args);
  }

  void println(boolean indent, String format, Object... args) {
    printf(indent, format, args);

    String s = lineBuffer.toString();
    lineBuffer.getBuffer().setLength(0);

    final int nonSpace = getLeadingSpaces(s);
    String continuation = s.substring(0, nonSpace) + "    ";
    while (s.length() > LINE_LENGTH) {
      final int i = getLineBreakIndex(s);
      out.append(stripTrailingSpaces(s.substring(0, i)))
          .append(System.lineSeparator());
      s = continuation + stripLeadingSpaces(s.substring(i));
    }
    out.append(s).append(System.lineSeparator());
  }

  static int getLeadingSpaces(String s) {
    int nonSpace = 0;
    while (nonSpace < s.length() && s.charAt(nonSpace) == ' ') {
      nonSpace++;
    }
    return nonSpace;
  }

  static int getLineBreakIndex(String s) {
    int i = s.lastIndexOf(' ', LINE_LENGTH);
    if (i <= 0) {
      return LINE_LENGTH;
    }

    if ("{".equals(s.substring(i).trim())) {
      final int comma = s.lastIndexOf(',', i);
      if (comma > 0) {
        return comma + 1;
      }
    }
    return i;
  }

  UncheckedAutoCloseable printScope() {
    return printScope(true, 1);
  }

  UncheckedAutoCloseable printScope(boolean codeBlock, int intendLevel) {
    println(false, codeBlock ? " {" : "");
    for (int i = 0; i < intendLevel; i++) {
      indentation += "  ";
    }
    return () -> {
      if (intendLevel > 0) {
        indentation = indentation.substring(2 * intendLevel);
      }
      if (codeBlock) {
        println("}");
      }
    };
  }

  static String getClassname(Class<?> clazz) {
    return getClassnameBuilder(clazz).toString();
  }

  String getClassnameForApi(Class<?> clazz) {
    if (clazz.getEnclosingClass() != null
        && clazz.getEnclosingClass().getSimpleName().endsWith("Protos")
        && hasSimpleNameConflict(clazz)) {
      return clazz.getEnclosingClass().getSimpleName() + "." + clazz.getSimpleName();
    }
    return getClassname(clazz);
  }

  boolean hasSimpleNameConflict(Class<?> clazz) {
    final String simpleName = clazz.getSimpleName();
    return Arrays.stream(api.getMethods())
        .flatMap(method -> Stream.concat(
            Arrays.stream(method.getParameterTypes()),
            Stream.of(method.getReturnType())))
        .anyMatch(type -> type != clazz && type.getSimpleName().equals(simpleName));
  }

  String getParameterStringForApi(Class<?>[] types, String[] names) {
    final StringBuilder b = new StringBuilder();
    final int n = names != null ? names.length : types.length;
    for (int i = 0; i < n; i++) {
      b.append(getClassnameForApi(types[i])).append(' ')
          .append(names != null ? names[i] : "arg" + i).append(", ");
    }
    if (b.length() > 0) {
      b.setLength(b.length() - 2);
    }
    return b.toString();
  }

  String classesToStringForApi(Class<?>[] classes, String suffix) {
    return arrayToString(classes, c -> getClassnameForApi(c) + suffix);
  }

  static StringBuilder getClassnameBuilder(Class<?> clazz) {
    final StringBuilder b = new StringBuilder(clazz.getSimpleName());
    for (Class<?> c = clazz.getEnclosingClass(); c != null; c = c.getEnclosingClass()) {
      final String name = c.getSimpleName();
      if (name.endsWith("Protos")) {
        break;
      }
      b.insert(0, name + ".");
    }
    return b;
  }

  static String getReturnTypeString(Method method) {
    final StringBuilder b = getClassnameBuilder(method.getReturnType());
    final String generic = method.getGenericReturnType().getTypeName();
    int i = generic.indexOf('<');
    if (i < 0) {
      return b.toString();
    }
    b.append('<');
    for (boolean more = true; more;) {
      final int j = generic.indexOf(", ", i);
      more = j >= 0;
      try {
        final Class<?> clazz = Class.forName(generic.substring(i + 1, more ? j : generic.length() - 1));
        b.append(getClassname(clazz)).append(", ");
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Failed to getReturnTypeString for  " + generic, e);
      }
      i = j + 1;
    }
    b.setLength(b.length() - 2);
    return b.append('>').toString();
  }

  static String classesToString(Class<?>[] classes, String suffix) {
    return arrayToString(classes, c -> getClassnameBuilder(c).append(suffix).toString());
  }

  static <T> String arrayToString(T[] objects, Function<T, String> toString) {
    return Arrays.stream(objects)
        .map(toString)
        .reduce("", (a, b) -> a.isEmpty() ? b : a + ", " + b);
  }

  static String getThrowsString(Class<?>[] exceptions) {
    if (exceptions.length == 0) {
      return "";
    }
    return " throws " + classesToString(exceptions, "");
  }

  static String getParameterString(Class<?>[] types, String[] names) {
    final StringBuilder b = new StringBuilder();
    final int n = names != null ? names.length : types.length;
    for (int i = 0; i < n; i++) {
      b.append(getClassname(types[i])).append(' ')
          .append(names != null ? names[i] : "arg" + i).append(", ");
    }
    if (b.length() > 0) {
      b.setLength(b.length() - 2);
    }
    return b.toString();
  }

  static Predicate<Method> getFilter(Boolean isDefault, Boolean isDeprecated) {
    return m -> {
      if (Modifier.isStatic(m.getModifiers())) {
        return false;
      }
      if (isDefault != null && isDefault != m.isDefault()) {
        return false;
      }
      if (isDeprecated != null && isDeprecated == (m.getAnnotation(Deprecated.class) == null)) {
        return false;
      }
      return true;
    };
  }

  List<Method> getMethods(Boolean isDefault, Boolean isDeprecated) {
    return getMethods(getFilter(isDefault, isDeprecated));
  }

  List<Method> getMethods(Predicate<Method> filter) {
    return Arrays.stream(api.getMethods())
        .filter(filter)
        .sorted(Comparator.comparing(Method::getName).thenComparing(Method::getParameterCount))
        .collect(Collectors.toList());
  }

  void printEnum() {
    printf("enum ReplicateMethod implements NameAndParameterTypes");
    try (UncheckedAutoCloseable ignore = printScope()) {
      final List<Method> apiMethods = getMethods(null, null);
      boolean first = true;
      for (Method m : apiMethods) {
        if (m.isDefault() || m.getAnnotation(Replicate.class) == null) {
          continue;
        }
        if (first) {
          first = false;
        } else {
          println(false, ",");
        }

        final List<Method> overrides = apiMethods.stream()
            .filter(method -> method.getName().equals(m.getName()))
            .sorted(Comparator.comparing(Method::getParameterCount))
            .collect(Collectors.toList());
        printEnumConstant(overrides);
      }
      println(false, ";");

      printEnumBody();
    }
  }

  private void printEnumBody() {
    println();
    println("private final Class<?>[][] parameterTypes;");

    println();
    printf("ReplicateMethod(Class<?>[][] parameterTypes)");
    try (UncheckedAutoCloseable ignore = printScope()) {
      println("this.parameterTypes = parameterTypes;");
    }

    println();
    println("@Override");
    printf("public Class<?>[] getParameterTypes(int numArgs)");
    try (UncheckedAutoCloseable ignore = printScope()) {
      println("return parameterTypes[numArgs];");
    }
  }

  private void printEnumConstant(List<Method> overrides) {
    final Class<?>[][] parameterTypes = getParameterTypes(overrides);
    printf("%s(new Class<?>[][] {", overrides.get(0).getName());
    try (UncheckedAutoCloseable ignore = printScope(false, 2)) {
      for (int i = 0; i < parameterTypes.length; i++) {
        if (i > 0) {
          println(false, ",");
        }

        final Class<?>[] classes = parameterTypes[i];
        if (classes == null) {
          printf("null");
        } else {
          printf("new Class<?>[] {%s}", classesToStringForApi(classes, ".class"));
        }
      }
    }
    println();
    printf("})");
  }

  static Class<?>[][] getParameterTypes(List<Method> overrides) {
    final Method last = overrides.get(overrides.size() - 1);
    final Class<?>[][] types = new Class<?>[last.getParameterCount() + 1][];
    for (Method method : overrides) {
      Preconditions.assertEquals(last.getName(), method.getName(), "methodName");
      final int i = method.getParameterCount();
      if (types[i] != null) {
        throw new IllegalArgumentException("Duplicate method parameters found: " + overrides);
      }
      types[i] = method.getParameterTypes();
    }
    return types;
  }

  void printHeaderMethods() {
    println();
    printf("public %s(%s impl, SCMRatisServer ratis)", invokerClassName, apiName);
    try (UncheckedAutoCloseable ignore = printScope()) {
      println("super(impl, %s::newProxy, ratis);", invokerClassName);
    }

    println();
    println("@Override");
    printf("public Class<%s> getApi()", apiName);
    try (UncheckedAutoCloseable ignore = printScope()) {
      println("return %s.class;", apiName);
    }
  }

  void printCase(List<Method> apiMethods, String actualParameter, String switchName, AtomicInteger argCount) {
    printf("case \"%s\":", apiMethods.get(0).getName());
    try (UncheckedAutoCloseable ignore = printScope(false, 1)) {
      if (apiMethods.size() == 1) {
        printMethodCall(apiMethods.get(0), actualParameter, argCount, false);
      } else {
        for (Method apiMethod : apiMethods) {
          printf("if (%s)", getParameterMatchCondition(apiMethod, actualParameter));
          try (UncheckedAutoCloseable ignored = printScope()) {
            printMethodCall(apiMethod, actualParameter, argCount, true);
          }
        }
        printMethodNotFound(null, switchName);
      }
    }
    println();
  }

  void printMethodCall(Method apiMethod, String actualParameter, AtomicInteger argCount, boolean exactArgs) {
    final Parameter[] apiParameters = apiMethod.getParameters();
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < apiParameters.length; i++) {
      final Parameter p = apiParameters[i];
      final String classname = getClassnameForApi(p.getType());
      final String arg = "arg" + argCount.getAndIncrement();
      b.append(arg).append(", ");
      if (exactArgs) {
        println("final %s %s = (%s) %s[%d];", classname, arg, classname, actualParameter, i);
      } else {
        println("final %s %s = %s.length > %d ? (%s) %s[%d] : %s;", classname, arg,
            actualParameter, i, classname, actualParameter, i, getDefaultValue(p.getType()));
      }
    }
    if (b.length() > 0) {
      b.setLength(b.length() - 2);
    }
    if (apiMethod.getReturnType() == void.class) {
      println("getImpl().%s(%s);", apiMethod.getName(), b);
      println("return Message.EMPTY;");
    } else {
      println("returnType = %s.class;", getClassnameForApi(apiMethod.getReturnType()));
      println("returnValue = getImpl().%s(%s);", apiMethod.getName(), b);
      println("break;");
    }
  }

  String getParameterMatchCondition(Method method, String actualParameter) {
    final StringBuilder b = new StringBuilder()
        .append(actualParameter)
        .append(".length == ")
        .append(method.getParameterCount());

    final Class<?>[] parameterTypes = method.getParameterTypes();
    for (int i = 0; i < parameterTypes.length; i++) {
      final Class<?> type = parameterTypes[i];
      final String checkType = getClassnameForApi(type.isPrimitive() ? getBoxedType(type) : type);
      if (type.isPrimitive()) {
        b.append(" && ").append(actualParameter).append('[').append(i).append("] instanceof ")
            .append(checkType);
      } else {
        b.append(" && (").append(actualParameter).append('[').append(i).append("] == null || ")
            .append(checkType).append(".class.isInstance(").append(actualParameter).append('[')
            .append(i).append("]))");
      }
    }
    return b.toString();
  }

  static Class<?> getBoxedType(Class<?> type) {
    if (type == boolean.class) {
      return Boolean.class;
    } else if (type == byte.class) {
      return Byte.class;
    } else if (type == char.class) {
      return Character.class;
    } else if (type == short.class) {
      return Short.class;
    } else if (type == int.class) {
      return Integer.class;
    } else if (type == long.class) {
      return Long.class;
    } else if (type == float.class) {
      return Float.class;
    } else if (type == double.class) {
      return Double.class;
    }
    return type;
  }

  static String getDefaultValue(Class<?> type) {
    if (!type.isPrimitive()) {
      return "null";
    } else if (type == boolean.class) {
      return "false";
    } else if (type == char.class) {
      return "'\\0'";
    } else if (type == long.class) {
      return "0L";
    } else if (type == float.class) {
      return "0F";
    } else if (type == double.class) {
      return "0D";
    }
    return "0";
  }

  void printMethodNotFound(String label, String switchName) {
    if (label != null) {
      printf("%s:", label);
      try (UncheckedAutoCloseable ignore = printScope(false, 1)) {
        println("throw new IllegalArgumentException(\"Method not found: \" + %s + \" in %s\");",
            switchName, apiName);
      }
    } else {
      println("throw new IllegalArgumentException(\"Method not found: \" + %s + \" in %s\");",
          switchName, apiName);
    }
  }

  void printSwitch(DeclaredMethod method) {
    final String switchName = method.getParameterName(0);
    printf("switch (%s)", switchName);
    try (UncheckedAutoCloseable ignored = printScope(true, 0)) {
      final AtomicInteger argCount = new AtomicInteger(0);
      final Map<String, List<Method>> methodsByName = getMethods(false, null).stream()
          .collect(Collectors.groupingBy(
              Method::getName, LinkedHashMap::new, Collectors.toList()));

      for (List<Method> apiMethods : methodsByName.values()) {
        printCase(apiMethods, method.getParameterName(1), switchName, argCount);
      }

      printMethodNotFound("default", switchName);
    }
  }

  void printInvokeMethod(DeclaredMethod method) {
    println();
    println("@SuppressWarnings(\"unchecked\")");
    println("@Override");
    printf(method.getSignature());
    try (UncheckedAutoCloseable ignored = printScope()) {
      println("final Class<?> returnType;");
      println("final Object returnValue;");
      printSwitch(method);
      println();
      println("return SCMRatisResponse.encode(returnValue, returnType);");
    }
  }

  void printProxyClassMethod(Method method) {
    final Replicate r = method.getAnnotation(Replicate.class);
    if (r == null && method.isDefault()) {
      // Do not print non-Replicate default methods, just use default implementation.
      return;
    }

    println();
    println("@Override");
    printf("public %s %s(%s)%s",
        getReturnTypeString(method),
        method.getName(),
        getParameterStringForApi(method.getParameterTypes(), null),
        getThrowsString(method.getExceptionTypes()));

    try (UncheckedAutoCloseable ignored = printScope()) {
      final String args = IntStream.range(0, method.getParameterCount())
          .mapToObj(i -> "arg" + i)
          .reduce("", (a, b) -> a.isEmpty() ? b : a + ", " + b);
      final String returnString = method.getReturnType() == void.class ? "" : "return ";
      if (r != null) {
        final String type = r.invocationType() == Replicate.InvocationType.DIRECT ? "Direct" : "Client";
        println("final Object[] args = {%s};", args);
        println("%sinvoker.invokeReplicate%s(ReplicateMethod.%s, args);", returnString, type, method.getName());
      } else {
        println("%sinvoker.getImpl().%s(%s);", returnString, method.getName(), args);
      }
    }
  }

  void printProxyClass() {
    printf("return new %s() {", apiName);
    try (UncheckedAutoCloseable ignored = printScope(false, 1)) {
      for (Method m : getMethods(null, false)) {
        printProxyClassMethod(m);
      }
    }
    println("};");
  }

  void printProxyMethod() {
    println();
    printf("static %s newProxy(ScmInvoker<%s> invoker)", apiName, apiName);
    try (UncheckedAutoCloseable ignored = printScope()) {
      printProxyClass();
    }
  }

  public String generateClass() {
    println("/** Code generated for {@link %s}.  Do not modify. */", apiName);
    printf("public class %s extends ScmInvoker<%s>", invokerClassName, apiName);
    try (UncheckedAutoCloseable ignored = printScope()) {
      printEnum();
      printHeaderMethods();
      printProxyMethod();
      printInvokeMethod(INVOKE_LOCAL);
    }
    return out.toString();
  }

  File updateFile(String classString) throws IOException {
    final File java = new File(DIR, invokerClassName + ".java");
    if (!java.isFile()) {
      throw new FileNotFoundException("Not found: " + java.getAbsolutePath());
    }
    final File tmp = new File(DIR, invokerClassName + "_tmp.java");
    if (tmp.exists()) {
      throw new IOException("Already exist: " + java.getAbsolutePath());
    }
    try (InputStream inStream = Files.newInputStream(java.toPath());
         BufferedReader in = new BufferedReader(new InputStreamReader(new BufferedInputStream(inStream), UTF_8));
         OutputStream outStream = Files.newOutputStream(tmp.toPath(), StandardOpenOption.CREATE_NEW);
         PrintWriter out = new PrintWriter(new OutputStreamWriter(outStream, UTF_8), true)) {
      String line;
      for (; (line = in.readLine()) != null;) {
        out.println(line);
        if (line.startsWith("import")) {
          break;
        }
      }
      for (; (line = in.readLine()) != null && line.startsWith("import");) {
        out.println(line);
      }
      out.println();
      out.print(classString);
    }

    Files.move(tmp.toPath(), java.toPath(), StandardCopyOption.REPLACE_EXISTING);
    return java;
  }

  public static void generate(Class<?> api, boolean updateFile) {
    final ScmInvokerCodeGenerator generator = new ScmInvokerCodeGenerator(api);
    final String classString = generator.generateClass();
    if (!updateFile) {
      System.out.println(classString);
      return;
    }

    final File file;
    try {
      file = generator.updateFile(classString);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to updateFile", e);
    }
    System.out.printf("Successfully update file: %s%n", file);
  }

  static class DeclaredMethod {
    private final String name;
    private final Class<?>[] parameterTypes;
    private final String[] parameterNames;
    private final Class<?> returnType;
    private final Class<?>[] exceptionTypes;

    DeclaredMethod(String name, Class<?>[] parameterTypes, String[] parameterNames,
        Class<?> returnType, Class<?>[] exceptionTypes) {
      this.name = name;
      this.parameterTypes = parameterTypes;
      this.parameterNames = parameterNames;
      this.returnType = returnType;
      this.exceptionTypes = exceptionTypes;
    }

    String getParameterName(int i) {
      return parameterNames[i];
    }

    String getSignature() {
      return String.format("public %s %s(%s)%s",
          getClassname(returnType),
          name,
          getParameterString(parameterTypes, parameterNames),
          getThrowsString(exceptionTypes));
    }
  }

  static String stripTrailingSpaces(String s) {
    int end = s.length();
    while (end > 0 && s.charAt(end - 1) == ' ') {
      end--;
    }
    return s.substring(0, end);
  }

  static String stripLeadingSpaces(String s) {
    int start = 0;
    while (start < s.length() && s.charAt(start) == ' ') {
      start++;
    }
    return s.substring(start);
  }
}
