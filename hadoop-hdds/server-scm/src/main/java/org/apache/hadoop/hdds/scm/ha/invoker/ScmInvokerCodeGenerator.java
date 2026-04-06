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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * A tool to (manually) generate code for copy-paste.
 */
public final class ScmInvokerCodeGenerator {
  static final DeclaredMethod INVOKE_LOCAL = new DeclaredMethod("invokeLocal",
      new Class[]{String.class, Object[].class},
      new String[]{"methodName", "p"},
      Object.class,
      new Class<?>[]{Exception.class});

  private final Class<?> api;
  private final String apiName;
  private final String invokerClassName;

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
      out.append(indentation);
    }
    out.append(String.format(format, args));
  }

  void println() {
    println(false, "");
  }

  void println(String format, Object... args) {
    println(true, format, args);
  }

  void println(boolean indent, String format, Object... args) {
    printf(indent, format, args);
    out.append(System.lineSeparator());
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
          printf("new Class<?>[] {%s}", classesToString(classes, ".class"));
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

  void printCase(Method apiMethod, String actualParameter, AtomicInteger argCount) {
    printf("case \"%s\":", apiMethod.getName());
    try (UncheckedAutoCloseable ignore = printScope(false, 1)) {
      final Parameter[] apiParameters = apiMethod.getParameters();
      final StringBuilder b = new StringBuilder();
      for (int i = 0; i < apiParameters.length; i++) {
        final Parameter p = apiParameters[i];
        final String classname = getClassname(p.getType());
        final String arg = "arg" + argCount.getAndIncrement();
        b.append(arg).append(", ");
        println("final %s %s = %s.length > %d ? (%s) %s[%d] : null;", classname, arg,
            actualParameter, i, classname, actualParameter, i);
      }
      if (b.length() > 0) {
        b.setLength(b.length() - 2);
      }
      if (apiMethod.getReturnType() == void.class) {
        println("getImpl().%s(%s);", apiMethod.getName(), b);
        println("return null;");
      } else {
        println("return getImpl().%s(%s);", apiMethod.getName(), b);
      }
    }
    println();
  }

  void printSwitch(DeclaredMethod method) {
    final String switchName = method.getParameterName(0);
    printf("switch (%s)", switchName);
    try (UncheckedAutoCloseable ignored = printScope(true, 0)) {
      final AtomicInteger argCount = new AtomicInteger(0);
      for (Method apiMethod : getMethods(false, null)) {
        printCase(apiMethod, method.getParameterName(1), argCount);
      }

      printf("default:");
      try (UncheckedAutoCloseable ignore = printScope(false, 1)) {
        println("throw new IllegalArgumentException(\"Method not found: \" + %s + \" in %s\");",
            switchName, apiName);
      }
    }
  }

  void printInvokeMethod(DeclaredMethod method) {
    println();
    println("@SuppressWarnings(\"unchecked\")");
    println("@Override");
    printf(method.getSignature());
    try (UncheckedAutoCloseable ignored = printScope()) {
      printSwitch(method);
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
        getParameterString(method.getParameterTypes(), null),
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
    out.getBuffer().setLength(0);
    println("/** Code generated for %s.  Do not modify. */", apiName);
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
    final String dir = "hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/ha/invoker/";
    final File java = new File(dir, invokerClassName + ".java");
    if (!java.isFile()) {
      throw new FileNotFoundException("Not found: " + java.getAbsolutePath());
    }
    final File tmp = new File(dir, invokerClassName + "_tmp.java");
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
}
