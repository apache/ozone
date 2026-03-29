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

package org.apache.hadoop.hdds.scm.ha;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * A tool to (manually) generate code for copy-paste.
 */
public class ScmInvokerCodeGenerator {
  static final DeclaredMethod INVOKE_LOCAL = new DeclaredMethod("invokeLocal",
      new Class[]{String.class, Object[].class},
      new String[]{"methodName", "params"},
      Object.class,
      new Class<?>[]{Exception.class});

  private final Class<?> api;
  private final String apiName;
  private final String requestTypeName;
  private final String invokerClassName;

  private final PrintStream out = System.out;
  private String indentation = "";

  public ScmInvokerCodeGenerator(Class<?> api, RequestType type) {
    this.api = api;
    this.apiName = api.getSimpleName();
    this.requestTypeName = type.name();
    this.invokerClassName = apiName + "Invoker";
  }

  void printf(String format, Object... args) {
    out.printf(indentation + format, args);
  }

  void println(String format, Object... args) {
    printf(format, args);
    out.println();
  }

  UncheckedAutoCloseable printScope() {
    return printScope(true, true);
  }

  UncheckedAutoCloseable printScope(boolean codeBlock, boolean intend) {
    out.println(codeBlock ? " {" : "");
    if (intend) {
      indentation += "  ";
    }
    return () -> {
      if (intend) {
        indentation = indentation.substring(2);
      }
      if (codeBlock) {
        println("}");
      }
    };
  }

  static String classesToString(Class<?>[] classes) {
    return arrayToString(classes, Class::getSimpleName);
  }

  static <T> String arrayToString(T[] objects, Function<T, String> toString) {
    return Arrays.stream(objects)
        .map(toString)
        .reduce(null, (a, b) -> a == null ? b : a + ", " + b);
  }

  void printHeaderMethods() {
    out.println();
    printf("public %s(%s impl)", invokerClassName, apiName);
    try (UncheckedAutoCloseable ignore = printScope()) {
      println("this.impl = impl;");
    }

    out.println();
    println("@Override");
    printf("public RequestType getType()");
    try (UncheckedAutoCloseable ignore = printScope()) {
      println("return %s;", requestTypeName);
    }

    out.println();
    println("@Override");
    printf("public Class<%s> getApi()", apiName);
    try (UncheckedAutoCloseable ignore = printScope()) {
      println("return %s.class;", apiName);
    }

    out.println();
    println("@Override");
    printf("public %s getImpl()", apiName);
    try (UncheckedAutoCloseable ignore = printScope()) {
      println("return impl;");
    }
  }

  void printMethodSignature(DeclaredMethod method) {
    printf("public %s %s(%s) throws %s",
        method.getReturnType().getSimpleName(),
        method.getName(),
        method.getParameterString(),
        classesToString(method.getExceptionTypes()));
  }

  void printCase(Method apiMethod, String actualParameter, AtomicInteger argCount) {
    printf("case \"%s\":", apiMethod.getName());
    try (UncheckedAutoCloseable ignore = printScope(false, true)) {
      final Parameter[] apiParameters = apiMethod.getParameters();
      final StringBuilder b = new StringBuilder();
      for (int i = 0; i < apiParameters.length; i++) {
        final Parameter p = apiParameters[i];
        final String classname = p.getType().getSimpleName();
        final String arg = "arg" + argCount.getAndIncrement();
        b.append(arg).append(", ");
        println("final %s %s = %s.length > %d ? (%s) %s[%d] : null;", classname, arg,
            actualParameter, i, classname, actualParameter, i);
      }
      if (b.length() > 0) {
        b.setLength(b.length() - 2);
      }
      if (apiMethod.getReturnType() == void.class) {
        println("impl.%s(%s);", apiMethod.getName(), b);
        println("return null;");
      } else {
        println("return impl.%s(%s);", apiMethod.getName(), b);
      }
    }
    out.println();
  }

  void printSwitch(DeclaredMethod method) {
    final String switchName = method.getParameterName(0);
    printf("switch (%s)", switchName);
    try (UncheckedAutoCloseable ignored = printScope(true, false)) {
      final Method[] apiMethods = api.getMethods();
      Arrays.sort(apiMethods, Comparator.comparing(Method::getName));
      final AtomicInteger argCount = new AtomicInteger(0);
      for (Method apiMethod : apiMethods) {
        if (!apiMethod.isDefault() && !Modifier.isStatic(apiMethod.getModifiers())) {
          printCase(apiMethod, method.getParameterName(1), argCount);
        }
      }

      printf("default:");
      try (UncheckedAutoCloseable ignore = printScope(false, true)) {
        println("throw new IllegalArgumentException(\"Method not found: \" + %s + \" in %s\");",
            switchName, apiName);
      }
    }
  }

  void printInvokeMethod(DeclaredMethod method) {
    out.println();
    println("@SuppressWarnings(\"unchecked\")");
    println("@Override");
    printMethodSignature(method);
    try (UncheckedAutoCloseable ignored = printScope()) {
      printSwitch(method);
    }
  }

  public void generateClass() {
    println("/** Code generated for %s.  Do not modify. */", apiName);
    printf("public %s implements ScmInvoker<%s>", invokerClassName, apiName);
    try (UncheckedAutoCloseable ignored = printScope()) {
      println("private final %s impl;", apiName);
      printHeaderMethods();
      printInvokeMethod(INVOKE_LOCAL);
    }
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

    String getName() {
      return name;
    }

    String getParameterName(int i) {
      return parameterNames[i];
    }

    String getParameterString() {
      final StringBuilder b = new StringBuilder();
      for (int i = 0; i < parameterNames.length; i++) {
        b.append(parameterTypes[i].getSimpleName()).append(' ')
            .append(parameterNames[i]).append(", ");
      }
      b.setLength(b.length() - 2);
      return b.toString();
    }

    Class<?> getReturnType() {
      return returnType;
    }

    Class<?>[] getExceptionTypes() {
      return exceptionTypes;
    }
  }
}
