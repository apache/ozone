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
import java.util.function.Function;
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

  private final PrintStream out;
  private String indentation = "  ";

  public ScmInvokerCodeGenerator(PrintStream out) {
    this.out = out;
  }

  void printf(String format, Object... args) {
    out.printf(indentation + format, args);
  }

  void println(String format, Object... args) {
    printf(format, args);
    out.println();
  }

  UncheckedAutoCloseable printScope() {
    out.println(" {");
    indentation += "  ";
    return () -> {
      indentation = indentation.substring(2);
      println("}");
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

  void printMethod(DeclaredMethod method) {
    printf("public %s %s(%s) throws %s",
        method.getReturnType().getSimpleName(),
        method.getName(),
        method.getParameterString(),
        classesToString(method.getExceptionTypes()));
  }

  void printCase(Method apiMethod, String actualParameter) {
    printf("case \"%s\":", apiMethod.getName());
    try (UncheckedAutoCloseable ignored = printScope()) {

      final Parameter[] apiParameters = apiMethod.getParameters();
      final StringBuilder b = new StringBuilder();
      for (int i = 0; i < apiParameters.length; i++) {
        final Parameter p = apiParameters[i];
        final String classname = p.getType().getSimpleName();
        b.append(p.getName()).append(", ");
        println("final %s %s = %s.length > %d ? (%s) %s[%d] : null;", classname, p.getName(),
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
  }

  void printSwitch(Class<?> api, DeclaredMethod method) {

    final String switchName = method.getParameterName(0);
    printf("switch (%s)", switchName);
    try (UncheckedAutoCloseable ignored = printScope()) {
      for (Method apiMethod : api.getMethods()) {
        if (!Modifier.isStatic(apiMethod.getModifiers()) && !apiMethod.isDefault()) {
          printCase(apiMethod, method.getParameterName(1));
        }
      }
    }
    println("throw new IllegalArgumentException(\"Method not found: \" + %s);", switchName);
  }

  void generateMethod(Class<?> api, DeclaredMethod method) {
    println("// Code generated for %s.  Do not modify.", api.getSimpleName());
    println("@SuppressWarnings(\"unchecked\")");
    println("@Override");
    printMethod(method);
    try (UncheckedAutoCloseable ignored = printScope()) {
      printSwitch(api, method);
    }
  }

  public void generateInvokeLocal(Class<?> api) {
    generateMethod(api, INVOKE_LOCAL);
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
