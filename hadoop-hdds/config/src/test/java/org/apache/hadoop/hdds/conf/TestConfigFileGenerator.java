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

package org.apache.hadoop.hdds.conf;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test the ConfigFileGenerator.
 * <p>
 * ConfigFileGenerator is an annotation processor and activated for the
 * testCompile. Therefore, in the unit test we can check the content of the
 * generated ozone-default-generated.xml
 */
public class TestConfigFileGenerator {

  @TempDir
  private Path tempDir;

  @Test
  public void testGeneratedXml() throws FileNotFoundException {
    String generatedXml =
        new Scanner(new File("target/test-classes/ozone-default-generated.xml"),
            StandardCharsets.UTF_8.name())
            .useDelimiter("//Z")
            .next();

    assertThat(generatedXml)
        .as("annotation in ConfigurationExample")
        .contains("<name>ozone.test.config.bind.host</name>");

    assertThat(generatedXml)
        .contains("<tag>MANAGEMENT</tag>");
  }

  @Test
  public void testDuplicatedKeyFailsCompilation() throws IOException {
    JavaFileObject source = new SimpleJavaFileObject(
        URI.create("string:///DuplicatedKeys.java"), JavaFileObject.Kind.SOURCE) {
      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) {
        return "package org.apache.hadoop.hdds.conf;\n"
            + "@ConfigGroup(prefix = \"ozone.test.config\")\n"
            + "class DuplicatedKeys {\n"
            + "  @ConfigGroup(prefix = \"ozone.test.config.nested\")\n"
            + "  static class Nested {\n"
            + "    @Config(key = \"ozone.test.config.nested.same\", defaultValue = \"1\",\n"
            + "        description = \"first\", tags = {})\n"
            + "    int first;\n"
            + "    @Config(key = \"ozone.test.config.nested.same\", defaultValue = \"2\",\n"
            + "        description = \"second\", tags = {})\n"
            + "    int second;\n"
            + "  }\n"
            + "}\n";
      }
    };

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    List<String> options = Arrays.asList(
        "-proc:only",
        "-processor", ConfigFileGenerator.class.getName(),
        "-classpath", Config.class.getProtectionDomain().getCodeSource().getLocation().getPath(),
        "-d", tempDir.toString());

    try (StandardJavaFileManager files =
        compiler.getStandardFileManager(diagnostics, null, null)) {
      assertThat(compiler.getTask(null, files, diagnostics, options, null,
          Arrays.asList(source)).call())
          .isFalse();
    }

    assertThat(diagnostics.getDiagnostics())
        .anyMatch(diagnostic ->
            diagnostic.getMessage(null).contains(
                "Duplicated config key 'ozone.test.config.nested.same'"));
  }
}
