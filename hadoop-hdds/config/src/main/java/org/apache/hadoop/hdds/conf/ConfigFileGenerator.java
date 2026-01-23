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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

/**
 * Annotation processor to generate config fragments from Config annotations.
 */
@SupportedAnnotationTypes("org.apache.hadoop.hdds.conf.ConfigGroup")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class ConfigFileGenerator extends AbstractProcessor {

  private static final String OUTPUT_FILE_NAME = "ozone-default-generated.xml";
  private static final String OUTPUT_FILE_POSTFIX = "-default.xml";

  @Override
  public boolean process(Set<? extends TypeElement> annotations,
      RoundEnvironment roundEnv) {
    if (roundEnv.processingOver()) {
      return false;
    }

    Filer filer = processingEnv.getFiler();

    try {

      //load existing generated config (if exists)
      boolean resourceExists = true;
      ConfigFileAppender appender = new ConfigFileAppender();
      String currentArtifactId = processingEnv.getOptions().get("artifactId");
      String outputFileName;
      if (currentArtifactId == null || currentArtifactId.isEmpty()) {
        outputFileName = OUTPUT_FILE_NAME;
      } else {
        outputFileName = currentArtifactId + OUTPUT_FILE_POSTFIX;
      }
      try (InputStream input = filer
          .getResource(StandardLocation.CLASS_OUTPUT, "",
              outputFileName).openInputStream()) {
        appender.load(input);
      } catch (FileNotFoundException | NoSuchFileException ex) {
        appender.init();
        resourceExists = false;
      }

      Set<? extends Element> annotatedElements =
          roundEnv.getElementsAnnotatedWith(ConfigGroup.class);
      for (Element annotatedElement : annotatedElements) {
        TypeElement configurationObject = (TypeElement) annotatedElement;

        ConfigGroup configGroupAnnotation =
            configurationObject.getAnnotation(ConfigGroup.class);

        writeConfigAnnotations(configGroupAnnotation, appender, configurationObject);
      }

      if (!resourceExists) {
        FileObject resource = filer
            .createResource(StandardLocation.CLASS_OUTPUT, "",
                outputFileName);

        try (Writer writer = new OutputStreamWriter(
            resource.openOutputStream(), StandardCharsets.UTF_8)) {
          appender.write(writer);
        }
      }

    } catch (IOException e) {
      processingEnv.getMessager().printMessage(Kind.ERROR,
          "Can't generate the config file from annotation: " + e);
    }
    return false;
  }

  private void writeConfigAnnotations(ConfigGroup configGroup,
      ConfigFileAppender appender,
      TypeElement typeElement) {
    for (Element element : typeElement.getEnclosedElements()) {
      if (element.getKind() == ElementKind.FIELD) {
        if (element.getAnnotation(Config.class) != null) {

          Config configAnnotation = element.getAnnotation(Config.class);
          String prefix = configGroup.prefix() + ".";
          String key = configAnnotation.key();
          if (!key.startsWith(prefix)) {
            processingEnv.getMessager().printMessage(Kind.ERROR,
                prefix + " is not a prefix of " + key,
                typeElement);
          }

          appender.addConfig(key,
              configAnnotation.defaultValue(),
              configAnnotation.description(),
              configAnnotation.tags());
        }
      }

    }
  }

}
