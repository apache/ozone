/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics util for metrics.
 */
public final class MetricsUtil {
  private static final String ANNOTATIONS = "annotations";
  private static final String ANNOTATION_DATA = "annotationData";
  private static final Class<? extends Annotation> ANNOTATION_TO_ALTER
      = Metrics.class;

  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsUtil.class);
  
  private MetricsUtil() {
  }

  /**
   * register metric with changing class annotation for metrics.
   * 
   * @param source source to register
   * @param name name of metric
   * @param desc description of metric
   * @param context context of metric
   * @param <T> source type
   */
  public static <T> void registerDynamic(
      T source, String name, String desc, String context) {
    updateAnnotation(source.getClass(), name, desc, context);
    DefaultMetricsSystem.instance().register(name, desc, source);
  }
  
  private static void updateAnnotation(
      Class clz, String name, String desc, String context) {
    try {
      Annotation annotationValue = new Metrics() {

        @Override
        public Class<? extends Annotation> annotationType() {
          return ANNOTATION_TO_ALTER;
        }

        @Override
        public String name() {
          return name;
        }

        @Override
        public String about() {
          return desc;
        }

        @Override
        public String context() {
          return context;
        }
      };
      
      Method method = clz.getClass().getDeclaredMethod(
          ANNOTATION_DATA, null);
      method.setAccessible(true);
      Object annotationData = method.invoke(clz);
      Field annotations = annotationData.getClass()
          .getDeclaredField(ANNOTATIONS);
      annotations.setAccessible(true);
      Map<Class<? extends Annotation>, Annotation> map =
          (Map<Class<? extends Annotation>, Annotation>) annotations
              .get(annotationData);
      map.put(ANNOTATION_TO_ALTER, annotationValue);
    } catch (Exception e) {
      LOG.error("Update Metrics annotation failed. ", e);
    }
  }
}
