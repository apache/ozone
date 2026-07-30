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

package org.apache.hadoop.ozone.upgrade;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.ComponentVersion;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common abstract provider for loading {@link UpgradeAction} implementations via reflection.
 *
 * @param <T> the concrete upgrade action type
 */
public abstract class AbstractUpgradeActionProvider<T extends UpgradeAction<?>>
    implements ComponentUpgradeActionProvider<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractUpgradeActionProvider.class);

  private final Set<Class<? extends Annotation>> annotationClasses;
  private final Class<T> actionClass;
  private final String[] packagesToScan;

  protected AbstractUpgradeActionProvider(Set<Class<? extends Annotation>> annotationClasses,
                                          Class<T> actionClass,
                                          String... packagesToScan) {
    this.annotationClasses = annotationClasses;
    this.actionClass = actionClass;
    this.packagesToScan = packagesToScan;
  }

  @Override
  public Map<ComponentVersion, T> load() {
    Map<ComponentVersion, T> upgradeActions = new HashMap<>();

    Reflections reflections = new Reflections(new ConfigurationBuilder()
        .forPackages(packagesToScan)
        .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner())
        .setExpandSuperTypes(false)
        .setParallel(true));
    Set<Class<?>> typesAnnotatedWith = new HashSet<>();
    for (Class<? extends Annotation> annotationClass : annotationClasses) {
      typesAnnotatedWith.addAll(reflections.getTypesAnnotatedWith(annotationClass));
    }

    typesAnnotatedWith.forEach(clazz -> {
      long annotationCount = annotationClasses.stream().filter(clazz::isAnnotationPresent).count();
      if (annotationCount > 1) {
        throw new IllegalStateException("Upgrade action class " + clazz.getName()
            + " has more than one upgrade action annotation. Only one is allowed.");
      }
      if (actionClass.isAssignableFrom(clazz)) {
        try {
          @SuppressWarnings("unchecked")
          T action = (T) clazz.getDeclaredConstructor().newInstance();
          ComponentVersion version = extractVersion(clazz);
          LOG.info("Registering Upgrade Action : {}", action.name());
          upgradeActions.put(version, action);
        } catch (Exception e) {
          LOG.error("Cannot instantiate Upgrade Action class {}",
              clazz.getSimpleName(), e);
        }
      } else {
        LOG.warn("Found upgrade action class not of type {} : {}",
            actionClass.getName(), clazz.getName());
      }
    });

    return upgradeActions;
  }

  /**
   * Subclasses must implement this to extract the version from the class's annotation.
   * Annotation interfaces cannot extend other interfaces, so there is no common way to extract the version from all
   * upgrade action annotations for different components.
   *
   * @param clazz class annotated with the action annotation
   * @return ComponentVersion the layout feature associated with the action
   */
  protected abstract ComponentVersion extractVersion(Class<?> clazz);
}
