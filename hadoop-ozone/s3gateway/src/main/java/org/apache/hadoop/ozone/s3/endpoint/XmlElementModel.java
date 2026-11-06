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

package org.apache.hadoop.ozone.s3.endpoint;

import com.google.common.collect.ImmutableSet;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * How a JAXB request class reads its XML body: for each element, whether JAXB reads it as one piece of text (a value)
 * or as a container of child elements. Built from the class's field annotations, once per request class.
 * <p>
 * Each node belongs to its parent, so an element name can mean different things under different parents. The walk
 * follows only the mappings the request classes use: a named {@code @XmlRootElement}, field access, fields that are
 * plain or annotated with {@code @XmlElement} or {@code @XmlAttribute}, values of type String, boolean, int, long or
 * an enum, model classes without a superclass, and a {@code List} of either. It fails on annotated methods, other
 * JAXB annotations on fields, other collection types, a type adapter on a field's type, two fields with the same
 * element name, and a model that contains itself, so that a new mapping cannot leave elements the filter does not
 * know about. Package-level type adapters are not checked; the request classes' package has none.
 */
final class XmlElementModel {

  /** An element read as one piece of text. */
  private static final XmlElementModel VALUE = new XmlElementModel(null);

  /** Field types read from an element's text. Any other field type must be a model class. */
  private static final Set<Class<?>> VALUE_TYPES =
      ImmutableSet.of(String.class, Boolean.class, boolean.class, Integer.class, int.class, Long.class, long.class);

  /** Child elements by name, or null for a value. */
  private final Map<String, XmlElementModel> children;

  private XmlElementModel(Map<String, XmlElementModel> children) {
    this.children = children;
  }

  /** The document of a request body: its only child is the request class's root element. */
  static XmlElementModel document(Class<?> requestClass) {
    XmlRootElement root = requestClass.getAnnotation(XmlRootElement.class);
    if (root == null || "##default".equals(root.name())) {
      throw new IllegalStateException(requestClass.getTypeName() + " needs @XmlRootElement with a name");
    }
    return new XmlElementModel(Collections.singletonMap(root.name(), container(requestClass, new HashSet<>())));
  }

  boolean isValue() {
    return children == null;
  }

  /** The child element with this name, or null if the request class does not define one. */
  XmlElementModel child(String name) {
    return children.get(name);
  }

  /** The tree with children sorted by name, for example {@code {Delete{Object{Key,VersionId},Quiet}}}. */
  @Override
  public String toString() {
    return isValue() ? "" : children.entrySet().stream()
        .map(child -> child.getKey() + child.getValue())
        .collect(Collectors.joining(",", "{", "}"));
  }

  /** Walks one model class; {@code enclosing} holds the classes above it, to refuse a model that contains itself. */
  private static XmlElementModel container(Class<?> type, Set<Class<?>> enclosing) {
    XmlAccessorType access = type.getAnnotation(XmlAccessorType.class);
    if (access == null || access.value() != XmlAccessType.FIELD) {
      throw new IllegalStateException(type.getTypeName() + " is not a JAXB model class with field access");
    }
    if (type.getSuperclass() != Object.class) {
      throw new IllegalStateException(type.getTypeName() + ": inherited fields are not supported");
    }
    if (!enclosing.add(type)) {
      throw new IllegalStateException(type.getTypeName() + ": a model that contains itself is not supported");
    }
    for (Method method : type.getDeclaredMethods()) {
      if (hasJaxbAnnotation(method, null)) {
        throw new IllegalStateException(type.getTypeName() + "." + method.getName() + ": methods are not supported");
      }
    }
    Map<String, XmlElementModel> children = new TreeMap<>();
    for (Field field : type.getDeclaredFields()) {
      int modifiers = field.getModifiers();
      if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers) || field.isSynthetic()
          || field.isAnnotationPresent(XmlTransient.class) || field.isAnnotationPresent(XmlAttribute.class)) {
        continue;
      }
      XmlElement element = field.getAnnotation(XmlElement.class);
      if (hasJaxbAnnotation(field, XmlElement.class) || element != null && element.type() != XmlElement.DEFAULT.class) {
        throw new IllegalStateException(type.getTypeName() + "." + field.getName() + ": unsupported mapping");
      }
      String name = element == null || "##default".equals(element.name()) ? field.getName() : element.name();
      Class<?> elementType = field.getType();
      if (Collection.class.isAssignableFrom(elementType)) {
        if (elementType != List.class) {
          throw new IllegalStateException(type.getTypeName() + "." + field.getName() + ": only List is supported");
        }
        elementType = (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
      }
      if (elementType.isAnnotationPresent(XmlJavaTypeAdapter.class)) {
        throw new IllegalStateException(type.getTypeName() + "." + field.getName() + ": type adapters are unsupported");
      }
      XmlElementModel child = VALUE_TYPES.contains(elementType) || elementType.isEnum()
          ? VALUE : container(elementType, enclosing);
      if (children.put(name, child) != null) {
        throw new IllegalStateException(type.getTypeName() + ": two fields map to element " + name);
      }
    }
    enclosing.remove(type);
    return new XmlElementModel(Collections.unmodifiableMap(children));
  }

  /** Whether the member carries a JAXB annotation other than {@code allowed}. */
  private static boolean hasJaxbAnnotation(AnnotatedElement member, Class<? extends Annotation> allowed) {
    for (Annotation annotation : member.getAnnotations()) {
      Class<? extends Annotation> annotationType = annotation.annotationType();
      if (annotationType != allowed && annotationType.getName().startsWith("javax.xml.bind.annotation.")) {
        return true;
      }
    }
    return false;
  }
}
