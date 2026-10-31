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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link XmlElementModel}: for each request body, which elements JAXB reads as one value and which hold child
 * elements. A change to a request class that changes this tree changes which bodies the gateway accepts.
 */
public class TestXmlElementModel {

  @ParameterizedTest
  @MethodSource
  public void requestBodies(Class<?> requestClass, String expectedTree) {
    assertEquals(expectedTree, XmlElementModel.document(requestClass).toString());
  }

  static Stream<Arguments> requestBodies() {
    return Stream.of(
        arguments(MultiDeleteRequest.class, "{Delete{Object{Key,VersionId},Quiet}}"),
        arguments(CompleteMultipartUploadRequest.class, "{CompleteMultipartUpload{Part{ETag,PartNumber}}}"),
        arguments(S3Tagging.class, "{Tagging{TagSet{Tag{Key,Value}}}}"),
        arguments(S3BucketAcl.class, "{AccessControlPolicy{AccessControlList{Grant{Grantee{DisplayName,ID},"
            + "Permission}},Owner{DisplayName,ID}}}"),
        arguments(S3LifecycleConfiguration.class, "{LifecycleConfiguration{Rule{AbortIncompleteMultipartUpload"
            + "{DaysAfterInitiation},Expiration{Date,Days},Filter{And{Prefix,Tag{Key,Value}},Prefix,Tag{Key,Value}},"
            + "ID,Prefix,Status,Transition{Date,Days,StorageClass}}}}"));
  }

  /** Fields are elements the way JAXB binds them with field access; attributes and unbound fields are not. */
  @Test
  public void fieldsAreElementsAsJaxbBindsThem() {
    assertEquals("{Sample{Item{Number},Named,unannotated}}", XmlElementModel.document(Sample.class).toString());
  }

  /** A mapping the walk cannot follow fails, instead of leaving elements the filter does not know about. */
  @ParameterizedTest
  @ValueSource(classes = {Wrapped.class, PropertyAccess.class, Inherited.class, MethodMapped.class,
      DefaultRootName.class, TypedElement.class, BeanWithoutFieldAccess.class, ArrayOfItems.class, Recursive.class,
      DuplicateName.class, SetOfItems.class, AdaptedType.class})
  public void unsupportedMappingFails(Class<?> requestClass) {
    assertThrows(IllegalStateException.class, () -> XmlElementModel.document(requestClass));
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Sample")
  static class Sample {
    private static final String CONSTANT = "c";
    @XmlElement(name = "Named")
    private String named;
    private String unannotated;
    private transient String notBound;
    @XmlTransient
    private String alsoNotBound;
    @XmlAttribute(name = "attr")
    private String attribute;
    @XmlElement(name = "Item")
    private List<Item> items;
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  static class Item {
    @XmlElement(name = "Number")
    private int number;
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Wrapped")
  static class Wrapped {
    @XmlElementWrapper(name = "Items")
    @XmlElement(name = "Item")
    private List<String> items;
  }

  @XmlAccessorType(XmlAccessType.PROPERTY)
  @XmlRootElement(name = "PropertyAccess")
  static class PropertyAccess {
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Inherited")
  static class Inherited extends Item {
  }

  /** JAXB binds an annotated getter even with field access. */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "MethodMapped")
  static class MethodMapped {
    private transient String key;

    @XmlElement(name = "Key")
    public String getKey() {
      return key;
    }
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement
  static class DefaultRootName {
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "TypedElement")
  static class TypedElement {
    @XmlElement(name = "Item", type = Item.class)
    private Object item;
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "BeanWithoutFieldAccess")
  static class BeanWithoutFieldAccess {
    @XmlElement(name = "Bean")
    private PlainBean bean;
  }

  /** JAXB maps a class without annotations as a bean with public-member access. */
  static class PlainBean {
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "ArrayOfItems")
  static class ArrayOfItems {
    @XmlElement(name = "Item")
    private Item[] items;
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Recursive")
  static class Recursive {
    @XmlElement(name = "Child")
    private Recursive child;
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "DuplicateName")
  static class DuplicateName {
    @XmlElement(name = "Value")
    private String first;
    @XmlElement(name = "Value", namespace = "urn:other")
    private Item second;
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "SetOfItems")
  static class SetOfItems {
    @XmlElement(name = "Item")
    private Set<Item> items;
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "AdaptedType")
  static class AdaptedType {
    @XmlElement(name = "Value")
    private Adapted value;
  }

  /** JAXB reads this enum through the adapter on its type, which can give it any XML shape. */
  @XmlJavaTypeAdapter(Adapted.FromString.class)
  enum Adapted {
    ONE;

    static class FromString extends XmlAdapter<String, Adapted> {
      @Override
      public Adapted unmarshal(String value) {
        return ONE;
      }

      @Override
      public String marshal(Adapted value) {
        return value.name();
      }
    }
  }
}
