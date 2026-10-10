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

import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_XML_NAMESPACE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayDeque;
import java.util.Deque;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * Custom unmarshaller to read Lifecycle configuration.
 */
public class PutBucketLifecycleConfigurationUnmarshaller
    extends MessageUnmarshaller<S3LifecycleConfiguration> {
  private static final ImmutableMap<String, ImmutableSet<String>> ALLOWED_CHILDREN =
      ImmutableMap.<String, ImmutableSet<String>>builder()
          .put("", ImmutableSet.of("LifecycleConfiguration"))
          .put("LifecycleConfiguration", ImmutableSet.of("Rule"))
          .put("Rule", ImmutableSet.of("ID", "Status", "Prefix", "Expiration", "AbortIncompleteMultipartUpload",
              "Filter", "Transition"))
          .put("Expiration", ImmutableSet.of("Days", "Date"))
          .put("AbortIncompleteMultipartUpload", ImmutableSet.of("DaysAfterInitiation"))
          .put("Filter", ImmutableSet.of("Prefix", "Tag", "And"))
          .put("And", ImmutableSet.of("Prefix", "Tag"))
          .put("Tag", ImmutableSet.of("Key", "Value"))
          .put("Transition", ImmutableSet.of("Days", "Date", "StorageClass"))
          .build();

  public PutBucketLifecycleConfigurationUnmarshaller() {
    super(S3LifecycleConfiguration.class);
  }

  @Override
  protected XmlNamespaceFilter createNamespaceFilter() {
    return new LifecycleNamespaceFilter();
  }

  private static class LifecycleNamespaceFilter extends XmlNamespaceFilter {
    private final Deque<String> parents = new ArrayDeque<>();

    LifecycleNamespaceFilter() {
      super(S3_XML_NAMESPACE);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
      // Match JAXB's name selection when the SAX parser is not namespace-aware.
      String name = localName == null || localName.isEmpty() ? qName : localName;
      String parent = parents.isEmpty() ? "" : parents.peek();
      if (!ALLOWED_CHILDREN.getOrDefault(parent, ImmutableSet.of()).contains(name)) {
        throw new SAXException("Unsupported lifecycle element: " + name);
      }
      parents.push(name);
      super.startElement(uri, localName, qName, attributes);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      super.endElement(uri, localName, qName);
      parents.pop();
    }
  }

}
