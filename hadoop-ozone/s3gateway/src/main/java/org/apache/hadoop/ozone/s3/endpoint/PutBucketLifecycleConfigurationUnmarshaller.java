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

import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.UnmarshallerHandler;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * Custom unmarshaller to read Lifecycle configuration namespace.
 */
public class PutBucketLifecycleConfigurationUnmarshaller
    implements MessageBodyReader<S3LifecycleConfiguration>  {

  private final JAXBContext context;
  private final XMLReader xmlReader;

  public PutBucketLifecycleConfigurationUnmarshaller() {
    try {
      context = JAXBContext.newInstance(S3LifecycleConfiguration.class);
      SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
      saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      xmlReader = saxParserFactory.newSAXParser().getXMLReader();
    } catch (Exception ex) {
      throw new AssertionError("Can not instantiate " +
          "PutBucketLifecycleConfiguration parser", ex);
    }
  }

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return type.equals(S3LifecycleConfiguration.class);
  }

  @Override
  public S3LifecycleConfiguration readFrom(Class<S3LifecycleConfiguration> type,
      Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders, InputStream inputStream)
      throws WebApplicationException {
    try {
      UnmarshallerHandler unmarshallerHandler =
          context.createUnmarshaller().getUnmarshallerHandler();
      XmlNamespaceFilter filter = new XmlNamespaceFilter(S3_XML_NAMESPACE);
      filter.setContentHandler(unmarshallerHandler);
      filter.setParent(xmlReader);
      filter.parse(new InputSource(inputStream));
      return (S3LifecycleConfiguration)(unmarshallerHandler.getResult());
    } catch (Exception e) {
      throw new WebApplicationException("Can't parse request body to XML.", e);
    }
  }
}
