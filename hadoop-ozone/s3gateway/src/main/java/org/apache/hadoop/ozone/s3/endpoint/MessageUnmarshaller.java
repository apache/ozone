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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_XML_NAMESPACE;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapOS3Exception;

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
 * Unmarshaller to create instances of type {@code T} from XML,
 * which may or may not have namespace.
 * @param <T> the object type to read from XML
 */
public class MessageUnmarshaller<T> implements MessageBodyReader<T> {

  private final JAXBContext context;
  private final SAXParserFactory saxParserFactory;
  private final Class<T> cls;

  public MessageUnmarshaller(Class<T> cls) {
    this.cls = cls;

    try {
      context = JAXBContext.newInstance(cls);
      saxParserFactory = SAXParserFactory.newInstance();
      saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    } catch (Exception ex) {
      throw new AssertionError("Can not instantiate XML parser for " + cls.getSimpleName(), ex);
    }
  }

  @Override
  public boolean isReadable(Class<?> aClass, Type type,
      Annotation[] annotations, MediaType mediaType) {
    return type.equals(cls);
  }

  @Override
  public T readFrom(
      Class<T> aClass, Type type,
      Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> multivaluedMap,
      InputStream inputStream
  ) throws WebApplicationException {
    try {
      XMLReader xmlReader = saxParserFactory.newSAXParser().getXMLReader();
      UnmarshallerHandler unmarshallerHandler =
          context.createUnmarshaller().getUnmarshallerHandler();
      XmlNamespaceFilter filter =
          new XmlNamespaceFilter(S3_XML_NAMESPACE);
      filter.setContentHandler(unmarshallerHandler);
      filter.setParent(xmlReader);
      filter.parse(new InputSource(inputStream));
      return cls.cast(unmarshallerHandler.getResult());
    } catch (Exception e) {
      throw wrapOS3Exception(INVALID_REQUEST.withMessage(e.getMessage()));
    }
  }

  /** Convenience method for programmatic invocation. */
  public T readFrom(InputStream inputStream) throws WebApplicationException {
    return readFrom(cls, cls, new Annotation[0], MediaType.APPLICATION_XML_TYPE, null, inputStream);
  }

}
