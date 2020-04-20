package org.apache.hadoop.ozone.s3.endpoint;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.UnmarshallerHandler;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_XML_NAMESPACE;

/**
 * Custom unmarshaller to read CompleteMultipartUploadRequest wo namespace.
 */

public class CompleteMultipartUploadRequestUnmarshaller
    implements MessageBodyReader<CompleteMultipartUploadRequest> {

  private final JAXBContext context;
  private final XMLReader xmlReader;

  public CompleteMultipartUploadRequestUnmarshaller() {
    try {
      context = JAXBContext.newInstance(CompleteMultipartUploadRequest.class);
      SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
      saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      xmlReader = saxParserFactory.newSAXParser().getXMLReader();
    } catch (Exception ex) {
      throw new AssertionError("Can not instantiate " +
          "CompleteMultipartUploadRequest parser", ex);
    }
  }
  @Override
  public boolean isReadable(Class<?> aClass, Type type,
      Annotation[] annotations, MediaType mediaType) {
    return type.equals(CompleteMultipartUploadRequest.class);
  }

  @Override
  public CompleteMultipartUploadRequest readFrom(
      Class<CompleteMultipartUploadRequest> aClass, Type type,
      Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> multivaluedMap,
      InputStream inputStream) throws IOException, WebApplicationException {
    try {
      UnmarshallerHandler unmarshallerHandler =
          context.createUnmarshaller().getUnmarshallerHandler();
      XmlNamespaceFilter filter =
          new XmlNamespaceFilter(S3_XML_NAMESPACE);
      filter.setContentHandler(unmarshallerHandler);
      filter.setParent(xmlReader);
      filter.parse(new InputSource(inputStream));
      return (CompleteMultipartUploadRequest) unmarshallerHandler.getResult();
    } catch (Exception e) {
      throw new WebApplicationException("Can't parse request body to XML.", e);
    }
  }
}
