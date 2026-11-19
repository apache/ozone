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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.inject.Singleton;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

/**
 * Custom unmarshaller to read MultiDeleteRequest w/wo namespace.
 */
@Provider
@Produces(MediaType.APPLICATION_XML)
@Singleton
public class MultiDeleteRequestUnmarshaller
    extends MessageUnmarshaller<MultiDeleteRequest> {

  public MultiDeleteRequestUnmarshaller() {
    super(MultiDeleteRequest.class);
  }

  @Override
  public MultiDeleteRequest readFrom(Class<MultiDeleteRequest> clazz, Type type, Annotation[] annotations,
      MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream inputStream)
      throws WebApplicationException {
    final byte[] body;
    try {
      body = IOUtils.toByteArray(inputStream);
    } catch (IOException ex) {
      throw new WebApplicationException(ex);
    }
    MultiDeleteRequest request = super.readFrom(clazz, type, annotations, mediaType, httpHeaders,
        new ByteArrayInputStream(body));
    request.setBodyContentMD5(DigestUtils.md5Hex(body));
    return request;
  }

}
