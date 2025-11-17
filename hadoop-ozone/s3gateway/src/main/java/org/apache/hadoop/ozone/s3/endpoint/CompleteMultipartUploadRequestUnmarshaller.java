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
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapOS3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

/**
 * Custom unmarshaller to read CompleteMultipartUploadRequest wo namespace.
 */
@Provider
public class CompleteMultipartUploadRequestUnmarshaller
    extends MessageUnmarshaller<CompleteMultipartUploadRequest> {

  public CompleteMultipartUploadRequestUnmarshaller() {
    super(CompleteMultipartUploadRequest.class);
  }

  @Override
  public CompleteMultipartUploadRequest readFrom(
      Class<CompleteMultipartUploadRequest> aClass, Type type,
      Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> multivaluedMap,
      InputStream inputStream) throws WebApplicationException {
    try {
      if (inputStream.available() == 0) {
        throw wrapOS3Exception(INVALID_REQUEST.withMessage("You must specify at least one part"));
      }
      return super.readFrom(aClass, type, annotations, mediaType, multivaluedMap, inputStream);
    } catch (IOException e) {
      throw wrapOS3Exception(INVALID_REQUEST.withMessage(e.getMessage()));
    }
  }

}
