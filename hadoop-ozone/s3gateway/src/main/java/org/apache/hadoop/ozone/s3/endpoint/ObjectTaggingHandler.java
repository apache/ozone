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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/** Handle requests for object tagging. */
class ObjectTaggingHandler extends ObjectOperationHandler {

  @Override
  Response handlePutRequest(ObjectRequestContext context, String keyName, InputStream body) throws IOException {
    if (context.ignore(getAction())) {
      return null;
    }

    try {
      S3Tagging tagging;
      try {
        tagging = new PutTaggingUnmarshaller().readFrom(body);
        tagging.validate();
      } catch (Exception ex) {
        OS3Exception exception = S3ErrorTable.newError(S3ErrorTable.MALFORMED_XML, keyName);
        exception.setErrorMessage(exception.getErrorMessage() + ". " + ex.getMessage());
        throw exception;
      }

      Map<String, String> tags = validateAndGetTagging(
          tagging.getTagSet().getTags(), // Nullity check was done in previous parsing step
          S3Tagging.Tag::getKey,
          S3Tagging.Tag::getValue
      );

      context.getBucket().putObjectTagging(keyName, tags);

      getMetrics().updatePutObjectTaggingSuccessStats(context.getStartNanos());

      return Response.ok().build();
    } catch (Exception e) {
      getMetrics().updatePutObjectTaggingFailureStats(context.getStartNanos());
      throw e;
    }
  }

  private S3GAction getAction() {
    if (queryParams().get(S3Consts.QueryParams.TAGGING) == null) {
      return null;
    }

    switch (getContext().getMethod()) {
    case HttpMethod.DELETE:
      return S3GAction.DELETE_OBJECT_TAGGING;
    case HttpMethod.GET:
      return S3GAction.GET_OBJECT_TAGGING;
    case HttpMethod.PUT:
      return S3GAction.PUT_OBJECT_TAGGING;
    default:
      return null;
    }
  }
}
