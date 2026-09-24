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
import java.io.InputStream;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;

/**
 * Rejects object subresource operations that are not implemented.
 * <p>
 * Must be added to the chain after the handlers of specific subresources ({@link ObjectAclHandler},
 * {@link ObjectAttributesHandler}, {@link ObjectGetTorrentHandler}, {@link ObjectTaggingHandler},
 * {@link MultipartKeyHandler}), etc. so that it only sees the HTTP methods those handlers do not handle, e.g. DELETE
 * {@code ?acl} or PUT {@code ?attributes}. Otherwise, the requests fall through to another operation of the same HTTP
 * method: a GET returns the object body, a PUT overwrites the object with the request body and a DELETE deletes it.
 * <p>
 * {@code ?uploads} is only valid for POST ({@code CreateMultipartUpload}), which does not go through this chain.
 */
class ObjectNotImplementedHandler extends ObjectOperationHandler {

  private static final Set<String> SUBRESOURCES = ImmutableSet.of(
      QueryParams.ACL, QueryParams.ANNOTATION, QueryParams.ATTRIBUTES, QueryParams.ENCRYPTION, QueryParams.LEGAL_HOLD,
      QueryParams.RENAME_OBJECT, QueryParams.RETENTION, QueryParams.TORRENT, QueryParams.UPLOADS);

  // DeleteObject with versionId targets a specific version; ignoring it would delete the current object instead.
  private static final Set<String> DELETE_SUBRESOURCES = ImmutableSet.<String>builder()
      .addAll(SUBRESOURCES)
      .add(QueryParams.VERSION_ID)
      .build();

  @Override
  Response handleGetRequest(ObjectRequestContext context, String keyName) {
    return rejectNotImplemented(context, SUBRESOURCES, keyName);
  }

  @Override
  Response handlePutRequest(ObjectRequestContext context, String keyName, InputStream body) {
    return rejectNotImplemented(context, SUBRESOURCES, keyName);
  }

  @Override
  Response handleDeleteRequest(ObjectRequestContext context, String keyName) {
    return rejectNotImplemented(context, DELETE_SUBRESOURCES, keyName);
  }
}
