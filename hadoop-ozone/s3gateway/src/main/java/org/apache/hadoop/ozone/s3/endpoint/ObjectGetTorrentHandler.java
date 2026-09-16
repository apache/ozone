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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;

import java.io.IOException;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;

/**
 * Handles GET object {@code ?torrent} ({@code GetObjectTorrent}).
 * <p>
 * This operation is not implemented; previously the request incorrectly fell
 * through to GetObject and returned the raw object body.
 */
class ObjectGetTorrentHandler extends ObjectOperationHandler {

  @Override
  Response handleGetRequest(ObjectRequestContext context, String keyName)
      throws IOException, OS3Exception {
    if (queryParams().get(QueryParams.TORRENT) == null) {
      return null;
    }

    context.setAction(S3GAction.GET_OBJECT_TORRENT);
    throw newError(NOT_IMPLEMENTED, "GetObjectTorrent");
  }
}
