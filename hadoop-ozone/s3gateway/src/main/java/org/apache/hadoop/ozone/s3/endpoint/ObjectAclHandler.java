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
import java.io.InputStream;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/** Not implemented yet. */
class ObjectAclHandler extends ObjectOperationHandler {

  @Override
  Response handlePutRequest(ObjectRequestContext context, String keyName, InputStream body) throws IOException {
    if (context.ignore(getAction())) {
      return null;
    }

    try {
      throw newError(NOT_IMPLEMENTED, keyName);
    } catch (Exception e) {
      getMetrics().updatePutObjectAclFailureStats(context.getStartNanos());
      throw e;
    }
  }

  @SuppressWarnings("SwitchStatementWithTooFewBranches")
  S3GAction getAction() {
    if (queryParams().get(S3Consts.QueryParams.ACL) == null) {
      return null;
    }

    switch (getContext().getMethod()) {
    case HttpMethod.PUT:
      return S3GAction.PUT_OBJECT_ACL;
    default:
      return null;
    }
  }
}
