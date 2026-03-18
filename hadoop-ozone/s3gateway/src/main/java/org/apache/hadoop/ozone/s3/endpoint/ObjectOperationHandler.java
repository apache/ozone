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
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/** Interface for handling object operations using chain of responsibility pattern. */
abstract class ObjectOperationHandler extends EndpointBase {

  Response handleDeleteRequest(ObjectRequestContext context, String keyName) throws IOException, OS3Exception {
    return null;
  }

  Response handleGetRequest(ObjectRequestContext context, String keyName) throws IOException, OS3Exception {
    return null;
  }

  Response handleHeadRequest(ObjectRequestContext context, String keyName) throws IOException, OS3Exception {
    return null;
  }

  Response handlePutRequest(ObjectRequestContext context, String keyName, InputStream body)
      throws IOException, OS3Exception {
    return null;
  }

  ObjectOperationHandler copyDependenciesFrom(EndpointBase other) {
    other.copyDependenciesTo(this);
    return this;
  }
}
