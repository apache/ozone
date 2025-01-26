/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.RequestIdentifier;

/**
 * Builder for ObjectEndpoint in tests.
 */
public class ObjectEndpointBuilder extends
    EndpointBuilder<ObjectEndpoint, ObjectEndpointBuilder> {

  private ObjectEndpoint base;

  public ObjectEndpointBuilder setBase(ObjectEndpoint base) {
    this.base = base;
    return this;
  }

  @Override
  public ObjectEndpoint build() {
    ObjectEndpoint endpoint = base != null ? base : new ObjectEndpoint();

    if (client != null) {
      endpoint.setClient(client);
    }

    if (config == null) {
      config = new OzoneConfiguration();
    }
    endpoint.setOzoneConfiguration(config);

    if (requestId == null) {
      requestId = new RequestIdentifier();
    }
    endpoint.setRequestIdentifier(requestId);

    if (headers != null) {
      endpoint.setHeaders(headers);
    }

    if (context != null) {
      endpoint.setContext(context);
    }

    return endpoint;
  }
}
