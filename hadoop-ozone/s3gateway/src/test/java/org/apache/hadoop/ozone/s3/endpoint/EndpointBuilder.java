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
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.s3.RequestIdentifier;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;

/**
 * Base builder class for S3 endpoints in tests.
 * @param <T> Type of endpoint being built
 * @param <B> Type of concrete builder (for method chaining)
 */
public abstract class EndpointBuilder<T, B extends EndpointBuilder<T,B>> {
  protected OzoneClient client;
  protected OzoneConfiguration config;
  protected HttpHeaders headers;
  protected ContainerRequestContext context;
  protected RequestIdentifier requestId;

  protected EndpointBuilder() {
    this.config = new OzoneConfiguration();
    this.requestId = new RequestIdentifier();
  }

  @SuppressWarnings("unchecked")
  public B setClient(OzoneClient client) {
    this.client = client;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B setConfig(OzoneConfiguration config) {
    this.config = config;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B setHeaders(HttpHeaders headers) {
    this.headers = headers;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B setContext(ContainerRequestContext context) {
    this.context = context;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B setRequestId(RequestIdentifier requestId) {
    this.requestId = requestId;
    return (B) this;
  }

  public abstract T build();
}
