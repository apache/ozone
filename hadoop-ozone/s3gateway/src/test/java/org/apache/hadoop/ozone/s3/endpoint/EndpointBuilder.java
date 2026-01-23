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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;

/**
 * Base builder class for S3 endpoints in tests.
 * @param <T> Type of endpoint being built
 */
public class EndpointBuilder<T extends EndpointBase> {

  private final Supplier<T> constructor;
  private T base;
  private OzoneClient ozoneClient;
  private OzoneConfiguration ozoneConfig;
  private HttpHeaders httpHeaders;
  private ContainerRequestContext requestContext;
  private RequestIdentifier identifier;
  private SignatureInfo signatureInfo;

  protected EndpointBuilder(Supplier<T> constructor) {
    this.constructor = constructor;
    this.ozoneConfig = new OzoneConfiguration();
    this.identifier = new RequestIdentifier();
    this.signatureInfo = mock(SignatureInfo.class);
    when(signatureInfo.isSignPayload()).thenReturn(true);

    requestContext = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
  }

  public EndpointBuilder<T> setBase(T base) {
    this.base = base;
    return this;
  }

  public EndpointBuilder<T> setClient(OzoneClient newClient) {
    this.ozoneClient = newClient;
    return this;
  }

  public EndpointBuilder<T> setConfig(OzoneConfiguration newConfig) {
    this.ozoneConfig = newConfig;
    return this;
  }

  public EndpointBuilder<T> setHeaders(HttpHeaders newHeaders) {
    this.httpHeaders = newHeaders;
    return this;
  }

  public EndpointBuilder<T> setContext(ContainerRequestContext newContext) {
    this.requestContext = newContext;
    return this;
  }

  public EndpointBuilder<T> setRequestId(RequestIdentifier newRequestId) {
    this.identifier = newRequestId;
    return this;
  }

  public EndpointBuilder<T> setSignatureInfo(SignatureInfo newSignatureInfo) {
    this.signatureInfo = newSignatureInfo;
    return this;
  }

  public T build() {
    T endpoint = base != null ? base : constructor.get();

    if (endpoint.getClient() == null) {
      endpoint.setClient(getClient());
    }

    final OzoneConfiguration config = getConfig();
    endpoint.setOzoneConfiguration(config != null ? config : new OzoneConfiguration());

    endpoint.setContext(requestContext);
    endpoint.setHeaders(httpHeaders);
    endpoint.setRequestIdentifier(identifier);
    endpoint.setSignatureInfo(signatureInfo);

    endpoint.initialization();

    return endpoint;
  }

  protected OzoneClient getClient() {
    if (ozoneClient == null) {
      ozoneClient = new OzoneClientStub();
    }
    return ozoneClient;
  }

  protected OzoneConfiguration getConfig() {
    return ozoneConfig;
  }

  protected HttpHeaders getHeaders() {
    return httpHeaders;
  }

  protected ContainerRequestContext getContext() {
    return requestContext;
  }

  protected RequestIdentifier getRequestId() {
    return identifier;
  }

  protected SignatureInfo getSignatureInfo() {
    return signatureInfo;
  }

  public static EndpointBuilder<RootEndpoint> newRootEndpointBuilder() {
    return new EndpointBuilder<>(RootEndpoint::new);
  }

  public static EndpointBuilder<BucketEndpoint> newBucketEndpointBuilder() {
    return new EndpointBuilder<>(BucketEndpoint::new);
  }

  public static EndpointBuilder<BucketAclHandler> newBucketAclHandlerBuilder() {
    return new EndpointBuilder<>(BucketAclHandler::new);
  }

  public static EndpointBuilder<ObjectEndpoint> newObjectEndpointBuilder() {
    return new EndpointBuilder<>(ObjectEndpoint::new);
  }
}
