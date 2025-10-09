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
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.s3.RequestIdentifier;

/**
 * Builder pattern for creating endpoint instances with proper configuration.
 */
public class EndpointBuilder {

  private EndpointBuilder() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates a new BucketEndpointBuilder instance.
   * @return BucketEndpointBuilder instance
   */
  public static BucketEndpointBuilder newBucketEndpointBuilder() {
    return new BucketEndpointBuilder();
  }

  /**
   * Creates a new RootEndpointBuilder instance.
   * @return RootEndpointBuilder instance
   */
  public static RootEndpointBuilder newRootEndpointBuilder() {
    return new RootEndpointBuilder();
  }

  /**
   * Creates a new ObjectEndpointBuilder instance.
   * @return ObjectEndpointBuilder instance
   */
  public static ObjectEndpointBuilder newObjectEndpointBuilder() {
    return new ObjectEndpointBuilder();
  }

  /**
   * Builder for BucketEndpoint instances.
   */
  public static class BucketEndpointBuilder {
    private OzoneClient client;
    private RequestIdentifier requestIdentifier;
    private OzoneConfiguration ozoneConfiguration;
    private HttpHeaders headers;

    /**
     * Sets the Ozone client.
     * @param client the Ozone client
     * @return this builder instance
     */
    public BucketEndpointBuilder setClient(OzoneClient client) {
      this.client = client;
      return this;
    }

    /**
     * Sets the request identifier.
     * @param requestIdentifier the request identifier
     * @return this builder instance
     */
    public BucketEndpointBuilder setRequestIdentifier(RequestIdentifier requestIdentifier) {
      this.requestIdentifier = requestIdentifier;
      return this;
    }

    /**
     * Sets the request identifier (alias for setRequestIdentifier).
     * @param requestId the request identifier
     * @return this builder instance
     */
    public BucketEndpointBuilder setRequestId(RequestIdentifier requestId) {
      this.requestIdentifier = requestId;
      return this;
    }

    /**
     * Sets the Ozone configuration.
     * @param ozoneConfiguration the Ozone configuration
     * @return this builder instance
     */
    public BucketEndpointBuilder setOzoneConfiguration(OzoneConfiguration ozoneConfiguration) {
      this.ozoneConfiguration = ozoneConfiguration;
      return this;
    }

    /**
     * Sets the Ozone configuration (alias for setOzoneConfiguration).
     * @param config the Ozone configuration
     * @return this builder instance
     */
    public BucketEndpointBuilder setConfig(OzoneConfiguration config) {
      this.ozoneConfiguration = config;
      return this;
    }

    /**
     * Sets the HTTP headers.
     * @param headers the HTTP headers
     * @return this builder instance
     */
    public BucketEndpointBuilder setHeaders(HttpHeaders headers) {
      this.headers = headers;
      return this;
    }

    /**
     * Builds and initializes the BucketEndpoint instance.
     * @return configured BucketEndpoint instance
     * @throws IOException if initialization fails
     */
    public BucketEndpoint build() throws IOException {
      BucketEndpoint endpoint = new BucketEndpoint();
      
      if (client != null) {
        endpoint.setClient(client);
      }
      
      if (requestIdentifier != null) {
        endpoint.setRequestIdentifier(requestIdentifier);
      } else {
        endpoint.setRequestIdentifier(new RequestIdentifier());
      }
      
      if (ozoneConfiguration != null) {
        endpoint.setOzoneConfiguration(ozoneConfiguration);
      } else {
        endpoint.setOzoneConfiguration(new OzoneConfiguration());
      }

      if (headers != null) {
        endpoint.setHeaders(headers);
      }
      
      endpoint.init();
      return endpoint;
    }
  }

  /**
   * Builder for RootEndpoint instances.
   */
  public static class RootEndpointBuilder {
    private OzoneClient client;
    private RequestIdentifier requestIdentifier;

    /**
     * Sets the Ozone client.
     * @param client the Ozone client
     * @return this builder instance
     */
    public RootEndpointBuilder setClient(OzoneClient client) {
      this.client = client;
      return this;
    }

    /**
     * Sets the request identifier.
     * @param requestIdentifier the request identifier
     * @return this builder instance
     */
    public RootEndpointBuilder setRequestIdentifier(RequestIdentifier requestIdentifier) {
      this.requestIdentifier = requestIdentifier;
      return this;
    }

    /**
     * Sets the request identifier (alias for setRequestIdentifier).
     * @param requestId the request identifier
     * @return this builder instance
     */
    public RootEndpointBuilder setRequestId(RequestIdentifier requestId) {
      this.requestIdentifier = requestId;
      return this;
    }

    /**
     * Builds and initializes the RootEndpoint instance.
     * @return configured RootEndpoint instance
     * @throws IOException if initialization fails
     */
    public RootEndpoint build() throws IOException {
      RootEndpoint endpoint = new RootEndpoint();
      
      if (client != null) {
        endpoint.setClient(client);
      }
      
      if (requestIdentifier != null) {
        endpoint.setRequestIdentifier(requestIdentifier);
      } else {
        endpoint.setRequestIdentifier(new RequestIdentifier());
      }
      
      endpoint.init();
      return endpoint;
    }
  }

  /**
   * Builder for ObjectEndpoint instances.
   */
  public static class ObjectEndpointBuilder {
    private OzoneClient client;
    private RequestIdentifier requestIdentifier;
    private OzoneConfiguration ozoneConfiguration;
    private HttpHeaders headers;
    private ContainerRequestContext context;

    /**
     * Sets the Ozone client.
     * @param client the Ozone client
     * @return this builder instance
     */
    public ObjectEndpointBuilder setClient(OzoneClient client) {
      this.client = client;
      return this;
    }

    /**
     * Sets the request identifier.
     * @param requestIdentifier the request identifier
     * @return this builder instance
     */
    public ObjectEndpointBuilder setRequestIdentifier(RequestIdentifier requestIdentifier) {
      this.requestIdentifier = requestIdentifier;
      return this;
    }

    /**
     * Sets the request identifier (alias for setRequestIdentifier).
     * @param requestId the request identifier
     * @return this builder instance
     */
    public ObjectEndpointBuilder setRequestId(RequestIdentifier requestId) {
      this.requestIdentifier = requestId;
      return this;
    }

    /**
     * Sets the Ozone configuration.
     * @param ozoneConfiguration the Ozone configuration
     * @return this builder instance
     */
    public ObjectEndpointBuilder setOzoneConfiguration(OzoneConfiguration ozoneConfiguration) {
      this.ozoneConfiguration = ozoneConfiguration;
      return this;
    }

    /**
     * Sets the Ozone configuration (alias for setOzoneConfiguration).
     * @param config the Ozone configuration
     * @return this builder instance
     */
    public ObjectEndpointBuilder setConfig(OzoneConfiguration config) {
      this.ozoneConfiguration = config;
      return this;
    }

    /**
     * Sets the HTTP headers.
     * @param headers the HTTP headers
     * @return this builder instance
     */
    public ObjectEndpointBuilder setHeaders(HttpHeaders headers) {
      this.headers = headers;
      return this;
    }

    /**
     * Sets the container request context.
     * @param context the container request context
     * @return this builder instance
     */
    public ObjectEndpointBuilder setContext(ContainerRequestContext context) {
      this.context = context;
      return this;
    }

    /**
     * Builds and initializes the ObjectEndpoint instance.
     * @return configured ObjectEndpoint instance
     * @throws IOException if initialization fails
     */
    public ObjectEndpoint build() throws IOException {
      ObjectEndpoint endpoint = new ObjectEndpoint();
      
      if (client != null) {
        endpoint.setClient(client);
      }
      
      if (requestIdentifier != null) {
        endpoint.setRequestIdentifier(requestIdentifier);
      } else {
        endpoint.setRequestIdentifier(new RequestIdentifier());
      }
      
      if (ozoneConfiguration != null) {
        endpoint.setOzoneConfiguration(ozoneConfiguration);
      } else {
        endpoint.setOzoneConfiguration(new OzoneConfiguration());
      }

      if (headers != null) {
        endpoint.setHeaders(headers);
      }

      if (context != null) {
        endpoint.setContext(context);
      }
      
      endpoint.init();
      return endpoint;
    }
  }
}