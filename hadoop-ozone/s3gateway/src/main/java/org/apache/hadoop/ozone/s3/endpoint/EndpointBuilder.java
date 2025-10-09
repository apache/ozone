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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.s3.RequestIdentifier;

/**
 * Builder pattern for creating endpoint instances with proper configuration.
 */
public class EndpointBuilder {

  /**
   * Creates a new BucketEndpointBuilder instance.
   * @return BucketEndpointBuilder instance
   */
  public static BucketEndpointBuilder newBucketEndpointBuilder() {
    return new BucketEndpointBuilder();
  }

  /**
   * Builder for BucketEndpoint instances.
   */
  public static class BucketEndpointBuilder {
    private OzoneClient client;
    private RequestIdentifier requestIdentifier;
    private OzoneConfiguration ozoneConfiguration;

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
     * Sets the Ozone configuration.
     * @param ozoneConfiguration the Ozone configuration
     * @return this builder instance
     */
    public BucketEndpointBuilder setOzoneConfiguration(OzoneConfiguration ozoneConfiguration) {
      this.ozoneConfiguration = ozoneConfiguration;
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
      
      endpoint.init();
      return endpoint;
    }
  }
}
