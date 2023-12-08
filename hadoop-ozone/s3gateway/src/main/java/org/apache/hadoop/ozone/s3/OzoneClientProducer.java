/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import javax.annotation.PreDestroy;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates the OzoneClient for the Rest endpoints.
 */
@RequestScoped
public class OzoneClientProducer {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientProducer.class);

  private OzoneClient client;

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Context
  private ContainerRequestContext context;

  @Produces
  public synchronized OzoneClient createClient() throws WebApplicationException,
      IOException {
    ozoneConfiguration.set("ozone.om.group.rights", "NONE");
    client = getClient(ozoneConfiguration);
    return client;
  }

  @PreDestroy
  public void destroy() throws IOException {
    client.getObjectStore().getClientProxy().clearThreadLocalS3Auth();
  }

  private OzoneClient getClient(OzoneConfiguration config)
      throws IOException {
    OzoneClient ozoneClient = null;
    try {
      ozoneClient =
          OzoneClientCache.getOzoneClientInstance(ozoneConfiguration);
    } catch (Exception e) {
      // For any other critical errors during object creation throw Internal
      // error.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error during Client Creation: ", e);
      }
      throw e;
    }
    return ozoneClient;
  }

  public synchronized void setOzoneConfiguration(OzoneConfiguration config) {
    this.ozoneConfiguration = config;
  }
}
