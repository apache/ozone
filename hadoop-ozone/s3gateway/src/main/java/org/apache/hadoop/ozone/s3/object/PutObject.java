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
package org.apache.hadoop.ozone.s3.object;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.EndpointBase;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File upload.
 */
@Path("/{bucket}/{path:.+}")
public class PutObject extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(PutObject.class);

  @PUT
  @Produces(MediaType.APPLICATION_XML)
  public Response put(
      @Context HttpHeaders headers,
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath,
      @DefaultValue("STAND_ALONE" ) @QueryParam("replicationType")
          ReplicationType replicationType,
      @DefaultValue("ONE") @QueryParam("replicationFactor")
          ReplicationFactor replicationFactor,
      @DefaultValue("32 * 1024 * 1024") @QueryParam("chunkSize")
          String chunkSize,
      @HeaderParam("Content-Length") long length,
      InputStream body) throws IOException {

    try {
      Configuration config = new OzoneConfiguration();
      config.set(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY, chunkSize);

      OzoneBucket bucket = getVolume(getOzoneVolumeName(bucketName))
          .getBucket(bucketName);
      OzoneOutputStream output = bucket
          .createKey(keyPath, length, replicationType, replicationFactor);

      IOUtils.copy(body, output);
      output.close();

      return Response.ok().status(HttpStatus.SC_OK)
          .header("Content-Length", length)
          .build();
    } catch (IOException ex) {
      LOG.error("Exception occurred in PutObject", ex);
      throw ex;
    }
  }
}