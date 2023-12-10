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

package org.apache.hadoop.ozone.s3.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Class that represents BadRequestException.
 */
@Provider
public class BadRequestExceptionMapper implements
    ExceptionMapper<BadRequestException> {

  private static final Logger LOG =
      LoggerFactory.getLogger(BadRequestExceptionMapper.class);
  @Override
  public Response toResponse(BadRequestException exception) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Returning exception. ex: {}", exception.toString());
    }

    return Response.status(Response.Status.BAD_REQUEST)
        .entity(exception.getMessage()).build();
  }
}
