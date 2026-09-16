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

package org.apache.ozone.lib.wsrs;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JAX-RS <code>ExceptionMapper</code> implementation that maps exceptions.
 */
@InterfaceAudience.Private
public class ExceptionProvider implements ExceptionMapper<Throwable> {
  private static final Logger LOG
      = LoggerFactory.getLogger(ExceptionProvider.class);

  private static final String ENTER = System.getProperty("line.separator");

  protected Response createResponse(Response.Status status,
                                    Throwable throwable) {
    // Mirror hadoop-auth's HttpExceptionUtils JSON error contract so WebHDFS
    // clients can reconstruct the remote exception. hadoop's helper returns a
    // javax.ws.rs Response, so build the jakarta.ws.rs equivalent directly.
    Map<String, Object> error = new LinkedHashMap<>();
    error.put("message", getOneLineMessage(throwable));
    error.put("exception", throwable.getClass().getSimpleName());
    error.put("javaClassName", throwable.getClass().getName());
    Map<String, Object> json = Collections.singletonMap("RemoteException", error);
    return Response.status(status).type(MediaType.APPLICATION_JSON)
        .entity(json).build();
  }

  protected String getOneLineMessage(Throwable throwable) {
    String message = throwable.getMessage();
    if (message != null) {
      int i = message.indexOf(ENTER);
      if (i > -1) {
        message = message.substring(0, i);
      }
    }
    return message;
  }

  protected void log(Response.Status status, Throwable throwable) {
    LOG.debug("{}", throwable.getMessage(), throwable);
  }

  @Override
  public Response toResponse(Throwable throwable) {
    return createResponse(Response.Status.BAD_REQUEST, throwable);
  }

}
