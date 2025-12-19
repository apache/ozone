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

package org.apache.hadoop.ozone.s3;

import java.io.IOException;
import java.io.OutputStream;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.client.io.WrappedOutputStream;

/**
 * Filter used to add tracing span.
 */

@Provider
public class TracingFilter implements ContainerRequestFilter,
    ContainerResponseFilter {

  public static final String TRACING_SPAN_CLOSABLE = "TRACING_SPAN_CLOSABLE";

  @Context
  private ResourceInfo resourceInfo;

  @Override
  public void filter(ContainerRequestContext requestContext) {
    finishAndCloseActiveSpan();

    TracingUtil.TraceCloseable activatedSpan =
        TracingUtil.createActivatedSpan(resourceInfo.getResourceClass().getSimpleName() + "." +
            resourceInfo.getResourceMethod().getName());
    requestContext.setProperty(TRACING_SPAN_CLOSABLE, activatedSpan);
  }

  @Override
  public void filter(ContainerRequestContext requestContext,
      ContainerResponseContext responseContext) {
    final TracingUtil.TraceCloseable spanClosable
        = (TracingUtil.TraceCloseable) requestContext.getProperty(TRACING_SPAN_CLOSABLE);
    // HDDS-7064: Operation performed while writing StreamingOutput response
    // should only be closed once the StreamingOutput callback has completely
    // written the data to the destination
    OutputStream out = responseContext.getEntityStream();
    if (out != null) {
      responseContext.setEntityStream(new WrappedOutputStream(out) {
        @Override
        public void close() throws IOException {
          super.close();
          finishAndClose(spanClosable);
        }
      });
    } else {
      finishAndClose(spanClosable);
    }
  }

  private static void finishAndClose(TracingUtil.TraceCloseable spanClosable) {
    try {
      spanClosable.close();
    } catch (Exception e) {
      // Do nothing
    }
    finishAndCloseActiveSpan();
  }

  private static void finishAndCloseActiveSpan() {
    TracingUtil.getActiveSpan().end();
  }
}
