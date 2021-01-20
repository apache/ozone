/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.server;

import org.apache.hadoop.hdds.function.FunctionWithServiceException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;

import com.google.protobuf.ServiceException;
import io.opentracing.Span;
import org.slf4j.Logger;

import java.util.function.UnaryOperator;

/**
 * Dispatch message after tracing and message logging for insight.
 * <p>
 * This is a generic utility to dispatch message in ServerSide translators.
 * <p>
 * It logs the message type/content on DEBUG/TRACING log for insight and create
 * a new span based on the tracing information.
 */
public class OzoneProtocolMessageDispatcher<REQUEST, RESPONSE, TYPE> {

  private final String serviceName;

  private final ProtocolMessageMetrics<TYPE>
      protocolMessageMetrics;

  private final Logger logger;
  private final UnaryOperator<REQUEST> requestPreprocessor;
  private final UnaryOperator<RESPONSE> responsePreprocessor;

  public OzoneProtocolMessageDispatcher(String serviceName,
      ProtocolMessageMetrics<TYPE> protocolMessageMetrics,
      Logger logger) {
    this(serviceName, protocolMessageMetrics, logger, req -> req, resp -> resp);
  }

  public OzoneProtocolMessageDispatcher(String serviceName,
      ProtocolMessageMetrics<TYPE> protocolMessageMetrics,
      Logger logger,
      UnaryOperator<REQUEST> requestPreprocessor,
      UnaryOperator<RESPONSE> responsePreprocessor) {
    this.serviceName = serviceName;
    this.protocolMessageMetrics = protocolMessageMetrics;
    this.logger = logger;
    this.requestPreprocessor = requestPreprocessor;
    this.responsePreprocessor = responsePreprocessor;
  }

  public RESPONSE processRequest(
      REQUEST request,
      FunctionWithServiceException<REQUEST, RESPONSE> methodCall,
      TYPE type,
      String traceId) throws ServiceException {
    Span span = TracingUtil.importAndCreateSpan(type.toString(), traceId);
    try {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "[service={}] [type={}] request is received: <json>{}</json>",
            serviceName,
            type,
            escapeNewLines(requestPreprocessor.apply(request)));
      } else if (logger.isDebugEnabled()) {
        logger.debug("{} {} request is received",
            serviceName, type);
      }

      long startTime = System.currentTimeMillis();

      RESPONSE response = methodCall.apply(request);

      protocolMessageMetrics.increment(type,
          System.currentTimeMillis() - startTime);

      if (logger.isTraceEnabled()) {
        logger.trace(
            "[service={}] [type={}] request is processed. Response: "
                + "<json>{}</json>",
            serviceName,
            type,
            escapeNewLines(responsePreprocessor.apply(response)));
      }
      return response;

    } finally {
      span.finish();
    }
  }

  private static String escapeNewLines(Object input) {
    return input.toString().replaceAll("\n", "\\\\n");
  }
}
