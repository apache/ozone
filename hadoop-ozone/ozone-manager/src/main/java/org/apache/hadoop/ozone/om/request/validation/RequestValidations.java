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

package org.apache.hadoop.ozone.om.request.validation;

import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.request.validation.RequestProcessingPhase.PRE_PROCESS;

import com.google.protobuf.ServiceException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class to configure and set up and access the request/response
 * validation framework.
 */
public class RequestValidations {

  static final Logger LOG = LoggerFactory.getLogger(RequestValidations.class);
  private static final String DEFAULT_PACKAGE = "org.apache.hadoop.ozone";

  private String validationsPackageName = DEFAULT_PACKAGE;
  private ValidationContext context = null;
  private ValidatorRegistry registry = null;

  public synchronized RequestValidations fromPackage(String packageName) {
    validationsPackageName = packageName;
    return this;
  }

  public RequestValidations withinContext(ValidationContext validationContext) {
    this.context = validationContext;
    return this;
  }

  public synchronized RequestValidations load() {
    registry = new ValidatorRegistry(validationsPackageName);
    return this;
  }

  public OMRequest validateRequest(OMRequest request)
      throws Exception {
    List<Method> validations = registry.validationsFor(
        conditions(request), request.getCmdType(), PRE_PROCESS);

    OMRequest validatedRequest = request;
    try {
      for (Method m : validations) {
        LOG.debug("Running the {} request pre-process validation from {}.{}",
            m.getName(), m.getDeclaringClass().getPackage().getName(),
            m.getDeclaringClass().getSimpleName());
        validatedRequest =
            (OMRequest) m.invoke(null, validatedRequest, context);
      }
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof OMException) {
        throw (OMException) e.getCause();
      }
      throw new ServiceException(e);
    } catch (IllegalAccessException e) {
      throw new ServiceException(e);
    }
    return validatedRequest;
  }

  public OMResponse validateResponse(OMRequest request, OMResponse response)
      throws ServiceException {
    List<Method> validations = registry.validationsFor(
        conditions(request), request.getCmdType(), POST_PROCESS);

    OMResponse validatedResponse = response;
    try {
      for (Method m : validations) {
        LOG.debug("Running the {} request post-process validation from {}.{}",
            m.getName(), m.getDeclaringClass().getPackage().getName(),
            m.getDeclaringClass().getSimpleName());
        validatedResponse =
            (OMResponse) m.invoke(null, request, validatedResponse, context);
      }
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new ServiceException(e);
    }
    return validatedResponse;
  }

  private List<ValidationCondition> conditions(OMRequest request) {
    return Arrays.stream(ValidationCondition.values())
        .filter(c -> c.shouldApply(request, context))
        .collect(Collectors.toList());
  }
}
