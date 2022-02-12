/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.request.validation;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase.PRE_PROCESS;

/**
 * Main class to configure and set up and access the request/response
 * validation framework.
 */
public class RequestValidations {

  private static final String DEFAULT_PACKAGE = "org.apache.hadoop.ozone";

  private String validationsPackageName = DEFAULT_PACKAGE;
  private ValidationContext context = ValidationContext.of(null, -1);
  private ValidatorRegistry registry = ValidatorRegistry.emptyRegistry();

  public RequestValidations fromPackage(String packageName) {
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

  public OMRequest validateRequest(OMRequest request) throws ServiceException {
    List<Method> validations = registry.validationsFor(
        conditions(request), request.getCmdType(), PRE_PROCESS);

    OMRequest validatedRequest = request.toBuilder().build();
    try {
      for (Method m : validations) {
        validatedRequest =
            (OMRequest) m.invoke(null, validatedRequest, context);
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new ServiceException(e);
    }
    return validatedRequest;
  }

  public OMResponse validateResponse(OMRequest request, OMResponse response)
      throws ServiceException {
    List<Method> validations = registry.validationsFor(
        conditions(request), request.getCmdType(), POST_PROCESS);

    OMResponse validatedResponse = response.toBuilder().build();
    try {
      for (Method m : validations) {
        validatedResponse =
            (OMResponse) m.invoke(null, request, response, context);
      }
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new ServiceException(e);
    }
    return validatedResponse;
  }

  private List<ValidationCondition> conditions(OMRequest request) {
    List<ValidationCondition> conditions = new LinkedList<>();
    conditions.add(ValidationCondition.UNCONDITIONAL);
    if (context.serverVersion() != request.getVersion()) {
      if (context.serverVersion() < request.getVersion()) {
        conditions.add(ValidationCondition.NEWER_CLIENT_REQUESTS);
      } else {
        conditions.add(ValidationCondition.OLDER_CLIENT_REQUESTS);
      }
    }
    if (context.versionManager() != null
        && context.versionManager().needsFinalization()) {
      conditions.add(ValidationCondition.CLUSTER_NEEDS_FINALIZATION);
    }
    return conditions;
  }

}
