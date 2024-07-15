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

import com.google.common.collect.Sets;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.Version;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase.PRE_PROCESS;

/**
 * Main class to configure and set up and access the request/response
 * validation framework.
 */
public class RequestValidations {

  static final Logger LOG = LoggerFactory.getLogger(RequestValidations.class);
  private static final String DEFAULT_PACKAGE = "org.apache.hadoop.ozone";
  private static final Set<RequestProcessingPhase> ALLOWED_REQUEST_PROCESSING_PHASES =
      Sets.immutableEnumSet(PRE_PROCESS, POST_PROCESS);
  private String validationsPackageName = DEFAULT_PACKAGE;
  private ValidationContext context = null;
  private ValidatorRegistry<Type> registry = null;



  public synchronized RequestValidations fromPackage(String packageName) {
    validationsPackageName = packageName;
    return this;
  }

  public RequestValidations withinContext(ValidationContext validationContext) {
    this.context = validationContext;
    return this;
  }

  public synchronized RequestValidations load() {
    registry = new ValidatorRegistry<>(Type.class, validationsPackageName,
        Arrays.stream(VersionExtractor.values()).map(VersionExtractor::getVersionClass).collect(Collectors.toSet()),
        ALLOWED_REQUEST_PROCESSING_PHASES);
    return this;
  }

  public OMRequest validateRequest(OMRequest request)
      throws Exception {

    List<Method> validations = registry.validationsFor(request.getCmdType(), PRE_PROCESS,
        this.getVersions(request));
    OMRequest validatedRequest = request.toBuilder().build();
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

    List<Method> validations = registry.validationsFor(request.getCmdType(), POST_PROCESS, this.getVersions(request));

    OMResponse validatedResponse = response.toBuilder().build();
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

  private List<Version> getVersions(OMRequest request) {
    return Arrays.stream(VersionExtractor.values())
        .map(versionExtractor -> versionExtractor.extractVersion(request, context))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
