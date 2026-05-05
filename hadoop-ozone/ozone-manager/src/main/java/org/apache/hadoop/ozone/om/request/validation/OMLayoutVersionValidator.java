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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.request.validation.RegisterValidator;
import org.apache.hadoop.ozone.request.validation.RequestProcessingPhase;

/**
 * An annotation to mark methods that do certain request validations based on the
 * server's layout version and capability to perform a certain operation.
 *
 * The methods annotated with this annotation are collected by the
 * {@link ValidatorRegistry} class during the initialization of the server.
 *
 * The conditions describe the specific use case in which the validator should be
 * applied to the request.
 * See {@link VersionExtractor} for getting all the supported different {@link org.apache.hadoop.ozone.Versioned}
 * component's actual version.
 * The validator method will be applied to the specified request type(s).
 * To help keep these methods simple and straightforward use inheritance and annotate
 * the override method that just calls the method defined in the parent instead of specifying
 * multiple request types.
 * Note that the aim is to have these validators together with the request
 * processing code, so the handling of these specific situations are easy to
 * find.
 *
 * The annotated methods have to have a fixed signature.
 * A {@link RequestProcessingPhase#PRE_PROCESS} phase method is running before
 * the request is processed by the regular code.
 * Its signature has to be the following:
 * - it has to be static and idempotent
 * - it has to have two parameters
 * - the first parameter is an
 * {@link
 * org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest}
 * - the second parameter is a {@link ValidationContext}
 * - the method has to return the modified request, or throw a ServiceException
 *   in case the request is considered to be invalid
 * - the method does not need to care about preserving the request it gets,
 *   the original request is captured and saved by the calling environment.
 *
 * A {@link RequestProcessingPhase#POST_PROCESS} phase method is running once
 * the
 * {@link
 * org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse}
 * is calculated for a given request.
 * Its signature has to be the following:
 * - it has to be static and idempotent
 * - it has three parameters
 * - similalry to the pre-processing validators, first parameter is the
 *   OMRequest, the second parameter is the OMResponse, and the third
 *   parameter is a ValidationContext.
 * - the method has to return the modified OMResponse or throw a
 *   ServiceException if the request is considered invalid based on response.
 * - the method gets the request object that was supplied for the general
 *   request processing code, not the original request, while it gets a copy
 *   of the original response object provided by the general request processing
 *   code.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@RegisterValidator
public @interface OMLayoutVersionValidator {

  /**
   * Defines if the validation has to run before or after the general request
   * processing.
   * @return if this is a pre or post processing validator
   */
  RequestProcessingPhase processingPhase();

  /**
   * The type(s) of the request(s) handled by this validator method.
   * In case you are considering to specify multiple request types, please read the note above on
   * using multiple request types with this kind of validator method.
   * @return the requestType(s) onto which the validator should be applied
   */
  Type[] requestType();

  /**
   * The version before which the validator needs to run. The validator will run only for requests
   * having a version which precedes the specified version.
   * @returns the exclusive upper bound of the request's version under which the validator is applicable.
   */
  OMLayoutFeature applyBefore();

}
