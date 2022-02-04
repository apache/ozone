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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation to mark methods that does certain request validations.
 *
 * The methods annotated with this annotation are collected by the
 * {@link ValidatorRegistry} class during the initialization of the server.
 *
 * The conditions specify the specific use case in which the validator should be
 * applied to the request. See {@link ValidationCondition} for more details
 * on the specific conditions.
 * The validator method should be applied to just one specific request type
 * to help keep these methods simple and straightforward. If you want to use
 * the same validation for different request types, use inheritance, and
 * annotate the override method that just calls super.
 * Note that the aim is to have these validators together with the request
 * processing code, so the handling of these specific situations are easy to
 * find.
 *
 * The annotated methods have to have a fixed signature.
 * A {@link RequestProcessingPhase#PRE_PROCESS} phase method is running before
 * the request is processed by the regular code.
 * Its signature has to be the following:
 * - it has to be static and idempotent
 * - it has to have one or two parameters
 * - if not contextAware, the only parameter it should have is an
 * {@link
 * org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest}
 * - if contextAware, a second parameter of type {@link ValidationContext} has
 *   to be there in the argument list.
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
 * - it has 2 or 3 parameters
 * - similalry to the pre-processing validators, first parameter is the
 *   OMRequest, the second parameter is the OMResponse, and the third optional
 *   parameter is a ValidationContext if the method marked to be context aware.
 * - the method has to return the modified response of throw a ServiceException
 *   if the request is considered invalid based on the response.
 * - the method gets the request object that was supplied for the general
 *   request processing code, not the original request, while it gets a copy
 *   of the original response object provided by the general request processing
 *   code.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RequestFeatureValidator {

  /**
   * Runtime conditions in which a validator should run.
   * @return a list of conditions when the validator should be applied
   */
  ValidationCondition[] conditions();

  /**
   * Defines if the validation has to run before or after the general request
   * processing.
   * @return if this is a pre or post processing validator
   */
  RequestProcessingPhase processingPhase();

  /**
   * The type of the request handled by this validator method.
   * @return the requestType to whihc the validator shoudl be applied
   */
  Type requestType();

  /**
   * Tells whether the validator requires a {@link ValidationContext} or it
   * does not.
   * @return if the method requires the context.
   */
  boolean contextAware() default false;

}
