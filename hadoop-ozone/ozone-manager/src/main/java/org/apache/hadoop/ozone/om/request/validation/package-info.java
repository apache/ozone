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

/**
 * Request's validation handling.
 *
 * This package holds facilities to add new situation specific behaviour to
 * request handling without cluttering the basic logic of the request handler
 * code.
 *
 * Typical use case scenarios, that we had in mind during the design:
 * - during an upgrade, in the pre-finalized state certain request types are
 *   to be rejected based on provided properties of the request not based on the
 *   request type
 * - a client connects to the server but uses an older version of the protocol
 * - a client connects to the server but uses a newer version of the protocol
 * - a client connects to the server and performs an operation corresponding
 *   to a feature the server hasn't finalized for which these requests might have
 *   to be rejected.
 * - the code can handle certain checks that have to run all the time, but at
 *   first we do not see a general use case that we would pull in immediately.
 * These are the current registered
 * {@link org.apache.hadoop.ozone.om.request.validation.VersionExtractor}s
 * which would be extracted out of the om request and all validators
 * fulfilling the condition would be run.
 *
 * The system uses a reflection based discovery to find annotations that are
 * annotated with the
 * {@link org.apache.hadoop.ozone.request.validation.RegisterValidator}
 * annotation.
 * This annotation is used to register a particular annotation which in turn would be used to specify
 * the request type to which the validation should be applied,
 * and the request processing phase in which we apply the validation and the maxVersion corresponding to which this
 * is supposed to run.
 *
 * One validator can be applied based on multiple trigger conditions, E.g.
 * A validator registered can have both {@link org.apache.hadoop.ozone.om.request.validation.OMClientVersionValidator},
 * {@link org.apache.hadoop.ozone.om.request.validation.OMLayoutVersionValidator}
 *
 * The main reason to avoid validating multiple request types with the same
 * validator, is that these validators have to be simple methods without state
 * any complex validation has to happen in the reql request handling.
 * In these validators we need to ensure that in the given condition the request
 * is rejected with a proper message, or rewritten to the proper format if for
 * example we want to handle an old request with a new server, but we need some
 * additional values set to something default, while in the meantime we want to
 * add meaning to a null value from newer clients.
 *
 * In general, it is a good practice to have the request handling code, and the
 * validations tied together in one class.
 */
package org.apache.hadoop.ozone.om.request.validation;
