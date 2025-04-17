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
 * code for any server.
 * {@link org.apache.hadoop.ozone.request.validation.RegisterValidator}
 * is used to register any validator which has the following methods:
 * - applyBefore : Returns an enum which implement {@link org.apache.hadoop.ozone.Versioned}
 * - requestType: Returns an Enum value.
 * - processingPhase: Returns {@link org.apache.hadoop.ozone.request.validation.RequestProcessingPhase}
 *
 * The system uses a reflection based discovery to find methods that are
 * annotated with an annotation annotated with the
 * {@link org.apache.hadoop.ozone.request.validation.RegisterValidator}
 * annotation.
 * The RegisterValidator annotation is used to register a particular annotation which in turn would be used
 * to specify conditions in which a certain validator has to be used:
 * - the request type onto which the validation should be applied,
 * - the request processing phase in which the validation should be applied
 * - and the layout version before which the validation should be applied.
 */

package org.apache.hadoop.ozone.request.validation;
