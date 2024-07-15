/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.Version;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotations to register a validator. {@link org.apache.ozone.annotations.RegisterValidatorProcessor}
 * enforces other annotation to have the following methods:
 * maxVersion : Returns an enum which implement {@link Version}
 * requestType: Returns an Enum value.
 * processingPhase: Returns {@link RequestProcessingPhase}
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface RegisterValidator {
  String MAX_VERSION_METHOD_NAME = "maxVersion";
  String REQUEST_TYPE_METHOD_NAME = "requestType";
  String PROCESSING_PHASE_METHOD_NAME = "processingPhase";
}
