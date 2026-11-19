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

package org.apache.hadoop.ozone.om.upgrade;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.hadoop.ozone.OzoneManagerVersion;

/**
 * Annotation used to "disallow" an API until the OM has finalized to the
 * associated {@link OzoneManagerVersion}. Helps to keep the method logic
 * and upgrade related cross-cutting concerns separate.
 *
 * <p>This is the {@link OzoneManagerVersion}-keyed counterpart of
 * {@link DisallowedUntilLayoutVersion}, for features added after Zero Downtime Upgrade (ZDU) when
 * {@link OMLayoutFeature} was frozen.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DisallowedUntilOmVersion {
  OzoneManagerVersion value();
}
