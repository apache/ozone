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

package org.apache.hadoop.hdds.scm.metadata;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a method will be called via Ratis.
 * If a method is annotated with this annotation type the call made to this
 * method will go via Ratis.
 * <ul><li>
 * If the InvocationMethod is DIRECT, the call will be directly submitted
 * to Ratis Server. The current instance has to be the leader for this call to
 * work.
 * </li><li>
 * If the InvocationMethod is CLIENT, the call will be submitted to
 * Ratis Server via Ratis Client. The current instance need not be the leader.
 * </li></ul>
 */
@Inherited
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Replicate {
  /**
   * For DIRECT, the call will be directly submitted to Ratis Server.
   * <br>
   * For CLIENT, the call will be submitted to Ratis Server via Ratis Client.
   */
  enum InvocationType { DIRECT, CLIENT }

  InvocationType invocationType() default InvocationType.DIRECT;
}
