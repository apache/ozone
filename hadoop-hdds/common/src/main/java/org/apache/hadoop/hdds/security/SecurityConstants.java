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

package org.apache.hadoop.hdds.security;

/**
 * Class to define constants that are used in relation to different PKIX, PKCS, and CMS Structures as defined by
 * <a href="https://datatracker.ietf.org/doc/html/rfc7468">RFC-7468</a>.
 */
public final class SecurityConstants {
  private static final String PEM_PRE_ENCAPSULATION_BOUNDARY_FORMAT = "-----BEGIN %s-----";

  public static final String PEM_POST_ENCAPSULATION_BOUNDARY_FORMAT = "-----END %s-----";

  public static final String PEM_ENCAPSULATION_BOUNDARY_LABEL_PUBLIC_KEY = "PUBLIC KEY";

  public static final String PEM_ENCAPSULATION_BOUNDARY_LABEL_PRIVATE_KEY = "PRIVATE KEY";

  public static final String PEM_PRE_ENCAPSULATION_BOUNDARY_PUBLIC_KEY =
      String.format(PEM_PRE_ENCAPSULATION_BOUNDARY_FORMAT, PEM_ENCAPSULATION_BOUNDARY_LABEL_PUBLIC_KEY);

  public static final String PEM_POST_ENCAPSULATION_BOUNDARY_PUBLIC_KEY =
      String.format(PEM_POST_ENCAPSULATION_BOUNDARY_FORMAT, PEM_ENCAPSULATION_BOUNDARY_LABEL_PUBLIC_KEY);

  public static final String PEM_PRE_ENCAPSULATION_BOUNDARY_PRIVATE_KEY =
      String.format(PEM_PRE_ENCAPSULATION_BOUNDARY_FORMAT, PEM_ENCAPSULATION_BOUNDARY_LABEL_PRIVATE_KEY);

  public static final String PEM_POST_ENCAPSULATION_BOUNDARY_PRIVATE_KEY =
      String.format(PEM_POST_ENCAPSULATION_BOUNDARY_FORMAT, PEM_ENCAPSULATION_BOUNDARY_LABEL_PRIVATE_KEY);

  private SecurityConstants() { }
}
