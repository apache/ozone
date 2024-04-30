/**
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
package org.apache.hadoop.hdds.conf;

/**
 * The definitive list of configuration tags.  Used as a source to define the
 * value of {@code hadoop.tags.custom} and {@code ozone.tags.system}.
 */
public enum ConfigTag {
  ACL,
  BALANCER,
  CLIENT,
  CONTAINER,
  DATANODE,
  DATASTREAM,
  DEBUG,
  DELETION,
  DEPRECATED,
  FREON,
  HA,
  HDDS,
  KERBEROS,
  MANAGEMENT,
  OM,
  OPERATION,
  OZONE,
  OZONEFS,
  PERFORMANCE,
  PIPELINE,
  RATIS,
  RECON,
  REQUIRED,
  S3GATEWAY,
  SCM,
  SECURITY,
  STORAGE,
  TLS,
  TOKEN,
  UPGRADE,
  X509,
  CRYPTO_COMPLIANCE
}
