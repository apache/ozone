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
 * In secure mode, Ozone uses symmetric key algorithm to sign all its issued
 * tokens, such as block or container tokens. These tokens are then verified
 * by datanodes to ensure their authenticity and integrity.
 * <p/>
 *
 * That process requires symmetric {@link javax.crypto.SecretKey} to be
 * generated, managed, and distributed to different Ozone components.
 * For example, the token signer (Ozone Manager and SCM) and the
 * verifier (datanode) need to use the same SecretKey.
 * <p/>
 *
 * This package encloses the logic to manage symmetric secret keys
 * lifecycle. In details, it consists of the following components:
 * <ul>
 *   <li>
 *     The definition of manage secret key which is shared between SCM,
 *     OM and datanodes, see
 *     {@link org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey}.
 *   </li>
 *
 *   <li>
 *     The definition of secret key states, which is designed to get replicated
 *     across all SCM instances, see
 *     {@link org.apache.hadoop.hdds.security.symmetric.SecretKeyState}
 *   </li>
 *
 *   <li>
 *    The definition and implementation of secret key persistent storage, to
 *    help retain SecretKey after restarts, see
 *    {@link org.apache.hadoop.hdds.security.symmetric.SecretKeyStore} and
 *    {@link org.apache.hadoop.hdds.security.symmetric.LocalSecretKeyStore}.
 *   </li>
 *
 *   <li>
 *     The basic logic to manage secret key lifecycle, see
 *     {@link org.apache.hadoop.hdds.security.symmetric.SecretKeyManager}
 *   </li>
 * </ul>
 *
 * <p/>
 * The original overall design can be found at
 * <a href=https://issues.apache.org/jira/browse/HDDS-7733>HDDS-7733</a>.
 */
package org.apache.hadoop.hdds.security.symmetric;
