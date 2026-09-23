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

package org.apache.hadoop.hdds.server.http;

/**
 * Thrown when the HTTP server cannot be configured as requested, for example an
 * {@code ozone.http.filter.initializers} filter that Ozone cannot honor. Unlike
 * a bind or other runtime start-up failure (which the services treat as
 * non-fatal so the process still comes up without a web UI), a configuration
 * error is deterministic and will never succeed on retry, so the services let
 * it abort start-up instead of silently disabling the HTTP server.
 *
 * <p>Extends {@link IllegalArgumentException} (an unchecked exception) so it
 * propagates out of the {@code IOException}-declared {@link BaseHttpServer}
 * constructor without adding a checked-exception signature, and is a distinct
 * type a service can catch ahead of its generic HTTP start-up {@code catch}
 * block, which treats other start-up failures as non-fatal.
 *
 * <p>Each service honors this differently, so code that adds a generic {@code catch} around a
 * web-server start has to re-establish the contract there or that daemon silently goes back to
 * starting without a web server:
 * <ul>
 *   <li>{@code OzoneManager.startHttpServer()} -- rethrown ahead of the non-fatal catch; shared
 *       by {@code start()} and {@code restart()}.</li>
 *   <li>{@code StorageContainerManager.start()} -- rethrown ahead of the non-fatal catch.</li>
 *   <li>{@code HddsDatanodeService.start()} -- rethrown ahead of the non-fatal catch.</li>
 *   <li>{@code ReconServer.call()} -- found by searching the cause chain, because Guice wraps a
 *       constructor failure in a {@code ProvisionException} that a plain {@code catch} misses.</li>
 *   <li>{@code Gateway.call()} (S3 Gateway) -- constructs its servers outside any {@code try}, so
 *       this propagates with no catch clause needed.</li>
 * </ul>
 */
public class HttpServerConfigurationException extends IllegalArgumentException {

  private static final long serialVersionUID = 1L;

  public HttpServerConfigurationException(String message) {
    super(message);
  }

  public HttpServerConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
