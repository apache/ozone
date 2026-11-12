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

package org.apache.hadoop.ozone.s3;

import org.eclipse.jetty.ee8.cdi.CdiDecoratingListener;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;

/**
 * Wires Weld into a Jetty 12 servlet context.
 *
 * <p>Weld picks its Jetty integration by probing candidates in order, and
 * {@code JettyContainer} matches only when the {@code org.eclipse.jetty.cdi}
 * context attribute is set. Without it Weld falls through to
 * {@code JettyLegacyContainer}, which still detects on
 * {@code org.eclipse.jetty.util.Decorator} (a class Jetty 12 kept) and then
 * fails in {@code LegacyWeldDecorator} on the removed
 * {@code org.eclipse.jetty.servlet.ServletContextHandler} with a
 * {@code NoClassDefFoundError} -- an {@code Error}, so Weld's
 * {@code catch (Exception)} does not contain it and context startup fails.
 */
final class WeldCdiIntegration {

  private WeldCdiIntegration() {
  }

  /**
   * Install Jetty's CDI decorating listener on the context, which sets the
   * attribute Weld probes for. Must run before the context is started.
   * Only an enabled server has a context, so callers guard on
   * {@code isEnabled()}; the S3 Gateway STS server is disabled by default.
   */
  static void enable(ServletContextHandler context) {
    new CdiDecoratingListener(context);
  }
}
