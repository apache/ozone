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

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.jmx.JMXJsonServlet;

/**
 * JMX servlet whose admin access check runs against Ozone's {@link HttpServer2}.
 *
 * <p>hadoop-common's {@link JMXJsonServlet} delegates the check to
 * {@code org.apache.hadoop.http.HttpServer2}, which cannot be loaded on a Jetty 12
 * classpath: it references {@code org.eclipse.jetty.servlet.ServletContextHandler},
 * removed in Jetty 12, so every {@code /jmx} request would fail with
 * NoClassDefFoundError. Overriding only the check keeps the endpoint working
 * without forking the servlet, and reads the ACL from the attributes Ozone's
 * HttpServer2 actually sets.
 */
public class HddsJMXJsonServlet extends JMXJsonServlet {

  @Override
  protected boolean isInstrumentationAccessAllowed(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    return HttpServer2.isInstrumentationAccessAllowed(getServletContext(),
        request, response);
  }
}
