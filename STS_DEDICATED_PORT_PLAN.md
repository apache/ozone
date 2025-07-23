# Refactoring Plan: Dedicated STS Port

This document outlines the plan to create a new, dedicated HTTP/HTTPS port for the STS (Security Token Service) API within the S3 Gateway. This approach provides clean separation from the existing S3 data and web admin endpoints, resolving application context conflicts and improving security and scalability.

## 1. Define New Port Configuration Keys

New configuration keys will be added to `S3GatewayConfigKeys.java` to define the address, port, and enabled status for the new STS server.

**File:** `hadoop-ozone/s3gateway/src/main/java/org/apache/hadoop/ozone/s3/S3GatewayConfigKeys.java`

```java
// Add these new keys
public static final String OZONE_S3G_STS_HTTP_ENABLED_KEY =
    "ozone.s3g.sts.http.enabled";
public static final String OZONE_S3G_STS_HTTP_BIND_HOST_KEY =
    "ozone.s3g.sts.http-bind-host";
public static final String OZONE_S3G_STS_HTTP_ADDRESS_KEY =
    "ozone.s3g.sts.http-address";
public static final int OZONE_S3G_STS_HTTP_BIND_PORT_DEFAULT = 9880;

public static final String OZONE_S3G_STS_HTTPS_ENABLED_KEY =
    "ozone.s3g.sts.https.enabled";
public static final String OZONE_S3G_STS_HTTPS_BIND_HOST_KEY =
    "ozone.s3g.sts.https-bind-host";
public static final String OZONE_S3G_STS_HTTPS_ADDRESS_KEY =
    "ozone.s3g.sts.https-address";
public static final int OZONE_S3G_STS_HTTPS_BIND_PORT_DEFAULT = 9881;
```

## 2. Create a New STS Web Application Directory

A new directory will be created to house the `web.xml` for the dedicated STS web application.

**New Directory:** `hadoop-ozone/s3gateway/src/main/resources/webapps/s3g-sts`

## 3. Create the STS `web.xml`

A new `web.xml` will be created for the STS service. It will define a single JAX-RS servlet configured by the `s3sts.Application` class. The `url-pattern` is set to `/sts/*` to ensure it only handles STS requests.

**File:** `hadoop-ozone/s3gateway/src/main/resources/webapps/s3g-sts/WEB-INF/web.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<web-app version="3.0" xmlns="http://java.sun.com/xml/ns/javaee"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/web-app_3_0.xsd">
  <servlet>
    <servlet-name>sts-jaxrs</servlet-name>
    <servlet-class>org.glassfish.jersey.servlet.ServletContainer</servlet-class>
    <init-param>
      <param-name>javax.ws.rs.Application</param-name>
      <param-value>org.apache.hadoop.ozone.s3sts.Application</param-value>
    </init-param>
    <load-on-startup>1</load-on-startup>
  </servlet>
  <servlet-mapping>
    <servlet-name>sts-jaxrs</servlet-name>
    <url-pattern>/sts/*</url-pattern>
  </servlet-mapping>
</web-app>
```
## 4. Create the `S3STSConfigKeys` class

A new configuration keys class will be created to hold the STS-specific configuration keys, similarly to how `S3SecretConfigKeys` is structured.

**File:** `hadoop-ozone/s3gateway/src/main/java/org/apache/hadoop/ozone/s3sts/S3STSConfigKeys.java`

```java
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
package org.apache.hadoop.ozone.s3sts;

public class S3STSConfigKeys {
  // Add these new keys
  public static final String OZONE_S3G_STS_HTTP_ENABLED_KEY =
      "ozone.s3g.sts.http.enabled";
  public static final String OZONE_S3G_STS_HTTP_BIND_HOST_KEY =
      "ozone.s3g.sts.http-bind-host";
  public static final String OZONE_S3G_STS_HTTP_ADDRESS_KEY =
      "ozone.s3g.sts.http-address";
  public static final int OZONE_S3G_STS_HTTP_BIND_PORT_DEFAULT = 9880;

  public static final String OZONE_S3G_STS_HTTPS_ENABLED_KEY =
      "ozone.s3g.sts.https.enabled";
  public static final String OZONE_S3G_STS_HTTPS_BIND_HOST_KEY =
      "ozone.s3g.sts.https-bind-host";
  public static final String OZONE_S3G_STS_HTTPS_ADDRESS_KEY =
      "ozone.s3g.sts.https-address";
  public static final int OZONE_S3G_STS_HTTPS_BIND_PORT_DEFAULT = 9881;

  /**
   * Never constructed.
   */
  private S3STSConfigKeys() {

  }
}
```

## 5. Create the `S3GatewayStsServer` Class

A new HTTP server class will be created for the STS service, similar to the existing `S3GatewayHttpServer` and `S3GatewayWebAdminServer`. It includes an authentication filter for Kerberos.

**File:** `hadoop-ozone/s3gateway/src/main/java/org/apache/hadoop/ozone/s3/S3GatewayStsServer.java`

```java
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

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.server.http.BaseHttpServer;
import org.apache.hadoop.hdds.server.http.ServletElementsFactory;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.*;
import static org.apache.hadoop.security.authentication.server.AuthenticationFilter.AUTH_TYPE;

/**
 * HTTP server for the S3 Gateway STS endpoint.
 */
public class S3GatewayStsServer extends BaseHttpServer {

  public S3GatewayStsServer(MutableConfigurationSource conf, String name) throws IOException {
    super(conf, name);
    addSTSAuthentication(conf);
  }

  private void addSTSAuthentication(MutableConfigurationSource conf)
      throws IOException {
    String authType = conf.get(getHttpAuthType());
    if (UserGroupInformation.isSecurityEnabled()
        && authType.equals("kerberos")) {
      ServletHandler handler = getWebAppContext().getServletHandler();
      Map<String, String> params = new HashMap<>();

      String principalInConf = conf.get(getSpnegoPrincipal());
      if (!Strings.isNullOrEmpty(principalInConf)) {
        params.put("kerberos.principal", SecurityUtil.getServerPrincipal(
            principalInConf, conf.get(getHttpBindHostKey())));
      }
      String httpKeytab = conf.get(getKeytabFile());
      if (!Strings.isNullOrEmpty(httpKeytab)) {
        params.put("kerberos.keytab", httpKeytab);
      }
      params.put(AUTH_TYPE, "kerberos");

      FilterHolder holder = ServletElementsFactory.createFilterHolder(
          "stsAuthentication", AuthenticationFilter.class.getName(),
          params);
      FilterMapping filterMapping =
          ServletElementsFactory.createFilterMapping(
              "stsAuthentication",
              new String[]{"/sts/*"});

      handler.addFilter(holder, filterMapping);
    }
  }

  @Override
  protected String getHttpAddressKey() {
    return OZONE_S3G_STS_HTTP_ADDRESS_KEY;
  }

  @Override
  protected String getHttpBindHostKey() {
    return OZONE_S3G_STS_HTTP_BIND_HOST_KEY;
  }

  @Override
  protected String getHttpsAddressKey() {
    return OZONE_S3G_STS_HTTPS_ADDRESS_KEY;
  }

  @Override
  protected String getHttpsBindHostKey() {
    return OZONE_S3G_STS_HTTPS_BIND_HOST_KEY;
  }

  @Override
  protected String getBindHostDefault() {
    return OZONE_S3G_HTTP_BIND_HOST_DEFAULT;
  }

  @Override
  protected int getHttpBindPortDefault() {
    return OZONE_S3G_STS_HTTP_BIND_PORT_DEFAULT;
  }

  @Override
  protected int getHttpsBindPortDefault() {
    return OZONE_S3G_STS_HTTPS_BIND_PORT_DEFAULT;
  }

  @Override
  protected String getKeytabFile() {
    return OZONE_S3G_KEYTAB_FILE;
  }

  @Override
  protected String getSpnegoPrincipal() {
    return OZONE_S3G_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL;
  }

  @Override
  protected String getEnabledKey() {
    return OZONE_S3G_STS_HTTP_ENABLED_KEY;
  }

  @Override
  protected String getHttpAuthType() {
    return OZONE_S3G_HTTP_AUTH_TYPE;
  }

  @Override
  protected String getHttpAuthConfigPrefix() {
    return OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX;
  }
}
```

## 5. Integrate the New Server into `Gateway.java`

The main `Gateway` class will be updated to instantiate and manage the new `S3GatewayStsServer`.

**File:** `hadoop-ozone/s3gateway/src/main/java/org/apache/hadoop/ozone/s3/Gateway.java`

*   **Add a new field:**
    ```java
    private S3GatewayStsServer stsServer;
    ```

*   **Instantiate the server in the `call()` method:**
    ```java
    // Add this block inside the call() method
    this.stsServer = new S3GatewayStsServer(conf, "s3g-sts");
    ```

*   **Start the server in the `start()` method:**
    ```java
    // Add this line inside the start() method
    stsServer.start();
    ```

*   **Stop the server in the `stop()` method:**
    ```java
    // Add this line inside the stop() method
    if (stsServer != null) {
      stsServer.stop();
    }
    ```

*   **Join the server thread in the `join()` method:**
    ```java
    // Add this line inside the join() method
    if (stsServer != null) {
      stsServer.join();
    }
    ```

## 6. Update `ozone-default.xml`

The following properties will be added to `hadoop-hdds/common/src/main/resources/ozone-default.xml` to provide default values for the new STS configuration keys.

**File:** `hadoop-hdds/common/src/main/resources/ozone-default.xml`

```xml
<property>
  <name>ozone.s3g.sts.http.enabled</name>
  <value>true</value>
  <description>
    Whether the S3 Gateway STS HTTP endpoint is enabled.
  </description>
</property>

<property>
  <name>ozone.s3g.sts.http-bind-host</name>
  <value>0.0.0.0</value>
  <description>
    The bind host for the S3 Gateway STS HTTP server.
    If not set, the value of ozone.s3g.http-bind-host is used.
  </description>
</property>

<property>
  <name>ozone.s3g.sts.http-address</name>
  <value>0.0.0.0:9880</value>
  <description>
    The HTTP address for the S3 Gateway STS endpoint.
  </description>
</property>

<property>
  <name>ozone.s3g.sts.https.enabled</name>
  <value>false</value>
  <description>
    Whether the S3 Gateway STS HTTPS endpoint is enabled.
  </description>
</property>

<property>
  <name>ozone.s3g.sts.https-bind-host</name>
  <value>0.0.0.0</value>
  <description>
    The bind host for the S3 Gateway STS HTTPS server.
    If not set, the value of ozone.s3g.https-bind-host is used.
  </description>
</property>

<property>
  <name>ozone.s3g.sts.https-address</name>
  <value>0.0.0.0:9881</value>
  <description>
    The HTTPS address for the S3 Gateway STS endpoint.
  </description>
</property>
```
## 7. Delete The Old STS Endpoint 
The old STS endpoint that was previously part of the S3 Gateway will be removed. This includes other unused classes and configurations related to the old STS implementation. The new dedicated STS server will handle all STS requests independently.

This plan creates a fully independent STS service on a new port, which is the most robust and maintainable solution.
