---
title: "Securing HTTP"
date: "2020-06-17"
summary: Secure HTTP web-consoles for Ozone services 
weight: 4
menu:
   main:
      parent: Security
icon: lock
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

This document describes how to configure Ozone HTTP web-consoles to require user authentication. 

### Default authentication 

By default Ozone HTTP web-consoles (OM, SCM, S3G, Recon, Datanode)
allow access without authentication based on the following default configurations. 

Property| Value
-----------------------------------|-----------------------------------------
ozone.security.http.kerberos.enabled | false
ozone.http.filter.initializers | <empty>

If you have an SPNEGO enabled Ozone cluster and want to disable it for all Ozone services,
just make sure the two key mentioned are configured as above.

### Kerberos based SPNEGO authentication
However, they can be configured to require Kerberos authentication using HTTP SPNEGO protocol (supported
by browsers like Firefox and Chrome). To achieve that, the following keys must
be configured first. 

Property| Value
-----------------------------------|-----------------------------------------
hadoop.security.authentication | kerberos
ozone.security.http.kerberos.enabled | true
ozone.http.filter.initializers | org.apache.hadoop.security.AuthenticationFilterInitializer

After that, individual component needs to configure properly to completely enable
SPNEGO or SIMPLE authentication.

### Filter initializer compatibility

Ozone's HTTP servers run on Jetty 12 (EE10, `jakarta.servlet`). Filters registered
through `ozone.http.filter.initializers` must use `jakarta.servlet.Filter`, or must
belong to one of these bridged `javax.servlet` families:

* `AuthenticationFilter` (hadoop-auth: Kerberos/SPNEGO and simple authentication)
* `StaticUserFilter` (static user for unsecured consoles)
* `CrossOriginFilter` (CORS response headers)
* `RestCsrfPreventionFilter` (CSRF prevention)

Any other `javax.servlet.Filter` — including Hadoop's `XFrameOptionsFilter` and
site-specific wrapper filters — is not bridgeable. Registering one causes the
daemon (OM, SCM, Datanode, S3G, Recon) to **abort start-up**.

**Upgrade note:** before the Jetty 12 migration, a failed HTTP server start was logged
and the daemon continued without a web UI. After it, the daemon refuses to start so
that a misconfigured filter is never silently skipped. If your cluster sets
`ozone.http.filter.initializers` to a custom filter, migrate it to
`jakarta.servlet.Filter` before upgrading.

### Jetty 12 URI compliance

Jetty 12 answers `400 Bad Request` for ambiguous URI constructs that Jetty 9.4 accepted:
empty path segments (e.g., `bucket//key`), ambiguous percent-encodings, encoded path
separators, and suspicious path characters (a decoded backslash, `DEL`, or a C0 control
byte).

Because S3 object keys and WebHDFS paths legitimately contain these sequences, the **S3
Gateway REST endpoint** and **HttpFS** relax exactly those four checks, so they keep serving
the URIs they served before the migration. Every other Ozone HTTP server — OM, SCM, Datanode,
Recon, and the S3 Gateway's web-admin and STS endpoints — uses the Jetty 12 defaults and
answers `400 Bad Request` for such URIs.

Genuinely illegal URI characters that RFC 3986 forbids in unencoded form — such as `[`, `]`,
`{`, `}`, and `|` — are rejected with `400 Bad Request` on every server, including the two
that relax the ambiguity checks. Conforming S3 and WebHDFS clients already percent-encode
these characters; a client that sends them unencoded must be updated before upgrading.

### Enable SPNEGO authentication for OM HTTP
Property| Value
-----------------------------------|-----------------------------------------
ozone.om.http.auth.type | kerberos
ozone.om.http.auth.kerberos.principal | HTTP/_HOST@REALM
ozone.om.http.auth.kerberos.keytab| /path/to/HTTP.keytab

### Enable SPNEGO authentication for S3G HTTP
Property| Value
-----------------------------------|-----------------------------------------
ozone.s3g.http.auth.type | kerberos
ozone.s3g.http.auth.kerberos.principal | HTTP/_HOST@REALM
ozone.s3g.http.auth.kerberos.keytab| /path/to/HTTP.keytab

### Enable SPNEGO authentication for RECON HTTP
Property| Value
-----------------------------------|-----------------------------------------
ozone.recon.http.auth.type | kerberos
ozone.recon.http.auth.kerberos.principal | HTTP/_HOST@REALM
ozone.recon.http.auth.kerberos.keytab| /path/to/HTTP.keytab

### Enable SPNEGO authentication for SCM HTTP
Property| Value
-----------------------------------|-----------------------------------------
hdds.scm.http.auth.type | kerberos
hdds.scm.http.auth.kerberos.principal | HTTP/_HOST@REALM
hdds.scm.http.auth.kerberos.keytab| /path/to/HTTP.keytab

### Enable SPNEGO authentication for DATANODE HTTP
Property| Value
-----------------------------------|-----------------------------------------
hdds.datanode.http.auth.type | kerberos
hdds.datanode.http.auth.kerberos.principal | HTTP/_HOST@REALM
hdds.datanode.http.auth.kerberos.keytab| /path/to/HTTP.keytab

Note: Ozone datanode does not have a default webpage, which prevents you from 
accessing "/" or "/index.html". But it does provide standard 
servlet like jmx/conf/jstack via HTTP. 
 
In addition, Ozone HTTP web-console support the equivalent of Hadoop's Pseudo/Simple authentication. 
If this option is enabled, the user name must be specified in the first browser interaction using the user.name 
query string parameter. e.g., http://scm:9876/?user.name=scmadmin.

### Enable SIMPLE authentication for OM HTTP
Property| Value
-----------------------------------|-----------------------------------------
ozone.om.http.auth.type | simple
ozone.om.http.auth.simple.anonymous.allowed | false

If you don't want to specify the user.name in the query string parameter, 
change ozone.om.http.auth.simple.anonymous.allowed to true.

### Enable SIMPLE authentication for S3G HTTP
Property| Value
-----------------------------------|-----------------------------------------
ozone.s3g.http.auth.type | simple
ozone.s3g.http.auth.simple.anonymous.allowed | false

If you don't want to specify the user.name in the query string parameter, 
change ozone.s3g.http.auth.simple.anonymous.allowed to true.

### Enable SIMPLE authentication for RECON HTTP
Property| Value
-----------------------------------|-----------------------------------------
ozone.recon.http.auth.type | simple
ozone.recon.http.auth.simple.anonymous.allowed | false

If you don't want to specify the user.name in the query string parameter, 
change ozone.recon.http.auth.simple.anonymous.allowed to true.


### Enable SIMPLE authentication for SCM HTTP
Property| Value
-----------------------------------|-----------------------------------------
hdds.scm.http.auth.type | simple
hdds.scm.http.auth.simple.anonymous.allowed | false

If you don't want to specify the user.name in the query string parameter, 
change hdds.scm.http.auth.simple.anonymous.allowed to true.

### Enable SIMPLE authentication for DATANODE HTTP
Property| Value
-----------------------------------|-----------------------------------------
hdds.datanode.http.auth.type | simple
hdds.datanode.http.auth.simple.anonymous.allowed | false

If you don't want to specify the user.name in the query string parameter, 
change hdds.datanode.http.auth.simple.anonymous.allowed to true.
