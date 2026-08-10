---
title: IPv6 Support
summary: Enable Ozone on dual-stack and IPv6-only networks without changing IPv4 defaults.
date: 2026-08-04
jira: HDDS-15763
status: draft
author: Siyao Meng
---

<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Summary

This proposal enables Apache Ozone services and clients to operate on dual-stack and IPv6-only networks. It preserves
existing IPv4 defaults, makes IPv6 activation explicit, defines one canonical host and port representation, and requires
end-to-end validation of HA, security, data, administration, and observability paths before IPv6 is considered
supported.

# Problem statement

Ozone uses addresses across Hadoop RPC, gRPC, Ratis, HTTP, command-line tools, configuration, and service metadata. Some
paths already accept IPv6, but other paths force the JVM to IPv4, concatenate a host and a port with `:`, or parse an
endpoint with `String.split(":")`. An IPv6 literal contains colons, so these assumptions can produce an ambiguous or
invalid endpoint such as `2001:db8::10:9862`.

Fixing one parser is not sufficient. A cluster can start successfully and still fail later during leader failover,
certificate enrollment, delegation-token use, an administrative command, or metrics collection. Ozone therefore needs a
single address contract and a test matrix that covers complete service paths.

The proposal uses an incremental approach:

1. Do not force the JVM into an IPv4-only networking mode.
2. Preserve current IPv4 bind defaults and let operators opt into IPv6.
3. Store hosts and ports separately in code and use bracket-aware parsing and formatting at text boundaries.
4. Qualify dual-stack and IPv6-only operation independently, including secure and HA deployments.

# Goals

- Run SCM, OM, datanodes, Recon, S3 Gateway, HTTPFS Gateway, OzoneFS clients, and administrative clients on dual-stack
  and IPv6-only networks.
- Preserve existing IPv4 behavior and configuration defaults.
- Support DNS names, IPv4 literals, and IPv6 literals in every documented endpoint setting.
- Make Hadoop RPC, gRPC, Ratis, HTTP/HTTPS, OzoneFS, S3 Gateway, HA failover, and administrative paths IPv6-safe.
- Define secure-mode requirements for Kerberos, delegation tokens, TLS certificates, and endpoint verification.
- Validate replicated and erasure-coded data paths over IPv6.
- Preserve access to Prometheus and JMX metrics over IPv6.
- Provide repeatable CI coverage and operator documentation for IPv6 configuration and limitations.

# Non-goals

- Change rack or network-topology semantics, or add IPv6 CIDR routing logic to SCM.
- Change existing bind defaults from `0.0.0.0` to `::`.
- Prefer raw IP literals over DNS names for Kerberos or TLS identities.
- Support link-local or scoped IPv6 addresses as persistent cluster identities. Scope identifiers are interface-local
  and are not valid X.509 IP subject alternative names.
- Replace every address string with a new Protobuf type in the first delivery.
- Guarantee IPv6 support in applications or network services outside the Apache Ozone project. Ozone will document and
  test the public integration points that it uses.

# Technical description

## Support profiles

Ozone will distinguish the following network profiles:

- **IPv4:** Use existing listener defaults and IPv4 routing. Clients use IPv4 or DNS names that resolve to IPv4.
- **Dual-stack:** Provide IPv6-capable listeners and both address families. Clients use IPv4, IPv6, or DNS names with A
  and/or AAAA records.
- **IPv6-only:** Provide IPv6 listeners and routing with no usable IPv4 fallback. Clients use IPv6 or DNS names that
  resolve to IPv6.

A deployment is not IPv6-only merely because a client prefers IPv6. The qualification environment must remove or block
IPv4 connectivity so that a test cannot silently fall back to IPv4.

## Address representation

Text boundaries must follow these rules:

- **Host-only configuration:** Accept a DNS name, IPv4 literal, or bare IPv6 literal such as `::`. Keep the canonical
  host value free of URI brackets.
- **Host and port configuration:** Accept `host:port`, `ipv4:port`, or `[ipv6]:port`. Enclose IPv6 literals in brackets
  in the canonical output.
- **URI authority:** Accept and emit an RFC-compliant authority, including `[ipv6]:port`.
- **HTTP `Host` header:** Accept a DNS name, IPv4 literal, `[ipv6]`, or `[ipv6]:port`. Remove brackets before subsequent
  matching.
- **In-memory endpoint:** Keep the host and port as separate values. Combine them only when crossing a text boundary.

An unbracketed IPv6 literal followed by a port is ambiguous and will not be an accepted endpoint form. Code must not use
`String.split(":")`, `lastIndexOf(':')`, or string concatenation to parse or construct a network authority.

Shared helpers will parse and format endpoints. Existing Ozone code may use Guava `HostAndPort`, `URI`,
`InetSocketAddress`, or a verified Hadoop/Ozone helper as appropriate for the boundary. The canonical internal host
value does not include brackets. Formatting adds brackets only when a literal is combined with a port or placed in a URI
authority.

## JVM and listener behavior

Ozone no longer sets `java.net.preferIPv4Stack=true` by default. Operators and tests can still set it explicitly when
IPv4-only behavior is required.

Existing listener defaults remain unchanged. An operator opts into an IPv6 listener by setting the applicable bind-host
property to an IPv6 address or to `::`. Code must construct listeners from separate host and port values rather than
first constructing an authority string.

Binding to `::` does not provide the same dual-stack behavior on every operating system. The result depends on the
operating system and its `IPV6_V6ONLY` behavior. The deployment guide will describe this dependency, and the test
environment will verify the address families actually accepted by each listener. Ozone will not treat an IPv6 bind as
proof of IPv4 reachability.

Bind addresses and advertised addresses are different concepts. Wildcard addresses such as `0.0.0.0` and `::` are
suitable listener values but must not be advertised as peer or client endpoints, Kerberos principals, or certificate
identities. Services will continue to advertise a routable DNS name or address.

## Service workstreams

### Common address utilities

The common layer will provide and use bracket-aware parsing and formatting for host-only values, endpoints, and URI
authorities. Production endpoint code will be audited for ad hoc colon parsing and host/port concatenation. Non-network
colon-delimited formats will be left unchanged and documented where necessary.

### SCM and Ratis

SCM peer construction, Ratis role reporting, leader matching, safe-mode checks, Recon consumers, and SCM administration
must accept IPv6 peers. Ratis and gRPC targets will use bracketed IPv6 authorities.

The existing SCM Ratis role response is a colon-delimited string. The first delivery keeps the existing field and
brackets IPv6 address fields, with one shared parser used by all Ozone consumers. IPv4 and DNS output remains unchanged.
Before IPv6 is activated, every consumer of this response must use the new parser.

A later cleanup may add a structured, additive Protobuf field. Such a change would dual-populate the legacy and
structured fields, make new clients prefer the structured field, and retain legacy fallback for the compatibility
window. It is not required for the initial IPv6 delivery.

### OM and OzoneFS

OM RPC addresses, HA failover, service discovery, and OzoneFS URI parsing must support bracketed IPv6 authorities. The
`ofs` form can use a bracketed literal, for example `ofs://[2001:db8::10]:9862/`.

The `o3fs` authority also contains bucket and volume components. A raw IPv6 literal cannot be safely embedded in that
dotted authority form. Deployments that use `o3fs` over IPv6 will identify OM with a service ID or DNS name.

### Datanodes and data protocols

Datanode registration, block locations, xceiver gRPC, Ratis pipelines, container reports, and client retry paths must
preserve IPv6 addresses without loss or ambiguous serialization. Replicated and erasure-coded paths will reuse the
common endpoint rules; they will not introduce protocol-specific IPv6 parsers.

### S3 Gateway and HTTP services

Jetty-based HTTP and HTTPS services must bind and advertise IPv6-safe endpoints. S3 Gateway will parse a bracketed
literal in the HTTP `Host` header without treating IPv6 colons as a port separator.

Path-style S3 requests can target a literal IPv6 endpoint. Virtual-host-style requests should use DNS names with AAAA
records because a literal address does not provide a bucket-prefixed DNS name or a practical wildcard-certificate
identity. AWS Signature Version 4, proxy forwarding, and HTTPS endpoint verification must be exercised in both styles
that apply.

### Administration and observability

Administrative commands must parse addresses returned by SCM, OM, datanodes, and Recon with the same endpoint rules as
service clients. Human-readable output must bracket IPv6 literals when it includes a port.

Prometheus endpoints, JMX access, health checks, and `ozone insight` must remain reachable over IPv6. Tests will verify
both endpoint reachability and the expected metric change after an operation; a healthy TCP connection alone does not
prove that metrics are collected from the correct service.

## Security

IPv6 support must work with `ozone.security.enabled=true`. A result obtained only in non-secure mode does not qualify
secure IPv6 support.

### Kerberos and delegation tokens

Kerberos service principals should use stable DNS names with consistent forward and reverse DNS records. `_HOST`
substitution must resolve to the same fully qualified name that the service advertises. A wildcard bind address or a raw
IPv6 literal is not a stable principal identity.

Hadoop delegation-token service identifiers can be derived from the endpoint address. The Hadoop version used by Ozone
must round-trip DNS, IPv4, and IPv6 token services without ambiguous colon parsing. Both settings of
`hadoop.security.token.service.use_ip` will be tested. If IP-based IPv6 token services are not safe in the selected
Hadoop version, IPv6 support will require either a Hadoop fix or hostname-based token services; the failure will not be
hidden by Ozone-specific parsing.

### TLS and certificate enrollment

Ozone's automatic certificate enrollment must distinguish listener addresses from certificate identities. Automatic
subject alternative name discovery will omit unspecified (any-local), loopback, and scoped addresses. An explicitly
advertised, unscoped IPv6 address can be encoded as an IP subject alternative name. DNS names with AAAA records remain
the preferred identity.

Tests must use certificates with real DNS or IPv6 IP subject alternative names and normal endpoint verification.
Authority overrides such as `localhost` can test transport setup, but they do not qualify IPv6 identity handling. The
secure matrix will cover relevant Hadoop RPC, gRPC, Ratis, HTTP, and HTTPS channels, including certificate renewal and
trust reload.

## Compatibility and upgrade

The initial delivery does not require a metadata layout or Protobuf schema change. Existing IPv4 and DNS configuration
remains valid, and default bind addresses remain unchanged.

IPv6-specific endpoint text is new input. Older components may not understand bracketed literals or may select IPv6
after a DNS AAAA record is added. During a rolling upgrade, operators must continue to use an address family understood
by all running components and clients. IPv6-only addresses and IPv6 activation must be introduced only after all Ozone
services, command-line clients, and relevant filesystem clients have been upgraded.

For settings that combine a host and port, the accepted IPv6 literal form is `[ipv6]:port`. Host-only properties use a
bare literal. Ozone will reject ambiguous values instead of guessing where an IPv6 address ends and a port begins.

## Dependency requirements

Ozone depends on Apache Hadoop and Apache Ratis for address handling in several protocols and tools. Qualification tests
will run against the exact dependency versions selected by the Ozone build.

- Hadoop `NetUtils`, RPC, and delegation-token helpers must parse and format bracketed IPv6 endpoints consistently.
- The Ratis version must contain IPv6-safe peer parsing, including the Ratis shell path tracked by RATIS-2592, or Ozone
  must avoid the affected path.
- The supported JDK must provide the expected resolver, socket, and TLS behavior in each network profile.

A dependency failure is not considered fixed merely because an Ozone wrapper accepts the same input. The complete
producer-to-consumer path must round-trip the endpoint.

## Validation and exit criteria

### Unit and component tests

Tests will cover DNS names, IPv4, `[::1]:port`, global IPv6 literals, `::`, malformed authorities, and host-only values.
They will exercise the production code path selected by each input, not only a standalone parser.

Required coverage includes:

- common host and port helpers;
- listener construction and advertised-address selection;
- SCM Ratis roles, leader suggestions, and failover;
- OzoneFS and S3 Gateway authority parsing;
- administrative and Recon clients;
- certificate subject alternative name selection and endpoint verification;
- delegation-token service construction and decoding; and
- metrics URL construction and collection.

### End-to-end matrix

- **IPv4 regression:** Existing service, client, HA, and secure tests remain green with current defaults.
- **Dual-stack:** Every enabled service listens as configured. Clients complete operations over IPv4 and IPv6 where the
  host operating system supports dual-stack sockets.
- **IPv6-only:** IPv4 routing is unavailable. SCM, OM, datanodes, Recon, S3 Gateway, OzoneFS, HTTPFS Gateway, and
  administrative clients complete their normal workflows over IPv6.
- **Secure IPv6-only:** Kerberos login and re-login, Hadoop RPC protection, delegation-token lifecycle, TLS identity
  checks, certificate renewal, and authenticated HTTP endpoints pass without an IPv4 fallback.
- **HA and failure handling:** SCM and OM leader changes, suggested-leader handling, client failover, Ratis role
  reporting, and Recon access preserve valid IPv6 endpoints.
- **Erasure coding:** An EC bucket completes write, read, degraded read, and reconstruction with IPv6-reachable
  datanodes and the expected topology placement.

The erasure-coding reconstruction test requires enough datanodes for the chosen policy plus a distinct reconstruction
target. For Reed-Solomon 3-2 (RS-3-2), the minimum topology is six datanodes: five participants and one spare target.

An IPv6-enabled CI job will run the checked-in dual-stack and IPv6-only scenarios. A scenario that exits because the
runner has no IPv6 support is a skip, not a pass, and cannot satisfy the IPv6 exit criteria.

Test results will record the Ozone commit, Hadoop and Ratis versions, JDK, operating system, container or Kubernetes
network configuration, DNS records, and whether IPv4 was available. This information is needed to reproduce
address-family selection and listener behavior.

# Alternatives

## Change all bind defaults to `::`

This would make IPv6 available by default, but it could break IPv4-only hosts and would make behavior depend on the
operating system's dual-stack socket policy. The proposal preserves `0.0.0.0` defaults and makes IPv6 listener selection
explicit.

## Require DNS names and reject IPv6 literals

DNS names are preferred for Kerberos and TLS, but operators also need literals for listener configuration, diagnostics,
small deployments, and environments without complete DNS. The proposal supports both and documents where DNS is required
by an identity or protocol form.

## Fix each call site independently

Local fixes can make one service start while leaving failover, tooling, or security paths broken. A shared endpoint
contract and a production call-site inventory reduce inconsistent parsing and make regression coverage reusable.

## Replace address strings with structured Protobuf messages immediately

Structured messages remove delimiter ambiguity but expand the initial delivery into an RPC compatibility project. The
selected approach fixes current text boundaries without changing the wire schema. A future structured field must be
additive and retain a compatibility fallback.

## Rely on JVM address parsing alone

The JVM can represent IPv6 sockets, but Ozone also serializes addresses in configuration, URIs, HTTP headers, Ratis role
output, token services, and CLI text. Those boundaries still require an explicit representation and cannot be made safe
only by changing socket construction.

# Plan

Implementation is tracked under [HDDS-15763](https://issues.apache.org/jira/browse/HDDS-15763). Work is divided into
independently reviewable changes:

1. Remove the forced IPv4 JVM preference and establish common bracket-aware helpers.
2. Fix Ratis peer construction, SCM Ratis role parsing, leader suggestions, and administrative consumers.
3. Fix OzoneFS, S3 Gateway, OM, datanode, Recon, HTTP, and metrics boundaries.
4. Complete secure-mode handling for certificate identities, Kerberos, and delegation-token services, including any
   required Hadoop or Ratis update.
5. Add required dual-stack and IPv6-only CI scenarios and publish operator documentation.

The umbrella tracks work items HDDS-9894, HDDS-15768, HDDS-15772, HDDS-15773, HDDS-15774, HDDS-15775, HDDS-15776,
HDDS-15777, HDDS-15779, HDDS-15780, and HDDS-15895. Each change will test its changed behavior. The final CI
and documentation work will verify the combined behavior rather than infer support from the individual patches.

# Open questions

- Should Ozone add a structured SCM role response after IPv6 support lands, or retain the bracketed legacy
  representation until another API change requires it?
- Which CI environment can provide the required IPv6-only network, secure services, and enough datanodes for HA and
  erasure-coding reconstruction?
- Should support for scoped IPv6 endpoints be rejected explicitly in all advertised-address settings, or only
  documented as unsupported?
- Which Hadoop version first provides the required IPv6 delegation-token service round-trip behavior for both
  token-service modes?

# References

- [HDDS-15763: Ozone IPv6 support](https://issues.apache.org/jira/browse/HDDS-15763)
- [HDDS-9894: IPv6 address checks for certificate requests](https://issues.apache.org/jira/browse/HDDS-9894)
- [RATIS-2592: IPv6-safe Ratis peer-address parsing](https://issues.apache.org/jira/browse/RATIS-2592)
- [RFC 3986: Uniform Resource Identifier](https://www.rfc-editor.org/rfc/rfc3986)
- [RFC 4291: IPv6 Addressing Architecture](https://www.rfc-editor.org/rfc/rfc4291)
- [RFC 9844: Entering IPv6 Zone Identifiers in User Interfaces](https://www.rfc-editor.org/rfc/rfc9844)
