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
- Use address-family translation or tunneling as a substitute for native IPv6 connectivity, including NAT64/DNS64 and
  IPv4-over-IPv6.
- Prefer raw IP literals over DNS names for Kerberos or TLS identities.
- Link-local and scoped IPv6 addresses are not supported as persistent cluster identities. Advertised endpoint settings
  reject them because scope identifiers are interface-local and cannot be encoded in X.509 IP subject alternative names.
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
- **Scoped IPv6:** Reject an address containing a zone identifier, such as `fe80::1%eth0`, in every advertised endpoint
  setting. Do not strip the identifier and advertise the remaining address.
- **In-memory endpoint:** Keep the host and port as separate values. Combine them only when crossing a text boundary.

An unbracketed IPv6 literal followed by a port is ambiguous and will not be an accepted endpoint form. Code must not use
`String.split(":")`, `lastIndexOf(':')`, or string concatenation to parse or construct a network authority.

Shared helpers will parse and format endpoints. Existing Ozone code may use Guava `HostAndPort`, `URI`,
`InetSocketAddress`, or a verified Hadoop/Ozone helper as appropriate for the boundary. The canonical internal host
value does not include brackets. Formatting adds brackets only when a literal is combined with a port or placed in a URI
authority.

## JVM and listener behavior

The implementation described by this proposal will stop setting `java.net.preferIPv4Stack=true` by default. Operators
and tests will still be able to set it explicitly when IPv4-only behavior is required.

Existing listener defaults will remain unchanged. Operators will opt into an IPv6 listener by setting the applicable
bind-host property to an IPv6 address or to `::`. Code must construct listeners from separate host and port values
rather than first constructing an authority string.

Binding to `::` does not provide the same dual-stack behavior on every operating system. The result depends on the
operating system and its `IPV6_V6ONLY` behavior. The deployment guide will describe this dependency, and the test
environment will verify the address families actually accepted by each listener. Ozone will not treat an IPv6 bind as
proof of IPv4 reachability.

Bind addresses and advertised addresses are different concepts. Wildcard addresses such as `0.0.0.0` and `::` are
suitable listener values but must not be advertised as peer or client endpoints, Kerberos principals, or certificate
identities. Services will continue to advertise a routable DNS name or unscoped address. Configuration validation will
reject scoped advertised addresses before startup, registration, or publication.

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

Ozone will retain this bracketed legacy representation after IPv6 support lands. A structured Protobuf field will be
considered only when a separate API requirement justifies it. Any such field must be additive, dual-populated with the
legacy field, and introduced with client preference and fallback rules for the compatibility window.

Existing SCM and OM HA Ratis groups must keep their group IDs, peer IDs, logs, and storage during migration. Stable DNS
names can keep the peer address text unchanged while operators change DNS records to IPv6. If the group stores IPv4
literals, migration must update the Ratis peer configuration with the same peer IDs and new IPv6 addresses. A change to
`ozone-site.xml` alone is not sufficient because Ratis recovers the peer configuration from its log.

Datanode Ratis pipelines have a different rule. When a datanode address changes, SCM closes the affected pipelines and
creates replacements. The datanode UUID and container data do not change.

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

Hadoop RPC service authorization and proxy-user authorization also evaluate the client address. Exact IPv6 addresses,
DNS names, and wildcard host rules can use Hadoop's existing `MachineList` address matching. IPv6 CIDR rules cannot:
`MachineList` delegates CIDR parsing to the IPv4-only Commons Net `SubnetUtils`. Secure IPv6 qualification must consume
a Hadoop fix for IPv6 CIDR ranges so existing address-based security policies retain equivalent behavior.

### TLS and certificate enrollment

Ozone's automatic certificate enrollment must distinguish listener addresses from certificate identities. Automatic
subject alternative name discovery will omit unspecified (any-local), loopback, and scoped addresses. An explicitly
advertised, unscoped IPv6 address can be encoded as an IP subject alternative name. DNS names with AAAA records remain
the preferred identity.

Tests must use certificates with real DNS or IPv6 IP subject alternative names and normal endpoint verification.
Authority overrides such as `localhost` can test transport setup, but they do not qualify IPv6 identity handling. The
secure matrix will cover relevant Hadoop RPC, gRPC, Ratis, HTTP, and HTTPS channels, including certificate renewal and
trust reload.

### Secure HTTP clients

OM and Recon checkpoint transfer, KMS access, and some administrative HTTP clients use Hadoop's `URLConnectionFactory`
or `SSLFactory`. `URLConnectionFactory` can open a correctly bracketed IPv6 URL, but Hadoop's default
`SSLHostnameVerifier` only reads DNS subject alternative names. It ignores an X.509 IP address subject alternative name
and receives a bracketed host from an IPv6 URL. A certificate containing the correct IPv6 IP subject alternative name
therefore cannot satisfy normal Hadoop HTTPS endpoint verification.

The Hadoop verifier must support IPv4 and IPv6 IP subject alternative names before Ozone qualifies literal HTTPS
endpoints. DNS names with matching DNS subject alternative names remain a temporary configuration path, not a substitute
for that fix.

## Compatibility and upgrade

The initial delivery does not require a metadata layout or Protobuf schema change. Existing IPv4 and DNS configuration
remains valid, and default bind addresses remain unchanged.

Ozone supports non-rolling software upgrades. An existing cluster must first use that procedure to install an
IPv6-capable release while retaining its IPv4 configuration. This requires a maintenance window; this proposal does not
guarantee a zero-downtime software upgrade or address-family migration.

After the upgrade, operators can migrate from IPv4 through dual-stack to IPv6-only. IPv4 endpoints must remain usable
until IPv6 listeners, advertised endpoints, security services, and clients have been validated. IPv4 can be removed only
after all Ozone services, command-line clients, and relevant filesystem clients can use IPv6.

A datanode retains its persisted UUID when its IP address or hostname changes. When it re-registers, SCM updates the
existing datanode and closes stale pipelines so that replacement pipelines can be created.
`hdds.datanode.use.datanode.hostname=true` can simplify the transition by using stable DNS names for datanode
data-transfer and Ratis endpoints, but it is not an identity or IPv6 requirement. Qualification will cover both
settings.

IPv6-specific endpoint text is new input. Older clients may not understand bracketed literals or may select IPv6 after a
DNS AAAA record is added. IPv6 activation must therefore wait until all relevant clients have been upgraded.

For settings that combine a host and port, the accepted IPv6 literal form is `[ipv6]:port`. Host-only properties use a
bare literal. Ozone will reject ambiguous values instead of guessing where an IPv6 address ends and a port begins.

## Dependency requirements

Ozone depends on Apache Hadoop and Apache Ratis for address handling in several protocols and tools. Qualification tests
will run against the exact dependency versions selected by the Ozone build. As of 2026-08-10, that build selects Hadoop
3.4.3. The dependency gates below are based on Hadoop releases through 3.5.0 and current Hadoop trunk.

A dependency failure is not considered fixed merely because an Ozone wrapper accepts the same input. The complete
producer-to-consumer path must round-trip the endpoint. Jira resolution state and code on a development branch are also
insufficient: the required commit must be present in the Hadoop release consumed by Ozone.

### P0: Hadoop release blockers

P0 items block the baseline secure IPv6 exit criteria and must be submitted upstream early enough to enter a Hadoop
release that Ozone can consume.

- **Endpoint parsing and token services:** Hadoop `NetUtils` and `SecurityUtil` must agree on bracketed IPv6 hosts in
  both resolver modes. `SecurityUtil.buildTokenService` must produce a value that `SecurityUtil.getTokenServiceAddr` can
  decode for both settings of `hadoop.security.token.service.use_ip`. Ozone uses these paths for OM HA addresses,
  OzoneFS delegation tokens, and token selection in its Hadoop RPC and SASL stack.
- **Secure HTTP literal verification:** Hadoop `SSLHostnameVerifier` must normalize a bracketed IPv6 URL host and
  validate X.509 IP address subject alternative names. This blocks normal HTTPS verification for literal OM and Recon
  checkpoint URLs and for literal KMS endpoints.

No Hadoop release through 3.5.0, and no commit on current Hadoop trunk, satisfies the endpoint and token-service or
literal HTTPS gates. `SecurityUtil.buildTokenService` still emits `host + ":" + port`, while its decoder treats the
result as an authority. Both `use_ip=true` and an IPv6 literal used with `use_ip=false` fail to round-trip. The
`use_ip=false` path also exposes an incompatibility between the bracketed host returned by `NetUtils` and
`SecurityUtil.QualifiedHostResolver`.

HADOOP-12491 and HADOOP-17542 contain earlier IPv6 changes on nontrunk Hadoop branches, but their implementing commits
are not in the release tags inspected here or in current trunk. Neither issue covers the complete token-service or HTTPS
verification requirement. New Hadoop subtasks under HADOOP-11890 are required for these P0 gates.

### P1: Hadoop feature gates

P1 items block a named Ozone feature or supported configuration. They should proceed in parallel with P0 work because
they still depend on a Hadoop release.

- **Automatic service hostname discovery:** OM, SCM, and datanodes call `HddsUtils.getHostName`, which delegates to
  Hadoop `DNS.getDefaultHost` when no hostname is configured. Hadoop `DNS.reverseDns` still constructs only an IPv4
  `in-addr.arpa` query and can fail while examining an IPv6 interface. Explicit hostnames avoid this path, but automatic
  discovery requires IPv6 `ip6.arpa` support. HADOOP-3619 describes the defect but was resolved without a trunk fix.
- **KMS and transparent data encryption:** Ozone OM and clients use Hadoop `KMSUtil`, `KeyProvider`, and
  `KMSClientProvider`. The KMS provider still separates its authority with `split(":")`, so a literal IPv6 KMS authority
  fails before connection. It also inherits the P0 token-service and HTTPS-verifier defects. DNS with an AAAA record and
  `use_ip=false` is only a temporary configuration path. HADOOP-12491 contains nontrunk candidate work but does not
  provide a released fix.
- **Address-based RPC security rules:** Ozone's Hadoop RPC stack uses Hadoop `ServiceAuthorizationManager`,
  `ProxyUsers`, `SaslPropertiesResolver`, and `MachineList`. Exact IPv6 addresses and DNS names resolve to `InetAddress`
  and can be matched, but IPv6 CIDR entries are unsupported because `MachineList` uses the IPv4-only `SubnetUtils`. This
  also affects the optional `WhitelistBasedResolver` for per-address SASL protection. A new Hadoop issue is needed to
  provide IPv6 CIDR parity for these existing security controls.

### Hadoop uses that require validation but not another known upstream fix

- Ozone carries its own copy of Hadoop RPC. Socket connection and listener code uses `InetSocketAddress`; the known
  release dependency is the P0 `NetUtils` and `SecurityUtil` behavior. No separate production RPC change was identified;
  Hadoop RPC still needs an IPv6 stress test against the selected dependency version.
- OM and Recon use HDFS `URLConnectionFactory` for checkpoint downloads. Its URL connection path accepts a bracketed
  IPv6 URL. Qualification still depends on the P0 HTTPS verifier and on Kerberos SPNEGO using a stable DNS principal.
- Ozone uses Hadoop Metrics2 for source registration. The default file sink has no network-address dependency. Optional
  Ganglia server lists use `NetUtils`, while StatsD and Graphite use separate host and port settings. Network sinks need
  tests after the P0 helper changes, but no additional Hadoop defect is established here.
- SCM uses Hadoop `DNSToSwitchMapping` and `ScriptBasedMapping`. The script implementation passes each address as one
  process argument and does not split it on colons. The operator's topology script must accept IPv6 input; this is a
  configuration and validation requirement rather than a known Hadoop release blocker.
- OzoneFS extends Hadoop `FileSystem` and `DelegateToFileSystem`, not `ChecksumFileSystem`. HADOOP-17845 is therefore
  not an Ozone IPv6 prerequisite. OzoneFS still inherits the P0 delegation-token behavior through its canonical service.

### Ozone-owned boundaries

Direct Ozone uses of `NetUtils.createSocketAddr` and `NetUtils.getHostPortString`, URI construction, advertised service
addresses, failover metadata, and command output remain Ozone work. They can use the common Ozone bracket-aware helper
without waiting for a Hadoop release, provided the complete path does not later re-enter an unsafe Hadoop helper.

Ozone also carries its own `HttpServer2` implementation, so HADOOP-19695 does not make Ozone HTTP listeners IPv6-safe.
Listener construction, published HTTP addresses, Prometheus and JMX URLs, and copied RPC address display are Ozone-owned
changes. The Ratis version must separately contain IPv6-safe peer parsing, including the shell path tracked by
RATIS-2592, or Ozone must avoid that path. The supported JDK must provide the expected resolver, socket, and TLS
behavior in every network profile.

## Validation and exit criteria

### Unit and component tests

Tests will cover DNS names, IPv4, `[::1]:port`, global IPv6 literals, `::`, scoped IPv6 rejection, malformed
authorities, and host-only values. They will exercise the production code path selected by each input, not only a
standalone parser.

Required coverage includes:

- common host and port helpers;
- listener construction and advertised-address selection;
- rejection of scoped addresses in every configured, discovered, and published advertised-endpoint path;
- SCM Ratis roles, leader suggestions, and failover;
- OzoneFS and S3 Gateway authority parsing;
- administrative and Recon clients;
- certificate subject alternative name selection and endpoint verification;
- delegation-token service construction and decoding;
- automatic hostname discovery with IPv6 forward and reverse DNS;
- KMS provider creation, encrypted-key access, and KMS delegation-token lifecycle;
- service, proxy-user, and SASL policy selection with exact IPv6, DNS, and IPv6 CIDR rules;
- HTTPS and SPNEGO checkpoint download by OM and Recon; and
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
- **Existing-cluster migration:** Starting from an IPv4 configuration, complete the supported non-rolling upgrade and
  migrate through dual-stack to IPv6-only. Preserve SCM and OM Ratis IDs, logs, and storage. Test stable DNS and address
  updates that keep the same peer IDs for IPv4 literal peers. Validate datanode UUID retention, pipeline replacement,
  and rollback to IPv4 before the final cutover.
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

1. File the P0 and P1 Hadoop issues, land the P0 changes upstream, and select the first Hadoop release that contains
   them.
2. Remove the forced IPv4 JVM preference and establish common bracket-aware Ozone helpers.
3. Fix Ratis peer construction, SCM Ratis role parsing, leader suggestions, and administrative consumers.
4. Fix OzoneFS, S3 Gateway, OM, datanode, Recon, HTTP, KMS, authorization, and metrics boundaries.
5. Complete secure-mode handling for certificate identities, Kerberos, delegation tokens, HTTPS, and any required Ratis
   update.
6. Add required dual-stack and IPv6-only CI scenarios and publish operator documentation.

The umbrella tracks work items HDDS-9894, HDDS-15768, HDDS-15772, HDDS-15773, HDDS-15774, HDDS-15775, HDDS-15776,
HDDS-15777, HDDS-15779, HDDS-15780, and HDDS-15895. Each change will test its changed behavior. The final CI and
documentation work will verify the combined behavior rather than infer support from the individual patches.

# Open questions

- Given that GitHub-hosted runners currently do not provide native IPv6 connectivity, which self-hosted or dedicated CI
  environment can provide the required IPv6-only network, secure services, and enough datanodes for HA and
  erasure-coding reconstruction?

# References

- [HDDS-15763: Ozone IPv6 support](https://issues.apache.org/jira/browse/HDDS-15763)
- [HDDS-9894: IPv6 address checks for certificate requests](https://issues.apache.org/jira/browse/HDDS-9894)
- [Ozone non-rolling upgrades and downgrades]({{< ref "feature/Nonrolling-Upgrade.md" >}})
- [HADOOP-11890: Hadoop IPv6 support umbrella](https://issues.apache.org/jira/browse/HADOOP-11890)
- [HADOOP-3619: IPv6 reverse DNS failure](https://issues.apache.org/jira/browse/HADOOP-3619)
- [HADOOP-12491: IPv6-unsafe Hadoop Common parsing](https://issues.apache.org/jira/browse/HADOOP-12491)
- [HADOOP-17542: IPv6 parsing in NetUtils](https://issues.apache.org/jira/browse/HADOOP-17542)
- [HADOOP-19695: IPv6 support in Hadoop HttpServer2](https://issues.apache.org/jira/browse/HADOOP-19695)
- [RATIS-2592: IPv6-safe Ratis peer-address parsing](https://issues.apache.org/jira/browse/RATIS-2592)
- [actions/runner-images#668: IPv6 on GitHub-hosted runners](https://github.com/actions/runner-images/issues/668)
- [RFC 3986: Uniform Resource Identifier](https://www.rfc-editor.org/rfc/rfc3986)
- [RFC 4291: IPv6 Addressing Architecture](https://www.rfc-editor.org/rfc/rfc4291)
- [RFC 9844: Entering IPv6 Zone Identifiers in User Interfaces](https://www.rfc-editor.org/rfc/rfc9844)
