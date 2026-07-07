# Apache Ozone — Threat Model

## §1 Header

- **Project:** Apache Ozone (`apache/ozone`) — a distributed, scalable object
  store (S3-compatible + Hadoop-FS) built on Hadoop Distributed Data Store
  (HDDS).
- **Written against:** `master` @ HEAD (2026-06).
- **Author:** ASF Security team, via the threat-model-producer rubric (Scovetta
  rubric) at the Ozone PMC's request (path 3, confirmed siyao@ 2026-06-02).
- **Status:** v1 — ratified by the Ozone PMC. Maintainer review complete (Siyao Meng / smengcl and Wei-Chiu Chuang / jojochuang, 2026-06/07); wave-1–3 answers folded.
- **Version binding:** versioned with the project; a report against version *N*
  is triaged against the model as it stood at *N*.
- **Reporting cross-reference:** §8-violating findings go to
  `security@ozone.apache.org` (per [`SECURITY.md`](SECURITY.md)); §3/§9 findings
  are closed citing this document.
- **Provenance legend:** *(documented)* = project source/docs/`SECURITY.md`;
  *(maintainer)* = an Ozone maintainer in this review; *(inferred)* = reasoned
  from code/architecture — each has a §14 open question.
- **Draft confidence:** ~24 documented / 2 maintainer / 24 inferred (smengcl confirmed Q-secure + Q-ratis, 2026-06-23).

**What it is.** Ozone is a multi-daemon distributed object store. The
**Ozone Manager (OM)** owns the namespace/metadata and can issue delegation +
block tokens; the **Storage Container Manager (SCM)** manages blocks/containers
and acts as the cluster's **internal Certificate Authority** (root of service
identity); **Datanodes** store data in containers, replicate via **Ratis
(Raft)**, and enforce block/container tokens when enabled; the **S3 Gateway**
exposes an S3-compatible REST API to (potentially internet-facing) clients; **Recon** is a
read-only management/monitoring service; a **CSI driver** provisions Kubernetes
volumes. Clients reach it via the S3 API or the `ofs://`/`o3fs://` Hadoop
filesystem over Hadoop RPC.

## §2 Scope and intended use

Ozone is deployed as a **cluster of network services**, not an in-process
library. There is no single "caller"; the roles split:

- **Untrusted client** — an S3 REST client or RPC client outside the cluster
  trust boundary (the S3 Gateway may be internet-facing).
- **Authenticated user** — a Kerberos-authenticated principal acting within
  their granted ACLs; trusted to authenticate, **not** trusted to stay within
  authorization (an authenticated user attempting to read another tenant's data
  is in-model).
- **Operator/admin** — trusted for the deployment (KDC, Ranger policies, CA key,
  network).
- **Service peer** — OM/SCM/Datanode talking to each other, and Datanode-to-
  Datanode Ratis peers; authenticated via SCM-issued certificates, but a
  **compromised datanode is a distinct (Byzantine) actor** (§7).

**Component families.**

| Family | Entry point | Exposure | In model? |
| --- | --- | --- | --- |
| S3 Gateway | S3 REST (AWS SigV4) | **untrusted / internet-facing** | **Yes** |
| OM (namespace, tokens, ACLs) | Hadoop RPC (SASL/Kerberos) | authenticated clients | **Yes** |
| SCM (blocks + internal CA) | Hadoop RPC + cert server | services + admin | **Yes** (CA = root of trust) |
| Datanode (block store, Ratis) | block protocol + Ratis | token-gated clients where enabled + DN peers | **Yes** |
| Recon | read-only HTTP/RPC | operators | **Yes** (read path) |
| CSI driver | gRPC (kubelet) | in-cluster | **No — §3** |
| `ofs`/`o3fs` client libs | in-process in the caller's app | as the caller | client-side (§10) |
| test/integration modules | test | n/a | No — §3 |

## §3 Out of scope (explicit non-goals)

- **The security-providing infrastructure Ozone depends on but does not own:**
  the Kerberos KDC, the Ranger policy server + the *correctness of the
  authorization policies an operator writes*, the KMS/key material for
  transparent data encryption, and the network perimeter. Ozone consumes these;
  hardening them is the operator's (§10). *(documented/inferred — SecureOzone,
  SecurityWithRanger, SecuringTDE, NetworkPorts.)*
- **`ozone-thirdparty`** — a shaded-dependency packaging repo; build artifact,
  no runtime attack surface of its own. *(documented.)*
- **Non-secure mode as a target.** With `ozone.security.enabled=false` Ozone
  performs **no authentication** — it is a development/sandbox posture. Findings
  that only manifest in non-secure mode are `OUT-OF-MODEL: non-default-build`
  (§5a). Secure mode is confirmed as the supported production posture.
  *(maintainer — smengcl, 2026-06-23: secure mode is the supported posture; and
  with security enabled the S3 Gateway rejects anonymous access — no plan to
  support intended anonymous access, [HDDS-7961](https://issues.apache.org/jira/browse/HDDS-7961).)* *(maintainer — jojochuang, 2026-06-25: the S3 user doc
  states the secure-mode anonymous rejection only implicitly — making it
  explicit is tracked; note a future S3 web-hosting feature would require
  anonymous access by design, which would be a documented opt-in exception.)*
- **Compromise of the SCM CA root key.** If the SCM CA private key is stolen, all
  service identity collapses by design; protecting it is operational (§10/§7).
- **The CSI driver.** Not production-ready; excluded from security inspection.
  Recon, by contrast, **is** in scope — treat it as part of the production
  cluster. *(maintainer — jojochuang, 2026-06-25.)*
- Test/integration modules (`integration-test-*`, `*TestImpl`).

## §4 Trust boundaries and data flow

Boundaries, outermost first:

1. **S3 Gateway boundary** — untrusted REST clients; AWS SigV4 over per-user S3
   secrets (derived from the user's Kerberos identity / S3 secret store).
2. **RPC/control-plane boundary** — clients to OM/SCM over Hadoop RPC with SASL;
   S3 Gateway may reach OM over the OM gRPC transport, and SCM HA uses internal
   Inter-SCM gRPC for checkpoint transfer. In secure mode, authenticated via
   Kerberos / delegation tokens and, for gRPC, TLS/mTLS where configured.
3. **Block-access boundary** — when block/container tokens are enabled, a client
   obtains a **block token** from OM, presents it to a Datanode, which verifies
   the token's signature before serving the block. The Datanode does not
   re-authenticate the user; the token *is* the capability.
4. **Service-identity boundary** — OM/SCM/DN use **SCM-issued certificates**
   for service identity; SCM is the CA. Transport encryption is controlled by
   separate TLS/RPC privacy knobs (§5a).
5. **Ratis boundary** — Datanode peers replicate via Raft; a peer holds a
   legitimate certificate but may behave arbitrarily if compromised (§7).

```
S3 client ──SigV4──► S3 Gateway ──RPC(Kerberos)──► OM ──(block token)──► client
RPC client ─Kerberos/deleg token─► OM (ACL check) ─► SCM (block alloc) ─► Datanode
                                                  Datanode verifies block token, serves block
services ⇄ services : SCM-issued certs ; Datanodes ⇄ Datanodes : Ratis(Raft)
```

**Reachability precondition (triager's test):** a finding is in-model only if
reachable in **secure mode** (`ozone.security.enabled=true`) from the actor that
owns that boundary — an S3-Gateway finding from an untrusted REST client; an
OM/SCM finding from an authenticated-but-unauthorized user; a block-access
finding from a client without a valid token in a token-enabled deployment; a
consensus finding from a Byzantine Datanode peer below the honest-majority
threshold (§7).

## §5 Assumptions about the environment

- **Secure deployment** assumes a functioning **Kerberos KDC**, time sync,
  DNS/rDNS, and a **Ranger** server if Ranger authz is enabled. Kerberos and
  Ranger integration are documented; time/DNS are deployment preconditions.
- **PKI:** SCM is the root CA; service certs are issued/rotated by SCM. The CA
  key's secrecy is assumed. *(documented — `CertificateServer`/`CertificateClient`.)*
- **Storage:** Datanodes trust their local disks; on-disk encryption (TDE) keys
  come from an external KMS. *(documented — SecuringTDE.)*
- **Network:** Ozone exposes documented OM/SCM/Recon/S3G/Datanode listeners;
  admin/Ratis/datanode service ports are assumed reachable only by the cluster +
  authorized clients (operator-enforced).
- Ozone **does** open many network listeners and spawns service processes by
  design; the "no side effects" inventory does not apply to a server.

## §5a Build-time and configuration variants — **the central knob**

**`ozone.security.enabled` is load-bearing.** With it **true** (secure mode):
Kerberos authentication on RPC, delegation-token support, and SCM-issued
certificates for service identity are in the security posture. Several
authorization, capability, and confidentiality controls are separately
configured: object ACL checks (`ozone.acl.enabled`), block/container tokens
(`hdds.block.token.enabled`, `hdds.container.token.enabled`), transport
encryption, and TDE/KMS. With it **false** (non-secure mode): **no
authentication at all** — intended only for dev/sandbox.

**The insecure-default problem (wave-1 — answered).** Secure mode **is** the
supported production posture *(maintainer — smengcl, 2026-06-23)*: operators must
enable it for any untrusted exposure, so non-secure-mode findings are
`OUT-OF-MODEL: non-default-build` and §10 carries "run secure mode." For the S3
Gateway specifically: with security enabled, **anonymous access is rejected** and
there is no plan to support intended anonymous access
([HDDS-7961](https://issues.apache.org/jira/browse/HDDS-7961)) — so an
"unauthenticated S3 request is accepted" finding in secure mode is `VALID`, not a
disclaimed mode. Other knobs that move the envelope remain deployment choices:
object ACL authorizer, S3 secret storage backend, block/container-token
enablement, transport encryption, and TDE/KMS.

**Default authz/capability/crypto state — off by default even in secure mode**
*(maintainer — jojochuang, 2026-06-25)*. Several controls are disabled in a
stock install and must be explicitly enabled:

- **Object ACL checks** are off by default (`ozone.acl.enabled=false`); when
  enabled, **Native ACL is the default authorizer**. Operators can instead
  configure the Ranger plugin as the authorizer by setting
  `ozone.acl.authorizer.class` to
  `org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer`.
  Both are documented authorizers; S3 multi-tenancy setup requires Ranger.
  *(maintainer — smengcl, 2026-07-07)*
- **Block/container tokens** are off by default (`hdds.block.token.enabled=false`,
  `hdds.container.token.enabled=false`). When enabled, the block/container-token
  lifetime defaults to `hdds.block.token.expiry.time=1d`.
- **Delegation tokens** are enabled by default when security is enabled. OM
  delegation tokens renew every `1d` and stop renewing after `7d`.
- **Token signing keys** are SCM-issued symmetric keys. Defaults are
  `hdds.secret.key.expiry.duration=9d`, `hdds.secret.key.rotate.duration=1d`,
  `hdds.secret.key.rotate.check.duration=10m`, and `HmacSHA256`.
- **gRPC TLS** is off by default (`hdds.grpc.tls.enabled=false`) and protects
  gRPC traffic when enabled.
- **TDE/KMS** is optional and protects data at rest only for encrypted buckets;
  it requires a configured KMS, for example via `hadoop.security.key.provider.path`.

So a finding that assumes ACLs / block/container tokens / transport encryption /
TDE are active in a default build is `OUT-OF-MODEL: non-default-build` unless the
operator enabled them (§10); the §10 checklist lists these as required
production hardening. (Answers the Q-authz / Q-token / Q-tde default-state and
lifetime/rotation mechanism questions.)

## §6 Assumptions about inputs

Per-boundary input trust (grouped by family):

| Boundary | Input | Attacker-controllable? | Enforced by / caller must |
| --- | --- | --- | --- |
| S3 Gateway | REST request, SigV4 signature, headers, object data | **yes** | gateway verifies SigV4 against the user's S3 secret |
| OM RPC | request, Kerberos/delegation token, names | **yes (authenticated)** | auth; ACL/Ranger if enabled |
| Datanode | block/container read/write + token | **yes** | tokens verified when enabled |
| SCM | cert sign request, block alloc | **yes (authenticated service/admin)** | SCM verifies caller identity |
| Ratis | replicated log entries from a DN peer | **yes if peer compromised** | Raft quorum (honest majority) |
| TDE | object bytes | n/a (encryption is transparent) | KMS holds keys (operator) |

## §7 Adversary model

- **Untrusted S3/RPC client** — no valid identity; tries to access data,
  bypass SigV4/Kerberos, or exploit the gateway. In scope.
- **Authenticated-but-unauthorized user** — a valid Kerberos principal who
  tries to exceed their ACLs (read another bucket, escalate). In scope —
  authorization is the defence.
- **On-path network attacker** — passive/active on the wire. In scope where the
  deployment has enabled transport encryption for the relevant protocol.
- **Authenticated-but-Byzantine Datanode peer** — a compromised Datanode holding
  a legitimate SCM cert that then behaves arbitrarily in Ratis. In scope **up to
  the Raft honest-majority threshold**: Ratis gives standard Raft safety under an
  **honest majority** (e.g. 2 of 3 replicas for `RATIS THREE`) — it is **not**
  Byzantine fault tolerant. At or beyond a Byzantine majority, divergence is
  possible and **out of scope** (§3 / §8 conditions). Block-integrity defence is
  partial: Ozone has **checksum verification on normal reads plus replica/container
  checks**, so ordinary single-replica corruption is detected — but there is **no
  full guarantee against a Byzantine datanode that forges both data and metadata
  on the path it serves**. *(maintainer — smengcl, 2026-06-23; checksum behaviour documented in [ozone-site#397](https://github.com/apache/ozone-site/pull/397).)*
- **Out of scope:** compromised KDC / Ranger / SCM-CA-key / operator host;
  side-channel/co-tenant adversaries against the host; a client that an operator
  has authorized to do the thing it did.

## §8 Security properties the project provides (secure mode)

1. **Authenticated RPC.** All OM/SCM/DN RPC requires Kerberos (or a valid
   delegation token). *Violation:* unauthenticated request accepted. *Severity:*
   critical. *(documented — SASL/Kerberos; secure-mode posture folded into §5a.)*
2. **Capability-gated block/container access when tokens are enabled.** A
   Datanode serves protected blocks/containers only on a valid, unexpired,
   correctly-signed token. *Violation:* block/container read/write without a
   valid token in a token-enabled deployment. *Severity:* critical. *(documented
   — `BlockTokenVerifier`, `ContainerTokenVerifier`, and token configuration.)*
3. **Authorization when object ACL/Ranger checks are enabled.** Volume/bucket/key
   operations are checked against the configured Native ACL or Ranger authorizer.
   *Violation:* an authenticated user accesses data outside their ACLs in an
   ACL-enabled deployment. *Severity:* critical. *(documented — SecurityAcls,
   SecurityWithRanger; Ranger CLI/doc gaps tracked by HDDS-4089/HDDS-2093.)*
4. **Service identity.** Inter-service identity is backed by SCM-issued
   certificates. *Violation:* a rogue process impersonating a service on a
   certificate-backed path. *Severity:* critical. *(documented —
   `CertificateClient`/`CertificateServer`.)*
5. **Consensus safety (Ratis/Raft).** Committed metadata/data does not diverge or
   silently lose under the **honest-majority** bound (standard Raft safety, e.g.
   2 of 3 for `RATIS THREE`; **not** BFT — §7). *Violation:* fork / divergent
   replica state / acknowledged-write loss. *Severity:* critical; observable
   across nodes. *(maintainer — smengcl, 2026-06-23.)*
   - **Corollary — read-path integrity (partial).** Checksum verification on
     normal reads plus replica/container checks detect ordinary single-replica
     corruption; this does **not** extend to a Byzantine datanode that forges both
     data and metadata on its served path (that is §9 / out of scope).
     *(maintainer — smengcl, 2026-06-23.)*
6. **Confidentiality on configured transports / at rest.** Hadoop RPC privacy,
   gRPC TLS, or HTTPS protect the wire when configured; TDE protects data at
   rest when enabled (keys in KMS). *Violation:* plaintext on a transport
   configured for encryption / unencrypted blocks when TDE is configured.
   *Severity:* high. *(documented — protect-in-transit-traffic, SecuringTDE.)*

## §9 Security properties the project does *not* provide

- **No security in non-secure mode.** With `ozone.security.enabled=false` there
  is no authentication or token enforcement — by design, dev-only (§5a).
  - *False friend:* a reachable, "unauthenticated" endpoint in a non-secure dev
    cluster is **not** a vulnerability in the production (secure) posture.
- **It does not author your authorization policy.** Ozone enforces ACLs/Ranger
  *as configured*; an over-broad Ranger policy or world-readable ACL is an
  operator decision, not an Ozone flaw (§10).
- **It does not protect its dependencies.** KDC/Ranger/KMS/SCM-CA-key/network
  security are the operator's (§3/§10).
- **No defence against a Byzantine majority** of a Ratis ring, and **no full
  guarantee against a single Byzantine datanode that forges both data and metadata
  on the path it serves** — Ratis is not BFT, and while checksum + replica/container
  checks catch ordinary corruption, a peer that can forge consistently on its
  served path is out of model (§7). *(maintainer — smengcl, 2026-06-23.)*
- **Block tokens are bearer capabilities** — a leaked block token grants access
  until expiry; the caller/operator must protect tokens in transit (TLS). The
  default block/container-token lifetime is `1d` when those tokens are enabled.
  *(documented — token verifier/secret-manager code.)*
- **Well-known classes left to the operator/integrator:** SSRF/credential-relay
  via a misconfigured S3 Gateway, request smuggling at an LB in front of the
  gateway, and authz-policy errors.

## §10 Downstream responsibilities (operator + client)

- **Run secure mode** (`ozone.security.enabled=true`) for any non-sandbox
  cluster; require authentication on the S3 Gateway.
- **Secure the dependencies:** harden/operate the KDC, author least-privilege
  Ranger/ACL policies, protect the **SCM CA private key**, manage KMS keys,
  network-isolate datanode/Ratis/admin ports.
- **Protect tokens and secrets:** enable the relevant transport encryption
  (Hadoop RPC privacy, gRPC TLS, and/or HTTPS) so block/delegation tokens and S3
  secrets aren't sniffable; rotate S3 secrets; review token lifetimes for the
  deployment.
- **Protect service metadata at rest.** The OM, SCM, and Recon RocksDB stores
  hold critical credential/identity data — set restrictive file permissions and,
  ideally, encrypt them on disk. *(maintainer — jojochuang, 2026-06-25.)*
- **Isolate the KMS** in a separate, firewalled network segment. *(maintainer —
  jojochuang, 2026-06-25.)*
- **Client side:** treat data read from Ozone per your own trust needs; protect
  delegation tokens your app caches.

A consolidated **production secure-deployment checklist** for operators is
tracked for the Ozone docs (ozone-site) — gathering the secure-mode, ACL, token,
TDE/KMS, metadata-protection, and network-isolation steps above into one setup
list. *(requested by jojochuang, 2026-06-25.)*

## §11 Known misuse patterns

- Exposing a **non-secure** cluster (or an unauthenticated S3 Gateway) to an
  untrusted network.
- Treating **native ACLs** as sufficient where Ranger fine-grained authz is
  needed (or vice-versa), or writing world-permissive policies.
- Leaking **block/delegation tokens** over plaintext channels.
- Co-locating Datanode/SCM admin ports on an untrusted network segment.

## §11a Known non-findings (recurring false positives)

- **Unauthenticated endpoint reachable with `ozone.security.enabled=false`** —
  non-finding: non-secure mode is dev-only (§5a/§9). `OUT-OF-MODEL: non-default-build`.
- **"An authenticated user could request a token / cert"** — non-finding when
  it's within their identity; tokens/certs are the mechanism, trust is ACL/CA
  scoped (§6/§8).
- **Ranger policy too permissive** — operator policy decision, not Ozone code
  (§9/§10). `OUT-OF-MODEL: trusted-input`.
- **Findings in `ozone-thirdparty`, `integration-test-*`, `*TestImpl`** —
  `OUT-OF-MODEL: unsupported-component` (§3).
- **KDC/KMS/Ranger/SCM-CA-key compromise scenarios** — out of layer (§3/§7).
- **Hadoop-inherited RPC/SASL "issues"** already fixed upstream — check the
  Hadoop dependency version before reporting.

## §12 Conditions that would change this model

- A change to secure-mode defaults, transport-encryption defaults, the S3
  Gateway auth requirement, or the ACL/Ranger authorizer (§5a).
- A new network surface, a new token type, or a change to block-token
  verification.
- A change to the Ratis honest-majority assumptions or block integrity checks.
- A report unroutable to a §13 disposition → revise §8/§9.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | A §8 property breaks in secure mode, via an in-scope actor. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 break, but a §11 misuse is too easy to fall into. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of operator config (Ranger/ACL/keys). | §6/§10 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Needs KDC/CA-key/Byzantine-majority. | §7 |
| `OUT-OF-MODEL: non-default-build` | Only in non-secure mode (or a discouraged knob). | §5a |
| `OUT-OF-MODEL: unsupported-component` | thirdparty / test / infra Ozone doesn't own. | §3 |
| `BY-DESIGN: property-disclaimed` | Non-secure mode, policy correctness, dependency security. | §9 |
| `KNOWN-NON-FINDING` | Matches §11a. | §11a |
| `MODEL-GAP` | Unroutable. | triggers §12 |

## §14 Open questions for the maintainers

**Wave 1 — the load-bearing ones.**

- **Q-secure.** *(Answered — maintainer, smengcl 2026-06-23: yes, secure mode is
  the supported production posture; with security enabled the S3 Gateway rejects
  anonymous access, no plan otherwise — [HDDS-7961](https://issues.apache.org/jira/browse/HDDS-7961). Folded into §3/§5a/§9/§11a.)*
  Confirm secure mode (`ozone.security.enabled=true`) is the supported production
  posture, so non-secure-mode findings are `OUT-OF-MODEL: non-default-build`. Does
  the S3 Gateway ever support intended anonymous access? (§5a/§9/§11a/§13.)
- **Q-roles.** Confirm the actor split (untrusted client / authenticated-
  unauthorized user / operator / service peer / Byzantine datanode) and that
  the in-scope adversaries are the first, second, third-on-the-wire, and the
  bounded Byzantine peer. (§2/§7.)
- **Q-ratis.** *(Answered — maintainer, smengcl 2026-06-23: standard Raft safety
  under an honest majority (2 of 3 for `RATIS THREE`), not BFT; checksum +
  replica/container checks detect ordinary single-replica corruption, but no full
  guarantee against a Byzantine datanode forging both data and metadata on its
  served path. Folded into §7/§8/§9.)* What is the Ratis honest-majority safety
  bound you stand behind, and is there an independent block/container integrity
  check so a single Byzantine datanode can't serve corrupted data undetected?
  (§7/§8.)

**Wave 2 — mechanism confirmations.**

- **Q-authz.** *(Answered — maintainer, smengcl 2026-07-07: when
  `ozone.acl.enabled=true`, **Native ACL is the default authorizer**; the Ranger
  plugin is an opt-in alternative via
  `ozone.acl.authorizer.class=org.apache.ranger.authorization.ozone.authorizer.RangerOzoneAuthorizer`.
  The §8 authorization property holds for whichever authorizer is configured.
  Folded into §5a.)* (§8.)
- **Q-token.** Block/delegation token lifetimes, signing-key rotation, and the
  bearer-token caveat in §9 — confirm. (§8/§9.)
- **Q-tde / Q-net / Q-infra.** TDE/KMS production expectations, the
  network-isolation assumptions, and which dependencies (KDC/Ranger/KMS) you want
  explicitly named as operator-owned in §3/§10. (§5/§3/§10.)

**Wave 3 — scope & coexistence.**

- **Q-csi / Q-recon.** *(Answered — maintainer, jojochuang 2026-06-25: CSI driver
  is out of scope because it is not production-ready; Recon is in scope as part
  of the production cluster. Folded into §2/§3.)*
- **Q-doc.** This adds `THREAT_MODEL.md` + `AGENTS.md` alongside your existing
  `SECURITY.md` (preserved, pointer added). Confirm, and whether the model
  should become canonical. (§1/§15.)

## §15 Appendix — existing-policy back-map

The repo `SECURITY.md` is a disclosure-process policy (report to
`security@ozone.apache.org`); it embeds no threat model. This `THREAT_MODEL.md`
is additive — `SECURITY.md` is preserved and gains a pointer. Ozone's published
security documentation (secure-mode setup, ACLs, tokens, TDE) is a strong source
for refining §8/§11a in a later pass.
