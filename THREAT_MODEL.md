# Apache Ozone — Threat Model (v0 draft)

## §1 Header

- **Project:** Apache Ozone (`apache/ozone`), `master`. A distributed object store for big-data / Kubernetes workloads, with an S3-compatible API; part of the Hadoop ecosystem. Scope for this engagement: **apache/ozone** (the server + clients — `hadoop-hdds`, `hadoop-ozone`) and **apache/ozone-thirdparty** (vendored/shaded native deps — see §2/§3).
- **Date:** 2026-06-02. **Status:** draft — for the Apache Ozone PMC to review. **Author:** ASF Security team (drafted via the Scovetta threat-model rubric), for PMC ratification.
- **Version binding:** versioned with the project; a report against version *N* is triaged against the model as it stood at *N*.
- **Reporting cross-reference:** §8-property violations → report privately to **security@ozone.apache.org** per the project [`SECURITY.md`](./SECURITY.md); §3/§9 findings are closed citing this document.
- **Provenance legend:** *(documented)* = Ozone's own docs/repo; *(maintainer)* = confirmed by an Ozone PMC member through this process (siyao@ confirmed scope + path 3 + the focus areas on 2026-06-02); *(inferred)* = reasoned from architecture, not yet confirmed — each has a matching §14 open question.
- **Draft confidence:** ~14 documented / ~3 maintainer / ~34 inferred.
- **What Ozone is:** Apache Ozone is a scalable, distributed object/key-value store. Clients read/write keys in buckets/volumes via a native RPC client or an S3-compatible REST gateway. The **Ozone Manager (OM)** owns the namespace/metadata, the **Storage Container Manager (SCM)** manages blocks/containers and the certificate/block-token infrastructure, **DataNodes** store the block data, the **S3 Gateway** offers the AWS-S3 API, and **Recon** provides monitoring. HA and replication use Apache Ratis (Raft); security uses Kerberos plus delegation/block tokens. *(documented — README, ozone.apache.org; maintainer — siyao@ 2026-06-02: scope apache/ozone + apache/ozone-thirdparty, focus OM/SCM RPC, S3 gateway, Kerberos/token auth)*

## §2 Scope and intended use

- **Primary use:** an operator-deployed, multi-node object store accessed over the network by object clients and S3 clients. *(documented)*
- **Caller roles** (network service — roles split):
  - **object / S3 client** — reads/writes keys via the RPC client or the S3 gateway; authenticates with Kerberos (RPC) or S3 credentials/AWS SigV4 (gateway); constrained by ACLs. **Untrusted beyond its grants.** *(inferred)*
  - **operator/admin** — Kerberos admin principal; controls config, keytabs, the SCM CA, Ranger policies. **Trusted.** *(inferred)*
  - **cluster service peer** — OM / SCM / DataNode service principals + Ratis ring members. Authenticated; trust posture is a §14 question. *(inferred)*
  - **Recon** — read-mostly monitoring service. *(inferred)*

**Component-family table** *(in/out of model):*

| Family | Entry point | Touches network/OS | In model? |
| --- | --- | --- | --- |
| Ozone Manager (OM) | metadata RPC (volumes/buckets/keys, ACLs, delegation tokens) | network (listens) | **In — primary boundary** *(documented)* |
| Storage Container Manager (SCM) | block/container RPC, CA + block-token issuance | network | **In — primary boundary** *(documented)* |
| DataNode data plane | block read/write (block-token-gated), container ops | network | **In** *(inferred)* |
| S3 Gateway | S3 REST API, AWS SigV4, S3 secrets | network (HTTP, often edge) | **In — high value** *(documented)* |
| Auth / tokens | Kerberos, delegation tokens, block tokens, S3 secret, SCM CA/certs | — | **In** *(documented: Kerberos+tokens)* |
| Authorization | native ACLs + Apache Ranger plugin | — | **In** *(inferred)* |
| Web UIs / Recon | HTTP (SPNEGO) | network | **In iff reachable; admin-ish** *(inferred)* |
| Ratis (HA + replication) | OM/SCM HA ring, datanode pipeline replication | inter-node network | **In (cluster trust posture — §14)** *(inferred)* |
| `apache/ozone-thirdparty` | vendored/shaded native deps (protobuf/gRPC/etc.) | build | **In: Ozone's packaging/use; upstream internals out** *(maintainer — in scope per siyao@; boundary inferred)* |
| `hadoop-hdds/docs`, dev tooling | docs / build | — | **Out** *(see §3)* |

## §3 Out of scope (explicit non-goals)

- **Attackers who already control the host, the Ozone config, keytabs, the SCM CA private key, or a service principal.** Operator-trusted. *(inferred)*
- **The packaging/infra repos** (`ozone-site`, `ozone-docker*`, `ozone-helm-charts`, `ozone-installer`, `ozone-go`) — the PMC narrowed scope to apache/ozone + apache/ozone-thirdparty *(maintainer — siyao@ 2026-06-02)*.
- **Upstream library internals** — Hadoop common, Ratis, RocksDB, gRPC/protobuf as released. A flaw in the upstream artifact unchanged by Ozone routes to that project. **`apache/ozone-thirdparty`** is largely repackaged/shaded upstream native deps: Ozone's *packaging and how it consumes them* is in scope; the upstream code's own internals are not. *(inferred — confirm the boundary)*
- **`docs/`, dev-support, test fixtures.** *(inferred)*
- **Generic DoS / capacity** beyond a to-be-stated line (§8). *(inferred)*

## §4 Trust boundaries and data flow

- **Primary boundary: the client-facing RPC + S3 surfaces.** OM/SCM RPC requests and S3-gateway requests arrive from clients authenticated by Kerberos or S3 credentials; a client is bounded by its ACLs. The in-model question is whether a client can read/write keys outside its ACLs, forge a token, or reach an admin operation. *(inferred; auth model documented)*
- **DataNode boundary: block tokens.** A client presents a **block token** (issued by OM/SCM) to a DataNode to read/write a block; the DataNode validates it. A finding where a client reads/writes block data **without a valid block token**, or forges one, is in-model. *(inferred)*
- **S3 boundary: AWS SigV4 + S3 secret.** The S3 gateway authenticates requests by SigV4 over an S3 secret derived per user; the gateway maps S3 identities to Ozone principals. *(inferred)*
- **Inter-node boundary (Ratis / SCM CA):** OM/SCM HA and datanode replication run over Ratis between service principals; SCM is the CA. Whether this is assumed to run on a trusted network or is fully mutually-authenticated against an active attacker is a §14 question. *(inferred)*
- **Reachability preconditions:**
  - A finding in OM/SCM/DataNode/S3G is in-model iff reachable from a client at its role under **a security-enabled (Kerberos) deployment** (see §5a). *(inferred)*
  - A finding requiring admin/host/keytab/CA control is `OUT-OF-MODEL: trusted-input`. *(inferred)*
  - A finding that only manifests with **security disabled** (`ozone.security.enabled=false` / simple auth) is `OUT-OF-MODEL: non-default-build` unless the PMC says the unsecured mode is supported for production (§14). *(inferred)*
  - A finding in upstream/`ozone-thirdparty` code unchanged by Ozone is `OUT-OF-MODEL: unsupported-component` / route upstream. *(inferred)*

## §5 Assumptions about the environment

- **Runtime:** JVM; multi-node cluster (OM, SCM, DataNodes, S3 Gateway, optional Recon). *(documented)*
- **Security:** Apache Hadoop security model — Kerberos for authentication, delegation/block tokens for delegated access, SCM as an internal CA issuing certificates to services and block tokens. *(documented)*
- **Metadata store:** RocksDB (OM/SCM). **Consensus:** Apache Ratis (Raft) for OM/SCM HA and datanode pipeline replication. *(documented)*
- **Network:** TLS/SPNEGO configurable; whether inter-node + client channels are encrypted by default is the operator's posture. *(inferred)*
- **Negative side-effects inventory** (inferred — wave-1/2 target): Ozone listens on multiple RPC/HTTP ports; reads keytabs + config; SCM holds CA key material; DataNodes write block data to local disks; talks to Ranger if configured. *(inferred)*

## §5a Build-time and configuration variants

| Knob | Default | Effect / stance |
| --- | --- | --- |
| `ozone.security.enabled` | **to confirm** (Hadoop default is typically `false` = simple auth) | The master switch for Kerberos + tokens. If it ships off, a report against an unsecured cluster is `non-default-build` *unless* the PMC designates secured-mode as the only supported production posture. **Wave-1 ruling.** *(inferred)* |
| `ozone.acl.enabled` + authorizer (native / Ranger) | to confirm | Whether ACLs are enforced + by which authorizer. *(inferred)* |
| S3 gateway auth | SigV4 over per-user S3 secret | Whether anonymous/unauthenticated S3 access is possible by default. *(inferred)* |
| TLS / SPNEGO on RPC, HTTP, inter-node | to confirm | Wire encryption posture. *(inferred)* |
| block-token / delegation-token enforcement | tied to `security.enabled` | Whether DataNodes require a valid block token. *(inferred)* |

**Insecure-default check:** the single biggest ruling — is the **security-enabled (Kerberos) deployment the in-model baseline** (so simple-auth findings are `non-default-build`), or is unsecured mode also a supported production posture? This reshapes §8/§13. Wave-1 question (§14).

## §6 Assumptions about inputs

Per-surface trust table *(auth model documented; trust framing inferred):*

| Surface | Input | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| OM RPC | volume/bucket/key ops, ACL ops, delegation-token ops | **yes, within Kerberos identity + ACLs** | ACLs; token validation; admin-op gating |
| SCM RPC | block/container ops, cert/token issuance | **yes (authenticated services + clients)** | service-principal auth; CA protection |
| DataNode | block read/write + **block token** | **yes** | block-token validation; no token = no access |
| S3 Gateway | S3 REST, AWS SigV4, headers, object data | **yes (often edge-exposed)** | SigV4 verification; S3-secret confidentiality; bucket/key ACL mapping |
| Web UI / Recon | HTTP | **yes** | SPNEGO/auth; network-restrict |
| Ratis inter-node | Raft messages, replicated data | **yes if the cluster net is exposed** | trusted net / mutual auth |
| key/object data | bytes stored & served | **yes** | integrity is the store's; not interpreted as code |

- **Size/shape/rate:** per-request and per-key bounds, and whether an expensive listing / huge multipart upload is a bug or operator-managed, is open (§8). *(inferred)*

## §7 Adversary model

- **Authenticated low-privilege client (primary)** — a valid Kerberos/S3 identity trying to read/write keys outside its ACLs, escalate, forge a delegation/block token, or reach an admin op. *(inferred)*
- **Network attacker on the S3/RPC edge** — unauthenticated, attempting auth bypass / SigV4 forgery / token replay; or, if security is disabled, direct access. *(inferred)*
- **Authenticated-but-Byzantine peer (OM/SCM/DataNode)** — holds a service identity and behaves arbitrarily; goal: corrupt metadata/blocks, break replication/consensus safety. Honest-fraction threshold + whether this is in scope is a §14 question. *(inferred)*
- **Out of scope:** admin / keytab / SCM-CA-key / host control; anyone who disabled security themselves. *(inferred)*

## §8 Security properties the project provides

*(All inferred pending PMC confirmation except where the auth architecture is documented.)*

- **Authentication + ACL enforcement (security-enabled).** A client cannot read/write/list keys, volumes, or buckets beyond its Kerberos/S3 identity's ACLs (native or Ranger); unauthenticated clients cannot act. *Violation symptom:* unauthorized key/bucket/volume access or admin op. *Severity:* CVE-class. *(inferred; ACL+Kerberos documented)*
- **Token integrity.** Delegation tokens and block tokens cannot be forged, replayed across scope, or used past expiry; a DataNode serves block data only against a valid block token. *Violation symptom:* token forgery/replay yields data access. *Severity:* CVE-class. *(inferred)*
- **S3 authentication.** The S3 gateway rejects requests with an invalid/absent SigV4 signature and maps S3 identities to the correct Ozone principal. *Violation symptom:* anonymous or wrong-identity S3 access. *Severity:* CVE-class. *(inferred)*
- **Tenant / volume isolation.** A principal scoped to one volume/bucket/tenant cannot reach another's keys. *Violation symptom:* cross-tenant data access. *Severity:* CVE-class (data exposure). *(inferred)*
- **Replication / consensus safety** (Ratis) — under the §7 honest-peer threshold, committed metadata/blocks don't fork or silently diverge. *Violation symptom:* state fork / divergent OM-DB / lost-write. *Severity:* CVE-class. *(inferred — confirm what's claimed against a Byzantine peer)*
- **Resource bounds — UNSPECIFIED.** Whether an expensive listing, huge multipart upload, or request flood is a bug or operator-managed is open. *(inferred)*

## §9 Security properties the project does *not* provide

- **No protection when `ozone.security.enabled=false`** (simple auth) on an untrusted network — that's a deliberately-unsecured mode; securing the deployment (Kerberos) is the operator's (pending §5a ruling). *(inferred)* **False friend:** simple-auth "usernames" are not authentication.
- **No defense against a malicious operator / holder of the SCM CA key or a service keytab.** *(inferred)*
- **Trusted-network assumption for inter-node Ratis/replication** unless mutual TLS is configured — Ozone does not, by default, defend the inter-node channel against an active network attacker. *(inferred)*
- **No responsibility for upstream library flaws** (Hadoop/Ratis/RocksDB/gRPC) or `ozone-thirdparty` upstream internals as released — routed upstream. *(inferred)*
- **Not a defense against S3-secret compromise** — a leaked S3 secret is the holder's identity; protecting it is the client's. *(inferred)*
- **Well-known classes left to the caller/operator:** Kerberos/keytab management, S3-secret confidentiality, network exposure of admin/Recon/RPC ports, and generic DoS. *(inferred)*

## §10 Downstream responsibilities (operator/deployer)

*(All inferred — confirm.)*

- **Enable security** (`ozone.security.enabled=true`, Kerberos) for any non-trivial/production deployment; do not expose an unsecured cluster. *(inferred)*
- **Enable + configure ACLs / Ranger**; default-deny across volumes/buckets. *(inferred)*
- **Protect keytabs and the SCM CA key material**; restrict admin RPCs. *(inferred)*
- **Run the inter-node (Ratis) channel on a trusted/segmented network**, or enable mutual TLS. *(inferred)*
- **Front the S3 gateway with TLS**; keep S3 secrets confidential; network-restrict Recon + web UIs + admin ports. *(inferred)*
- **Set listing/upload/rate limits** appropriate to capacity. *(inferred)*

## §11 Known misuse patterns

*(Draft one-liners — expand before publishing.)*

- Running with `ozone.security.enabled=false` on an untrusted network. *(inferred)*
- Exposing the S3 gateway / OM / SCM / Recon RPC or HTTP ports to the public internet without auth. *(inferred)*
- Treating simple-auth usernames as authentication. *(inferred)*
- Running Ratis replication across an untrusted network without mutual TLS. *(inferred)*
- Leaking an S3 secret / a keytab. *(inferred)*

## §11a Known non-findings (recurring false positives)

*(Seed list — confirmations here are the highest-leverage scan-suppression input.)*

- "No auth / anyone can access" on a cluster with `ozone.security.enabled=false` — unsecured mode is operator-chosen; `OUT-OF-MODEL: non-default-build` unless the PMC says otherwise. *(inferred)*
- "Admin can do destructive/privileged operation X" — admin is trusted (§7). *(inferred)*
- "Vulnerability in Hadoop/Ratis/RocksDB/gRPC" (or `ozone-thirdparty` upstream code) where the root cause is the upstream artifact as released — route upstream; not an Ozone bug. *(inferred)*
- "Ratis cluster traffic can be tampered/deserialized" — inter-node assumes a trusted network / configured mTLS (§9). *(inferred)*
- "Expensive listing / large upload consumes resources" — pending the §8 resource line; likely operator-managed unless super-linear. *(inferred)*
- "S3 access with a stolen S3 secret" — secret compromise is out of model (§9). *(inferred)*

## §12 Conditions that would change this model

- A change to the `ozone.security.enabled` default or the supported-posture ruling. *(inferred)*
- A new client-facing surface, a new token type, or a change to the S3-auth / block-token model. *(inferred)*
- A change to the inter-node (Ratis) trust posture or the SCM CA model. *(inferred)*
- Promoting a packaging/infra repo into scope, or pulling more of `ozone-thirdparty` in. *(inferred)*
- A report that cannot be routed to one §13 disposition → revise the model.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a §8 property via an in-scope adversary/input under a security-enabled deployment (ACL bypass, token forgery, S3-auth bypass, cross-tenant access, consensus fork). | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but a §11 misuse is easy enough to harden. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires admin / keytab / SCM-CA / config / service-principal control. | §6, §7 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires a capability the model excludes (host control, exposed inter-node net when posture says trusted). | §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in docs/dev-support, the deferred packaging repos, or upstream / `ozone-thirdparty` upstream internals. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests with security disabled (`ozone.security.enabled=false`) or another discouraged §5a setting. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (simple-auth, malicious operator, trusted-net inter-node, upstream libs, S3-secret compromise). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry. | §11a |
| `PUNT-UPSTREAM` | Root cause in Hadoop / Ratis / RocksDB / gRPC / a thirdparty lib as released. | §3, §9 |
| `MODEL-GAP` | Cannot be routed — triggers §12. | §12 |

## §14 Open questions for the maintainers

**Wave 1 — scope & the security baseline:**
1. Confirm scope: **apache/ozone** is the in-model core (OM/SCM/DataNode/S3G/Recon/client); **apache/ozone-thirdparty** is in scope for Ozone's packaging/consumption but its upstream internals are out; the packaging/infra repos are deferred. → §2/§3.
2. **The big ruling:** is a **security-enabled (Kerberos) deployment** the in-model baseline — so findings that only manifest with `ozone.security.enabled=false` (simple auth) are `OUT-OF-MODEL: non-default-build`? Or is unsecured mode also a supported production posture? What's the default? → §5a/§13.
3. Confirm `docs/`/dev-support/test fixtures are out of scope. → §3.

**Wave 2 — auth, tokens, S3:**
4. **Token model:** confirm delegation tokens + block tokens are the gate (a DataNode serves a block only against a valid block token), and that forgery/replay/cross-scope use is `VALID`. → §8.
5. **S3 gateway:** is anonymous/unauthenticated S3 access possible by default, or is SigV4 always required? How are S3 identities mapped to Ozone principals/ACLs? → §6/§8.
6. **Authorization:** native ACLs vs Ranger — which is the default enforcement, and is ACL bypass `VALID` under either? → §8.

**Wave 3 — cluster, resources, meta:**
7. **Inter-node (Ratis) posture:** trusted-network assumption, or mutually-authenticated against an active attacker? Any Byzantine-peer safety claim for OM/SCM HA + datanode replication, and the honest threshold? → §7/§8/§9.
8. **Resource/DoS line:** is an expensive listing / huge multipart upload / request flood a bug or operator-managed? Where's the line? → §8/§11a.
9. Any other recurring scanner/fuzzer false positives to seed §11a (e.g. simple-auth, Hadoop/Ratis upstream, RocksDB)? → §11a.
10. **Meta:** Ozone has a `SECURITY.md` (reporting policy → security@ozone.apache.org) but no in-repo threat model or `AGENTS.md`. This engagement adds `THREAT_MODEL.md`, appends a `## Threat Model` link to the existing `SECURITY.md`, and adds `AGENTS.md` (wiring `AGENTS.md → SECURITY.md → THREAT_MODEL.md`). Confirm the in-repo model is canonical and references the website security docs; confirm revision ownership. → §1.
