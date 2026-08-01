# Security Policy

## Supported Versions

Please check the Apache Ozone [website](https://ozone.apache.org/download/) for the list of versions currently supported.

## Reporting a Vulnerability

To report any found security issues or vulnerabilities, please send a mail to security@ozone.apache.org, so that they may be investigated and fixed before the vulnerabilities is published.

This email address is a private mailing list for discussion of potential security vulnerabilities issues.

This mailing list is **NOT** for end-user questions and discussion on security. Please use the dev@ozone.apache.org list for such issues.

In order to post to the list, it is **NOT** necessary to first subscribe to it.

## Threat Model

A threat model for Apache Ozone is maintained in [THREAT_MODEL.md](THREAT_MODEL.md).
It describes the multi-service trust boundaries (S3 Gateway, OM, SCM/CA,
Datanodes/Ratis), the load-bearing role of **secure mode**
(`ozone.security.enabled`), the properties Ozone provides versus those left to
the operator (Kerberos KDC, Ranger policy correctness, SCM CA key, KMS, network
isolation), and the recurring non-findings. Triagers of scanner, fuzzer, or
AI-generated findings should route them through `THREAT_MODEL.md` section 13.
