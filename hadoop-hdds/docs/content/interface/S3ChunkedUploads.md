---
title: S3 Chunked Uploads
weight: 5
menu:
   main:
      parent: "S3 Protocol"
summary: Signature and trailing checksum verification for S3 chunked uploads.
hideFromSectionPage: true
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

S3 Gateway accepts `aws-chunked` request bodies for `PutObject` and multipart `UploadPart`, including datastream writes.
These chunks frame one request body; they are separate from the parts of a multipart upload.

## Payload modes

The following `x-amz-content-sha256` values select the documented streaming modes:

| Value | Chunk HMAC | Trailer HMAC | Payload checksum |
|---|---|---|---|
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | Conditional | None | No trailer |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` | Conditional | Conditional | Verified |
| `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | None | None | Verified |

“Conditional” refers to the requirements below. `UNSIGNED-PAYLOAD` describes a raw body, not an `aws-chunked` body.

Each streaming request requires `x-amz-decoded-content-length`, the object or part size without chunk framing.
If `Content-Encoding` is supplied, its comma-separated values must include `aws-chunked`; omission is accepted for SDK compatibility.

## When signatures are verified

For object and part writes, S3 Gateway enables HMAC verification when all of these conditions hold:

* The request uses `Authorization` header authentication with `AWS4-HMAC-SHA256`.
* The payload mode is one of the two HMAC modes above.
* OM returns a derived SigV4 signing key when the key is opened.

The validator is attached before reading the payload. If verification is selected but no derived key is returned,
`ozone.security.enabled=true` causes `InternalError`; non-secure mode skips HMAC verification.
A returned derived key enables verification in either mode.

Presigned requests use query authentication and do not enable chunk or trailer HMAC verification.
Declared checksum trailers are still verified independently of HMAC.

## Verification and commit

1. S3 Gateway removes chunk framing while reading the payload. When HMAC verification is enabled, it hashes each chunk and verifies its signature, chained from the request signature through the preceding chunks.
2. For trailer modes, it also computes the selected checksum over the whole decoded request payload. Neither calculation buffers the whole payload.
3. After reading the decoded length, the write guard completes verification, including the final zero-byte chunk and any declared trailer. A signed trailer authenticates the checksum header text; the checksum comparison separately checks the payload bytes.
4. A transfer or verification failure prevents the object or part from being committed. Data can already have been streamed to datanodes, but the failed upload does not become a committed key or part.

Strict completion is required when HMAC verification is enabled or a checksum trailer is declared.
Signed streams without either condition retain lenient completion behavior and do not provide full body verification.

## Checksum trailers

Both trailer modes require exactly one supported name in `x-amz-trailer`:

| Header name | Decoded checksum size |
|---|---|
| `x-amz-checksum-crc32` | 4 bytes |
| `x-amz-checksum-crc32c` | 4 bytes |
| `x-amz-checksum-crc64nvme` | 8 bytes |
| `x-amz-checksum-sha1` | 20 bytes |
| `x-amz-checksum-sha256` | 32 bytes |

The matching trailer value must be Base64-encoded with the decoded size above; CRC values use big-endian byte order.
Comma-separated trailer declarations are rejected.
Signed trailers require an `x-amz-trailer-signature` line after the checksum; signature fields must contain 64 hexadecimal characters.
Without a validator, signature format is checked but the signatures are not authenticated.
The trailer section must end with a blank line and the request body must end there.

After parsing the trailer, any enabled HMAC check runs before checksum decoding and comparison.
A valid trailer HMAC alone does not establish that the checksum matches the payload.

| Failure | S3 error | HTTP status |
|---|---|---|
| Missing decoded length or required `x-amz-trailer` declaration | `InvalidArgument` | 400 |
| Unsupported trailer name, malformed checksum, invalid trailer framing, or incomplete verified stream | `InvalidRequest` | 400 |
| Checksum does not match the decoded payload | `BadDigest` | 400 |
| Well-formed chunk or trailer signature fails an enabled HMAC check | `SignatureDoesNotMatch` | 403 |
| Derived key missing when HMAC verification is required in secure mode | `InternalError` | 500 |

## Limitations

* **FSO directories:** With `ozone.s3g.fso.directory.creation` enabled, a zero-length upload to a key ending in `/` in an FSO bucket can create a directory without opening a key. This path does not verify chunk or trailer HMAC. The two trailer modes still validate framing and the empty-payload checksum; the non-trailer mode does not consume or verify the chunk body on this path.
* **ECDSA / SigV4a:** Both authorization parsers reject `AWS4-ECDSA-P256-SHA256`. An ECDSA streaming payload marker does not establish SigV4a support or guarantee a successful unverified upload.
* **Checksum storage:** Trailer verification does not persist these checksums for retrieval or calculate an aggregate checksum across multipart parts. For `UploadPart`, the checksum covers only that request's part.

## References

* [AWS SigV4 chunked uploads](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html)
* [AWS signed trailing headers](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming-trailers.html)
* [AWS trailing checksums](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#trailing-checksums)
