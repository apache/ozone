---
title: S3 Multi Chunks Verification
date: 2026-03-30
jira: HDDS-12542
status: design
author: Chung-En Lee
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

# Context & Motivation

Ozone S3 Gateway (S3G) currently utilizes SignedChunksInputStream to handle aws-chunked content-encoding for AWS Signature V4. However, it doesn’t do any signature verification now. This proposal aims to complete  the existing SignedChunksInputStream to make sure signature verification is correct and minimize performance overhead.

# Goal

Support signature verification for AWS Signature Version 4 streaming chunked uploads with the following algorithms:
- STREAMING-AWS4-HMAC-SHA256-PAYLOAD
- STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
- STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD
- STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER

# Proposed Solution

Currently, the SignedChunksInputStream successfully parses the S3 chunked upload payload but lacks the actual cryptographic verification. This proposal enhances the existing stream to perform real-time signature verification, while ensuring the output remains fully compatible with Ozone's native, high-throughput write APIs.

## HMAC-SHA256 Implementation

To achieve this with minimal overhead, we will reuse and refactor AWSV4AuthValidator to expose getSigningKey and calculateSignature separately. This allows the SignedChunksInputStream to compute the derived key strictly once per request, avoiding expensive HMAC recalculations per chunk and reusing the existing highly-optimized ThreadLocal Mac instances.

## ECDSA-SHA256 Implementation

While this proposal strictly focuses on HMAC-SHA256 verification (Goals), supporting AWS Signature V4a (ECDSA) for multi-region access is a recognized future enhancement.
AWS SDK relies on the AWS Common Runtime (CRT), a C-based native library, to perform the complex Key Derivation Function (NIST SP 800-108) required for SigV4a. To implement this in Ozone while adhering to Apache's strict supply chain security and avoiding the risks of pre-compiled third-party JNI binaries, we propose the following roadmap for a future Jira ticket:

- Native Module Compilation: Similar to Ozone's existing rocks-native module, we will introduce a dedicated module (e.g., hdds-aws-crt-native). This module will compile the AWS CRT C/C++ source code using CMake directly during the Ozone Maven build process.
- Dynamic Loading: We will leverage Ozone's existing NativeLibraryLoader infrastructure to safely extract and load the compiled dynamic libraries (.so, .dylib) at runtime.
- JNI Integration: A Java wrapper will be implemented to pass the secret key to the native KDF function to derive the ECDSA public key, which will then be used by a new ECDSAChunkSignatureVerifier.

# Trade-offs

## Verification in S3 Gateway (Fail-Fast vs. Backend Processing)

Rather than offloading the cryptographic verification to the backend Ozone cluster or introducing complex asynchronous pre-fetching, we decided to maintain the current stream-based architecture and execute the verification process entirely within the S3 Gateway (S3G). The primary reasons for this architectural choice are:

- Fail-Fast: This allows us to immediately reject requests with invalid signatures at the edge, preventing malformed data from consuming DataNode I/O, network bandwidth, and storage capacity.
- Stateless Scalability: Signature verification is a CPU-intensive task. Since the S3 Gateway is stateless and horizontally scalable, offloading the cryptographic computations to S3G prevents the backend Ozone Managers (OM) or DataNodes (DN) from becoming CPU bottlenecks. S3G instances can be scaled out independently as compute demands increase.

## Accumulative Buffering

The chunk size defined by AWS S3 Signature V4 (e.g., 64KB) is fundamentally different from—and typically much smaller than—Ozone's optimal internal transmission chunk size (e.g., 4MB).
To bridge this impedance mismatch, we introduce an internal buffering mechanism within the SignedChunksInputStream. The stream will aggressively read, verify, and accumulate multiple small S3 chunks in its buffer until the data reaches Ozone's preferred transmission length. This significantly reduces the number of micro-writes and system calls made to the downstream OzoneOutputStream, maximizing Ratis pipeline throughput.

## Incremental Hashing

To maintain a low memory footprint during the continuous buffering process, the signature calculation utilizes Mac.update() (Incremental Hashing) directly on the incoming byte streams. This ensures that we validate the payload on the fly without allocating massive temporary byte arrays, avoiding Garbage Collection (GC) spikes during large multi-gigabyte uploads.

# Performance

To ensure that introducing the real-time signature verification process does not significantly degrade the overall upload throughput, the architecture is designed with the following optimizations in mind. Furthermore, we plan to conduct simple benchmarks in the future to validate these performance expectations:

- Constant Memory: Incremental hashing processes byte streams on the fly. This prevents large memory allocations and avoids GC spikes during massive uploads.
- CPU Offloading & Scalability: Cryptographic computation is isolated in the stateless, horizontally scalable S3G instances. This allows verification throughput to scale easily by adding more S3G nodes, protecting backend OM and DataNodes from CPU bottlenecks.

# Compatibility

A core principle of this design is to introduce robust security enhancements without breaking existing workflows or requiring modifications from end-users. The proposed architecture ensures seamless integration with current S3 clients and minimal impact on Ozone's internal backend components.

- Client Compatibility: Standard S3 clients (e.g., AWS SDK) require no changes to use the signature verification.
- Backend Compatibility: No changes to existing Ozone data layouts or core RPC protocols. Only a lightweight OM API is added for S3G to retrieve the key.

# Test

To guarantee the correctness, stability, and security of the newly introduced chunk verification logic, a comprehensive testing strategy will be executed. This plan covers both granular unit testing for the stream parsing logic and end-to-end integration testing using official AWS SDKs.

- Unit Tests (SignedChunksInputStream.java): There is an existing test class for SignedChunksInputStream. To complete it, we should make sure it works well on different signatures.
- Integration Tests:
  - AbstractS3SDKV1Tests: Add a new test for testing STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD.
  - AbstractS3SDKV2Tests: Add a new test for testing STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER.

