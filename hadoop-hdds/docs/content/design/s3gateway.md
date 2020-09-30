---
title: S3 protocol support for Ozone
summary: Support any AWS S3 compatible client with dedicated REST endpoint
date: 2018-09-27
jira: HDDS-434
status: implemented
author: Marton Elek, Jitendra Pandey, Bharat Viswanadham & Anu Engineer
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

# Abstract

Introduction of a stateless separated daemon which acts as an S3 endpoint and transform calls to Ozone RPC calls.

This document is still valid but since the first implementation, more and more features are added.
 
# Link

 * https://issues.apache.org/jira/secure/attachment/12941573/S3Gateway.pdf
