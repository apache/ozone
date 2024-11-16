---
title: "GDPR in Ozone"
date: "2019-09-17"
weight: 3
icon: user
menu:
   main:
      parent: Security
summary: Support to implement the "Right to be Forgotten" requirement of GDPR
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

The General Data Protection Regulation (GDPR) is a law that governs how personal data should be handled. 
This is an European Union law, but due to the nature of software oftentimes spills into other geographies.

**Ozone supports GDPR's Right to Erasure(Right to be Forgotten) feature**

When GDPR support is enabled all the keys are encrypt, by default. The encryption key is stored on the metadata server and used to encrypt the data for each of the requests.

In case of a key deletion, Ozone deletes the metadata immediately but the binary data is deleted at the background in an async way. With GDPR support enabled, the encryption key is deleted immediately and as is, the data won't be possible to read any more even if the related binary (blocks or containers) are not yet deleted by the background process).

Once you create a GDPR compliant bucket, any key created in that bucket will 
automatically be GDPR compliant.

Enabling GDPR compliance in Ozone is very straight forward. During bucket
creation, you can specify `--enforcegdpr=true` or `-g=true` and this will
ensure the bucket is GDPR compliant. Thus, any key created under this bucket
will automatically be GDPR compliant.

GDPR can only be enabled on a new bucket. For existing buckets, you would
have to create a new GDPR compliant bucket and copy data from old bucket into
 new bucket to take advantage of GDPR.

Example to create a GDPR compliant bucket:

```shell
ozone sh bucket create --enforcegdpr=true /hive/jan

ozone sh bucket create -g=true /hive/jan
```

If you want to create an ordinary bucket then you can skip `--enforcegdpr`
and `-g` flags.

## References

 * [Design doc]({{< ref "design/gdpr.md" >}})
