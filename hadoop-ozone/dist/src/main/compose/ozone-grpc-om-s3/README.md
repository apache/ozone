<!---
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

# Using gRPC between OM and S3G

This is the same as `compose/ozone` but for testing operations with gRPC enabled 
between OzoneManager and S3Gateway Clients.

### Differences with `compose/ozone`

* in `docker-config`
  * for enabling grpc server on om side
    * `OZONE-SITE.XML_ozone.om.s3.grpc.server_enabled=true`
  * for enabling grpc transport on client side
    * `OZONE-SITE.XML_ozone.om.transport.class=org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransportFactory`