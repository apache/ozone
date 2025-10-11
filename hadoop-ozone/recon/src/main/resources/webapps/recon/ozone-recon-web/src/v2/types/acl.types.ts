/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export const ACLIdentityTypeList = [
  'USER',
  'GROUP',
  'WORLD',
  'ANONYMOUS',
  'CLIENT_IP'
] as const;
export type ACLIdentity = typeof ACLIdentityTypeList[number];

export const ACLRightList = [
  'READ',
  'WRITE',
  'CREATE',
  'LIST',
  'DELETE',
  'READ_ACL',
  'WRITE_ACL',
  'ALL',
  'NONE'
] as const;
export type ACLRight = typeof ACLRightList[number];


export type Acl = {
  type: string;
  name: string;
  scope: string;
  aclList: string[];
}
