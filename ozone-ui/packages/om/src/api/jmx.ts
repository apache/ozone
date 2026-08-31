/**
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

import { fetchJson } from '@ozone-ui/shared';

/**
 * The OM exposes runtime state via its JMX servlet at `GET /jmx?qry=<query>`,
 * always returning `{ beans: [...] }`. In development Vite proxies `/jmx` to the
 * json-server mock (see `mock/server.cjs`).
 *
 * Requests are issued via TanStack Query (see {@link useSuspenseJmxBean}); query-key
 * de-duplication means several sections depending on the same MBean (e.g. the OM
 * ServerRuntime bean feeds Instance Details, Roles and Metadata Volume) share a
 * single request, and sections fetch lazily so a query is never sent for a
 * section that is not rendered.
 */
export interface JmxResponse<T> {
  beans: T[];
}

/** Issue a JMX query and return the matching MBeans (empty array when none). */
export async function queryJmx<T>(qry: string): Promise<T[]> {
  const data = await fetchJson<JmxResponse<T>>('/jmx', { params: { qry } });
  return data?.beans ?? [];
}
