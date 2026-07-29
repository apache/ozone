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

import axios from 'axios';

/**
 * The OM exposes runtime state via its JMX servlet at `GET /jmx?qry=<query>`,
 * always returning `{ beans: [...] }`. In development Vite proxies `/jmx` to the
 * json-server mock (see `mock/server.cjs`).
 *
 * Fetches are keyed and de-duplicated by query string (see {@link fetchJmxBeans}):
 * several sections of a page may depend on the same MBean (e.g. the OM
 * ServerRuntime bean feeds Instance Details, Roles and Metadata Volume), yet the
 * query is only issued once. Sections also fetch lazily, so a query is never
 * sent for a section that is not rendered — this keeps us from pulling the full
 * multi-thousand-line JMX dump when only a few beans are needed.
 */
const client = axios.create({ baseURL: '' });

export interface JmxResponse<T> {
  beans: T[];
}

/** Issue a JMX query and return the matching MBeans (no caching). */
export async function queryJmx<T>(qry: string): Promise<T[]> {
  const { data } = await client.get<JmxResponse<T>>('/jmx', { params: { qry } });
  return data?.beans ?? [];
}

/** In-flight / resolved query cache, keyed by the JMX query string. */
const cache = new Map<string, Promise<unknown[]>>();

/**
 * Fetch MBeans for a query, sharing a single request across all callers that ask
 * for the same query. Failed requests are evicted so they can be retried.
 */
export function fetchJmxBeans<T>(qry: string): Promise<T[]> {
  let pending = cache.get(qry) as Promise<T[]> | undefined;
  if (!pending) {
    pending = queryJmx<T>(qry).catch((err) => {
      cache.delete(qry);
      throw err;
    });
    cache.set(qry, pending as Promise<unknown[]>);
  }
  return pending;
}

/** Fetch a single MBean for a query (the first bean), or `undefined`. */
export async function fetchJmxBean<T>(qry: string): Promise<T | undefined> {
  const beans = await fetchJmxBeans<T>(qry);
  return beans[0];
}

/** Drop all cached queries so the next fetch re-hits the endpoint (refresh). */
export function clearJmxCache(): void {
  cache.clear();
}
