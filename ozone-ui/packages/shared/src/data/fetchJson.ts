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

/** Query-string parameters. `undefined`/`null` values are skipped. */
export type QueryParams = Record<string, string | number | boolean | undefined | null>;

export interface FetchJsonOptions extends Omit<RequestInit, 'body'> {
  /** Query-string parameters appended to `url`. */
  params?: QueryParams;
  /** Request body; objects are JSON-encoded with a JSON content-type. */
  body?: BodyInit | Record<string, unknown> | null;
}

/** Error thrown for non-2xx responses, carrying the HTTP status. */
export class HttpError extends Error {
  constructor(
    public readonly status: number,
    public readonly url: string,
    message?: string
  ) {
    super(message ?? `Request to ${url} failed with status ${status}`);
    this.name = 'HttpError';
  }
}

function withParams(url: string, params?: QueryParams): string {
  if (!params) {
    return url;
  }
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      search.append(key, String(value));
    }
  }
  const qs = search.toString();
  return qs ? `${url}${url.includes('?') ? '&' : '?'}${qs}` : url;
}

/**
 * Minimal JSON fetch helper built on the native `fetch` API — the standard
 * transport for the Ozone service UIs (no third-party HTTP client). Appends
 * query parameters, JSON-encodes object bodies, throws {@link HttpError} on
 * non-2xx responses, and parses the JSON response as `T`.
 */
export async function fetchJson<T>(url: string, options: FetchJsonOptions = {}): Promise<T> {
  const { params, body, headers, ...rest } = options;

  const isJsonBody =
    body != null && typeof body === 'object' && !(body instanceof FormData) && !(body instanceof Blob);

  const response = await fetch(withParams(url, params), {
    ...rest,
    headers: {
      Accept: 'application/json',
      ...(isJsonBody ? { 'Content-Type': 'application/json' } : {}),
      ...headers,
    },
    body: isJsonBody ? JSON.stringify(body) : (body as BodyInit | null | undefined),
  });

  if (!response.ok) {
    throw new HttpError(response.status, url, `${response.status} ${response.statusText}`);
  }

  return (await response.json()) as T;
}

export default fetchJson;
