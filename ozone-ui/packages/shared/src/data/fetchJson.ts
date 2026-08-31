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

/** Error thrown for non-2xx responses, carrying the HTTP status and response body. */
export class HttpError extends Error {
  constructor(
    public readonly status: number,
    public readonly url: string,
    /** Raw response body, when the server sent one (e.g. an error explanation). */
    public readonly body?: string
  ) {
    super(`Request to ${url} failed with status ${status}` + (body ? `: ${body}` : ''));
    this.name = 'HttpError';
  }
}

/**
 * Error thrown when the request never reached the server — a transport failure
 * such as DNS/connection refused, CORS, offline, or timeout (a native `fetch`
 * rejection). Distinct from {@link HttpError} so callers can tell a network
 * outage apart from a server response.
 */
export class NetworkError extends Error {
  constructor(
    public readonly url: string,
    options?: { cause?: unknown }
  ) {
    super(`Network request to ${url} failed`, options);
    this.name = 'NetworkError';
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
 * query parameters, JSON-encodes object bodies, and:
 * - throws {@link NetworkError} when the request never reaches the server,
 * - throws {@link HttpError} (with the response body) on a non-2xx response,
 * - parses the JSON response as `T`, returning `undefined` for an empty body.
 */
export async function fetchJson<T>(url: string, options: FetchJsonOptions = {}): Promise<T> {
  const { params, body, headers, ...rest } = options;

  const isJsonBody =
    body != null &&
    typeof body === 'object' &&
    !(body instanceof FormData) &&
    !(body instanceof Blob);

  let response: Response;
  try {
    response = await fetch(withParams(url, params), {
      ...rest,
      headers: {
        Accept: 'application/json',
        ...(isJsonBody ? { 'Content-Type': 'application/json' } : {}),
        ...headers,
      },
      body: isJsonBody ? JSON.stringify(body) : (body as BodyInit | null | undefined),
    });
  } catch (cause) {
    // `fetch` only rejects when the request never completed (transport failure).
    throw new NetworkError(url, { cause });
  }

  if (!response.ok) {
    const errorBody = await response.text().catch(() => undefined);
    throw new HttpError(response.status, url, errorBody || undefined);
  }

  // Tolerate empty / non-JSON success bodies (e.g. a 204 from a POST): parse the
  // text only when present so callers of no-content endpoints don't blow up.
  const text = await response.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

export default fetchJson;
