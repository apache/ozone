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

import React from 'react';
import { QueryErrorResetBoundary } from '@tanstack/react-query';
import { HttpError } from '../../data/fetchJson';
import { NetworkErrorState, ServerErrorState } from '../ErrorState/ErrorState';
import { ErrorBoundary } from './ErrorBoundary';

/** Arguments passed to a custom {@link QueryErrorBoundary} fallback. */
export interface QueryErrorFallbackProps {
  error: Error;
  /** Reset the failed queries and clear the boundary (wired to Retry). */
  retry: () => void;
}

export interface QueryErrorBoundaryProps {
  children: React.ReactNode;
  /** Override the default (network vs. 500) error page. */
  fallback?: (props: QueryErrorFallbackProps) => React.ReactNode;
  /** Reset the boundary when any of these values change (e.g. the route). */
  resetKeys?: unknown[];
}

/**
 * The default fallback: a server-side failure (HTTP 5xx) shows the 500 state;
 * anything else — a network/timeout failure (native `fetch` rejects with a
 * `TypeError`), an aborted request, or an unclassified error — shows the network
 * state. Both wire their action button to `retry`.
 */
function defaultFallback({ error, retry }: QueryErrorFallbackProps): React.ReactNode {
  if (error instanceof HttpError && error.status >= 500) {
    return <ServerErrorState onAction={retry} />;
  }
  return <NetworkErrorState onAction={retry} />;
}

/**
 * Page-level error boundary for TanStack Query. Because the Ozone service UIs
 * read every section from one endpoint, a failure fails all queries — so this
 * renders a single error page for the whole subtree. Retrying resets the failed
 * queries (`QueryErrorResetBoundary`) and clears the boundary, so the suspended
 * children refetch.
 */
export const QueryErrorBoundary: React.FC<QueryErrorBoundaryProps> = ({
  children,
  fallback = defaultFallback,
  resetKeys,
}) => (
  <QueryErrorResetBoundary>
    {({ reset }) => (
      <ErrorBoundary
        onReset={reset}
        resetKeys={resetKeys}
        fallbackRender={({ error, reset: clearBoundary }) =>
          fallback({
            error,
            retry: () => {
              reset();
              clearBoundary();
            },
          })
        }
      >
        {children}
      </ErrorBoundary>
    )}
  </QueryErrorResetBoundary>
);

export default QueryErrorBoundary;
