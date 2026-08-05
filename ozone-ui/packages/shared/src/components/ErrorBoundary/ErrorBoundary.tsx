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

export interface ErrorBoundaryProps {
  /** Render the fallback UI from the caught error and a `reset` callback. */
  fallbackRender: (args: { error: Error; reset: () => void }) => React.ReactNode;
  /** Called when the boundary resets (e.g. to also reset query caches). */
  onReset?: () => void;
  /**
   * When any value in this array changes while an error is shown, the boundary
   * clears itself automatically (e.g. reset on route change).
   */
  resetKeys?: unknown[];
  children: React.ReactNode;
}

interface ErrorBoundaryState {
  error: Error | null;
}

/**
 * Minimal React error boundary (no external dependency). Catches render-time
 * errors — including those thrown by `useSuspenseQuery` — and renders a fallback
 * with a `reset` handler. Compose with `QueryErrorBoundary` to also reset the
 * query cache on retry.
 */
export class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidUpdate(prev: ErrorBoundaryProps) {
    if (this.state.error && prev.resetKeys !== this.props.resetKeys) {
      const changed =
        (prev.resetKeys?.length ?? 0) !== (this.props.resetKeys?.length ?? 0) ||
        (this.props.resetKeys ?? []).some((key, i) => !Object.is(key, prev.resetKeys?.[i]));
      if (changed) {
        this.reset();
      }
    }
  }

  reset = () => {
    this.props.onReset?.();
    this.setState({ error: null });
  };

  render() {
    if (this.state.error) {
      return this.props.fallbackRender({ error: this.state.error, reset: this.reset });
    }
    return this.props.children;
  }
}

export default ErrorBoundary;
