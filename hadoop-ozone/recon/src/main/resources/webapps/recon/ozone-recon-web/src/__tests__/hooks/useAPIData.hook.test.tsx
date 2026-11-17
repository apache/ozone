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

import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';

import { useApiData } from '@/v2/hooks/useAPIData.hook';

const RETRY_TEST_URL = '/api/v1/use-api-data-retry-test';

let requestCount = 0;

const retryTestServer = setupServer(
  http.get(RETRY_TEST_URL, () => {
    requestCount++;
    return HttpResponse.json({ message: 'Internal Server Error' }, { status: 500 });
  })
);

function RetryProbe({ onError }: { onError: (error: unknown) => void }) {
  const { loading, error } = useApiData(RETRY_TEST_URL, null, {
    retryAttempts: 2,
    retryDelay: 25,
    onError,
  });

  return (
    <div>
      <span data-testid="loading">{String(loading)}</span>
      <span data-testid="error">{error ?? ''}</span>
    </div>
  );
}

describe('useApiData', () => {
  describe('retry on 5xx', () => {
    beforeAll(() => retryTestServer.listen({ onUnhandledRequest: 'error' }));
    afterEach(() => {
      retryTestServer.resetHandlers();
      requestCount = 0;
    });
    afterAll(() => retryTestServer.close());

    test('retries until attempts are exhausted and invokes onError', async () => {
      const onError = vi.fn();
      render(<RetryProbe onError={onError} />);

        await waitFor(
          () => {
            expect(onError).toHaveBeenCalledTimes(1);
          },
          { timeout: 3000 }
        );

        expect(requestCount).toBe(3);
        expect(screen.getByTestId('loading')).toHaveTextContent('false');
        expect(screen.getByTestId('error').textContent).not.toBe('');
    });
  });
});
