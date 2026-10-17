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
import { cleanup, render, screen, waitFor, within } from '@testing-library/react';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';

import NUMetadata from '@/v2/components/nuMetadata/nuMetadata';
import { showDataFetchError } from '@/utils/common';

vi.mock('@/utils/common', async () => ({
  ...await vi.importActual('@/utils/common'),
  showDataFetchError: vi.fn()
}));

const server = setupServer(
  http.get('/api/v1/namespace/summary', () => HttpResponse.json({
    status: 'OK',
    type: 'ROOT',
    objectInfo: {},
    countStats: {}
  }))
);

describe('Namespace Usage metadata', () => {
  beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
  afterEach(() => {
    cleanup();
    server.resetHandlers();
    vi.clearAllMocks();
  });
  afterAll(() => server.close());

  test('hides quota rows and reports initialization when both responses are initializing', async () => {
    server.use(
      http.get('/api/v1/namespace/summary', () => HttpResponse.json({ status: 'INITIALIZING' })),
      http.get('/api/v1/namespace/quota', () => HttpResponse.json({
        status: 'INITIALIZING', allowed: 0, used: 0
      }))
    );

    render(<NUMetadata path='/' />);

    await waitFor(() => expect(showDataFetchError).toHaveBeenCalledWith(
      'The metadata is currently initializing. Please wait a moment and try again later'
    ));
    expect(screen.queryByText('Quota Allowed')).not.toBeInTheDocument();
    expect(screen.queryByText('Quota Used')).not.toBeInTheDocument();
    expect(showDataFetchError).toHaveBeenCalledWith(
      'The quota is currently initializing. Please wait a moment and try again later'
    );
  });

  test.each([
    ['INITIALIZING', 0, 0],
    ['TYPE_NOT_APPLICABLE', 0, 0],
    ['PATH_NOT_FOUND', 0, 0],
    ['OK', -1, -1]
  ])('hides quota rows for status %s with allowed %s and used %s', async (status, allowed, used) => {
    server.use(http.get('/api/v1/namespace/quota', () => HttpResponse.json({ status, allowed, used })));

    render(<NUMetadata path='/' />);

    if (status === 'PATH_NOT_FOUND') {
      await waitFor(() => expect(showDataFetchError).toHaveBeenCalledWith('Invalid Path: /'));
    } else {
      await screen.findByText('ROOT');
    }
    expect(screen.queryByText('Quota Allowed')).not.toBeInTheDocument();
    expect(screen.queryByText('Quota Used')).not.toBeInTheDocument();
  });

  test.each([
    [0, 0, '0 Bytes', '0 Bytes'],
    [1024, 512, '1 KB', '512 Bytes']
  ])('shows valid quota values including zero: allowed %s and used %s', async (allowed, used, allowedText, usedText) => {
    server.use(http.get('/api/v1/namespace/quota', () => HttpResponse.json({ status: 'OK', allowed, used })));

    render(<NUMetadata path='/' />);

    const allowedLabel = await screen.findByText('Quota Allowed');
    const usedLabel = await screen.findByText('Quota Used');
    expect(within(allowedLabel.closest('tr')!).getByText(allowedText)).toBeInTheDocument();
    expect(within(usedLabel.closest('tr')!).getByText(usedText)).toBeInTheDocument();
    expect(showDataFetchError).not.toHaveBeenCalled();
  });
});
