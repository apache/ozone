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
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { vi } from 'vitest';
import { Modal } from 'antd';

import BucketPendingDeletionTable from '@/v2/components/tables/insights/bucketPendingDeletionTable';
import { BucketPendingDeletion, SnapshotTrappedBytesJob } from '@/v2/types/insights.types';

vi.mock('@/v2/components/select/singleSelect', () => ({
  default: () => <div data-testid='limit-select' />,
}));

const BUCKETS: BucketPendingDeletion[] = [{
  volumeName: 'vol1',
  bucketName: 'bucket1',
  pendingDeletionKeys: 10,
  pendingDeletionKeyBytes: 2048,
  pendingDeletionDirs: 1,
  pendingDeletionDirBytes: 1024,
  totalPendingDeletionBytes: 3072
}];

const QUEUED_JOB: SnapshotTrappedBytesJob = {
  jobId: 'job-1',
  volumeName: 'vol1',
  bucketName: 'bucket1',
  status: 'QUEUED',
  submittedAt: 1730000000000,
  startedAt: 0,
  completedAt: 0,
  errorMessage: null,
  result: null
};

const COMPLETED_JOB: SnapshotTrappedBytesJob = {
  ...QUEUED_JOB,
  status: 'COMPLETED',
  startedAt: 1730000001000,
  completedAt: 1730000002000,
  result: {
    snapshotTrappedBytes: 2048,
    purgeableBytes: 1024,
    snapshotTrappedKeys: 6,
    purgeableKeys: 4,
    snapshotTrappedDirs: 0,
    purgeableDirs: 1
  }
};

const submittedRequests: string[] = [];

const server = setupServer(
  http.get('/api/v1/keys/deletePending/buckets', () => HttpResponse.json(BUCKETS)),
  http.get('/api/v1/snapshotTrappedBytes', () => HttpResponse.json([])),
  http.post('/api/v1/snapshotTrappedBytes', ({ request }) => {
    submittedRequests.push(new URL(request.url).search);
    return HttpResponse.json(QUEUED_JOB, { status: 202 });
  }),
  http.get('/api/v1/snapshotTrappedBytes/job-1', () => HttpResponse.json(COMPLETED_JOB))
);

function renderTable() {
  return render(
    <BucketPendingDeletionTable
      limit={{ label: '1000', value: '1000' }}
      paginationConfig={{}}
      handleLimitChange={vi.fn()} />
  );
}

describe('BucketPendingDeletionTable', () => {
  beforeAll(() => server.listen());
  afterEach(() => {
    // Confirm dialogs are rendered outside the test container, so they are not cleaned up with it
    Modal.destroyAll();
    server.resetHandlers();
    submittedRequests.length = 0;
  });
  afterAll(() => server.close());

  test('renders pending deletion per bucket', async () => {
    renderTable();
    expect(await screen.findByText('bucket1')).toBeInTheDocument();
    expect(screen.getByText('vol1')).toBeInTheDocument();
    expect(screen.getByText('3 KB')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Calculate' })).toBeInTheDocument();
  });

  test('warns before calculating, then polls the job until the result is shown', async () => {
    renderTable();
    fireEvent.click(await screen.findByRole('button', { name: 'Calculate' }));

    // Nothing is submitted until the user confirms the warning
    expect(await screen.findByText(/This is an expensive operation/)).toBeInTheDocument();
    expect(submittedRequests).toHaveLength(0);

    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Calculate' }));
    await waitFor(() => expect(submittedRequests).toEqual(['?volume=vol1&bucket=bucket1']));
    expect(await screen.findByText('Queued')).toBeInTheDocument();

    const result = await screen.findByTestId('snapshot-trapped-bytes-vol1/bucket1', {}, { timeout: 6000 });
    expect(result).toHaveTextContent('2 KB');
  }, 10000);

  test('does not submit when the warning is cancelled', async () => {
    renderTable();
    fireEvent.click(await screen.findByRole('button', { name: 'Calculate' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Cancel' }));

    // Give a submission, if any, the chance to happen before asserting there was none
    await new Promise(resolve => setTimeout(resolve, 200));
    expect(submittedRequests).toHaveLength(0);
    expect(within(screen.getByRole('table')).getByRole('button', { name: 'Calculate' })).toBeInTheDocument();
  });

  test('shows the result of a recent calculation without recalculating', async () => {
    server.use(http.get('/api/v1/snapshotTrappedBytes', () => HttpResponse.json([COMPLETED_JOB])));
    renderTable();

    const result = await screen.findByTestId('snapshot-trapped-bytes-vol1/bucket1');
    expect(result).toHaveTextContent('2 KB');
    expect(screen.getByTestId('snapshotTrappedKeys-vol1/bucket1')).toHaveTextContent('6');
    expect(screen.getByTestId('snapshotTrappedDirs-vol1/bucket1')).toHaveTextContent('0');
    expect(screen.getByTestId('purgeableBytes-vol1/bucket1')).toHaveTextContent('1 KB');
    expect(screen.getByTestId('purgeableKeys-vol1/bucket1')).toHaveTextContent('4');
    expect(screen.getByTestId('purgeableDirs-vol1/bucket1')).toHaveTextContent('1');
    expect(within(screen.getByRole('table')).queryByRole('button', { name: 'Calculate' })).not.toBeInTheDocument();
    expect(submittedRequests).toHaveLength(0);
  });
});
