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

import Capacity from '@/v2/pages/capacity/capacity';
import { capacityServer } from '@tests/mocks/capacityMocks/capacityServer';

vi.mock('@/components/autoReloadPanel/autoReloadPanel', () => ({
  default: () => <div data-testid="auto-reload-panel" />,
}));
vi.mock('@/components/eChart/eChart', () => ({
  EChart: () => <div data-testid="echart" />,
}));

describe('Capacity Page', () => {
  beforeAll(() => capacityServer.listen());
  afterEach(() => capacityServer.resetHandlers());
  afterAll(() => capacityServer.close());

  test('renders cluster and service breakdown with data', async () => {
    render(<Capacity />);

    expect(screen.getByText('Cluster Capacity')).toBeInTheDocument();
    expect(screen.getByTestId('auto-reload-panel')).toBeInTheDocument();

    const ozoneCapacityTitle = await screen.findByText('Ozone Capacity');
    const ozoneCapacityCard = ozoneCapacityTitle.closest('.ant-card');
    expect(ozoneCapacityCard).not.toBeNull();
    if (!ozoneCapacityCard) {
      return;
    }
    await waitFor(() =>
      expect(ozoneCapacityCard).toHaveTextContent(/TOTAL\s*10\s*KB/i)
    );
    expect(ozoneCapacityCard).toHaveTextContent(/OZONE USED SPACE\s*4\s*KB/i);
    expect(ozoneCapacityCard).toHaveTextContent(/OTHER USED SPACE\s*2\s*KB/i);
    expect(ozoneCapacityCard).toHaveTextContent(/CONTAINER PRE-ALLOCATED\s*1\s*KB/i);
    expect(ozoneCapacityCard).toHaveTextContent(/REMAINING SPACE\s*4\s*KB/i);

    const ozoneUsedSpaceTitle = screen.getByText('Ozone Used Space');
    const ozoneUsedSpaceCard = ozoneUsedSpaceTitle.closest('.ant-card');
    expect(ozoneUsedSpaceCard).not.toBeNull();
    if (!ozoneUsedSpaceCard) {
      return;
    }
    await waitFor(() =>
      expect(ozoneUsedSpaceCard).toHaveTextContent(/PENDING DELETION\s*6\s*KB/i)
    );
  });

  test('shows pending deletion and datanode detail values', async () => {
    render(<Capacity />);

    const pendingDeletionTitle = await screen.findByText('Pending Deletion');
    const pendingDeletionCard = pendingDeletionTitle.closest('.ant-card');
    expect(pendingDeletionCard).not.toBeNull();
    if (!pendingDeletionCard) {
      return;
    }
    await waitFor(() =>
      expect(pendingDeletionCard).toHaveTextContent(/OZONE MANAGER\s*2\s*KB/i)
    );
    expect(pendingDeletionCard)
      .toHaveTextContent(/STORAGE CONTAINER MANAGER\s*1\s*KB/i);
    expect(pendingDeletionCard).toHaveTextContent(/DATANODES\s*3\s*KB/i);

    const downloadLink = await screen.findByText('Download Insights');
    const datanodeCard = downloadLink.closest('.ant-card');
    expect(datanodeCard).not.toBeNull();
    if (!datanodeCard) {
      return;
    }
    await waitFor(() =>
      expect(datanodeCard).toHaveTextContent(/USED SPACE\s*5\s*KB/i)
    );
    expect(datanodeCard).toHaveTextContent(/FREE SPACE\s*3\s*KB/i);
  });
});
