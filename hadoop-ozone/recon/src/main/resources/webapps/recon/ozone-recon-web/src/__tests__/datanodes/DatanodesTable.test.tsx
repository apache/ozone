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
import {vi} from 'vitest';
import {fireEvent, render, screen} from '@testing-library/react';

import {DatanodeTableProps} from '@/v2/types/datanode.types';
import DatanodesTable from '@/v2/components/tables/datanodesTable';
import {datanodeServer} from '@tests/mocks/datanodeMocks/datanodeServer';
import {waitForDNTable} from '@tests/utils/datanodes.utils';

const defaultProps: DatanodeTableProps = {
  loading: false,
  selectedRows: [],
  data: [],
  decommissionUuids: [],
  searchColumn: 'hostname',
  searchTerm: '',
  selectedColumns: [
    { label: 'Hostname', value: 'hostname' },
    { label: 'State', value: 'state' },
  ],
  handleSelectionChange: vi.fn(),
};

function getDataWith(name: string, state: "HEALTHY" | "STALE" | "DEAD", uuid: number) {
  return {
    hostname: name,
    uuid: uuid,
    state: state,
    opState: 'IN_SERVICE',
    lastHeartbeat: 1728280581608,
    storageReport: {
      capacity: 125645656770,
      used: 4096,
      remaining: 114225606656,
      committed: 0,
      filesystemCapacity: 150000000000,
      filesystemUsed: 30000000000,
      filesystemAvailable: 120000000000
    },
    storageUsed: 4096,
    storageTotal: 125645656770,
    storageCommitted: 0,
    storageRemaining: 114225606656,
    pipelines: [
      {
          "pipelineID": "0f9f7bc0-505e-4428-b148-dd7eac2e8ac2",
          "replicationType": "RATIS",
          "replicationFactor": "THREE",
          "leaderNode": "ozone-datanode-3.ozone_default"
      },
      {
          "pipelineID": "2c23e76e-3f18-4b86-9541-e48bdc152fda",
          "replicationType": "RATIS",
          "replicationFactor": "ONE",
          "leaderNode": "ozone-datanode-1.ozone_default"
      }
    ],
    containers: 8192,
    openContainers: 8182,
    leaderCount: 2,
    version: '0.6.0-SNAPSHOT',
    setupTime: 1728280539733,
    revision: '3f9953c0fbbd2175ee83e8f0b4927e45e9c10ac1',
    buildDate: '2024-10-06T16:41Z',
    networkLocation: '/default-rack'
  }
}

describe('DatanodesTable Component', () => {
  // Start and stop MSW server before and after all tests
  beforeAll(() => datanodeServer.listen());
  afterEach(() => datanodeServer.resetHandlers());
  afterAll(() => datanodeServer.close());

  test('renders table with data', async () => {
    render(<DatanodesTable {...defaultProps} data={[]} />);

    // Wait for the table to render
    waitForDNTable();

    expect(screen.getByTestId('dn-table')).toBeInTheDocument();
  });

  test('filters data based on search term', async () => {
    render(
      <DatanodesTable
        {...defaultProps}
        searchTerm="ozone-datanode-1"
        data={[
          getDataWith('ozone-datanode-1', 'HEALTHY', 1),
          getDataWith('ozone-datanode-2', 'STALE', 2)
        ]}
      />
    );

    // Only the matching datanode should be visible
    expect(screen.getByText('ozone-datanode-1')).toBeInTheDocument();
    expect(screen.queryByText('ozone-datanode-2')).not.toBeInTheDocument();
  });

  test('handles row selection', async () => {
    render(
      <DatanodesTable
        {...defaultProps}
        data={[
          getDataWith('ozone-datanode-1', 'HEALTHY', 1),
          getDataWith('ozone-datanode-2', 'DEAD', 2)
        ]}
      />
    );

    // The first checkbox is for the table header "Select All" checkbox -> idx 0
    // Second checkbox is for the healthy DN                            -> idx 1
    // Third checkbox is the active one for Dead DN                     -> idx 2
    const checkbox = document.querySelectorAll('input.ant-checkbox-input')[2];
    fireEvent.click(checkbox);

    expect(defaultProps.handleSelectionChange).toHaveBeenCalledWith([2]);
  });

  test('disables selection for non-DEAD nodes', async () => {
    render(
      <DatanodesTable
        {...defaultProps}
        data={[
          getDataWith('ozone-datanode-1', 'HEALTHY', 1),
          getDataWith('ozone-datanode-2', 'DEAD', 2)
        ]}
      />
    );

    // Check disabled and enabled rows
    const checkboxes = document.querySelectorAll('input.ant-checkbox-input');
    expect(checkboxes[1]).toBeDisabled(); // HEALTHY node
    expect(checkboxes[2]).not.toBeDisabled(); // DEAD node
  });

  test('shows filesystem rows in storage tooltip when provided', async () => {
    render(
      <DatanodesTable
        {...defaultProps}
        data={[
          getDataWith('ozone-datanode-1', 'HEALTHY', 1)
        ]}
      />
    );

    const storageBar = document.querySelector('.capacity-bar-v2');
    expect(storageBar).not.toBeNull();
    fireEvent.mouseOver(storageBar as HTMLElement);

    expect(await screen.findByText('Filesystem Capacity')).toBeInTheDocument();
    expect(screen.getByText('Filesystem Used')).toBeInTheDocument();
    expect(screen.getByText('Filesystem Available')).toBeInTheDocument();
  });
});
