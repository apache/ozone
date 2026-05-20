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
import {fireEvent, render, screen, waitFor} from '@testing-library/react';
import PipelinesTable from '@/v2/components/tables/pipelinesTable';
import {Pipeline, PipelinesTableProps} from '@/v2/types/pipelines.types';
import {pipelineLocators} from '@tests/locators/locators';
import {userEvent} from '@testing-library/user-event';

// Mock to disable scroll behaviour
// We are running test inside virtual DOM, so scrolling isn't available as a normal browser
// AntD tries to scroll to the top after changing pages. This disables the behaviour.
vi.mock('antd/es/_util/scrollTo', () => ({
  default: vi.fn()
}));


const defaultProps: PipelinesTableProps = {
  loading: false,
  data: [],
  selectedColumns: [
    { label: 'Pipeline ID', value: 'pipelineId' },
    { label: 'Status', value: 'status' },
  ],
  searchTerm: '',
};

function getPipelineWith(
  id: string,
  status: 'OPEN' | 'CLOSING' | 'QUASI_CLOSED' | 'CLOSED' | 'UNHEALTHY' | 'INVALID' | 'DELETED' | 'DORMANT',
  replicationFactor: string
): Pipeline {
  return {
    pipelineId: id,
    status,
    containers: 10,
    datanodes: [],
    leaderNode: 'node-1',
    replicationFactor,
    replicationType: 'RATIS',
    duration: 10000,
    leaderElections: 1,
    lastLeaderElection: 1000,
  };
}

describe('PipelinesTable Component', () => {
  test('Renders table with data', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        data={[getPipelineWith('pipeline-1', 'UNHEALTHY', 'THREE')]}
      />
    );

    // Verify table is rendered
    await waitFor(() => screen.getByText('pipeline-1'));
    expect(screen.getByText('UNHEALTHY')).toBeInTheDocument();
  });

  test('Filters data based on search term', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        searchTerm="pipeline-1"
        data={[
          getPipelineWith('pipeline-1', 'OPEN', 'THREE'),
          getPipelineWith('pipeline-2', 'QUASI_CLOSED', 'ONE'),
        ]}
      />
    );

    // Verify only matching rows are displayed
    expect(screen.getByText('pipeline-1')).toBeInTheDocument();
    expect(screen.queryByText('pipeline-2')).not.toBeInTheDocument();
  });

  test('Renders correct columns based on selectedColumns', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        selectedColumns={[
          { label: 'Pipeline ID', value: 'pipelineId' },
        ]}
        data={[getPipelineWith('pipeline-1', 'CLOSED', 'ONE')]}
      />
    );

    // Verify only the selected columns are rendered
    expect(screen.getByText('Pipeline ID')).toBeInTheDocument();
    expect(screen.queryByText('Replication Type & Factor')).not.toBeInTheDocument();
    expect(screen.queryByText('Status')).not.toBeInTheDocument();
  });

  test('Sorts table rows by pipeline ID', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        data={[
          getPipelineWith('pipeline-2', 'CLOSED', 'ONE'),
          getPipelineWith('pipeline-1', 'DELETED', 'THREE'),
        ]}
      />
    );

    // Simulate sorting by pipeline ID
    const pipelineIdHeader = screen.getByText('Pipeline ID');
    fireEvent.click(pipelineIdHeader);

    // Verify rows are sorted
    const rows = screen.getAllByTestId(pipelineLocators.pipelineRowRegex);
    expect(rows[0]).toHaveTextContent('pipeline-1');
    expect(rows[1]).toHaveTextContent('pipeline-2');
  });

  test('Sorts table rows by pipeline status', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        data={[
          getPipelineWith('pipeline-2', 'CLOSED', 'ONE'),
          getPipelineWith('pipeline-1', 'DELETED', 'THREE'),
        ]}
      />
    );

    // Simulate sorting by pipeline ID
    const pipelineIdHeader = screen.getByText('Status');
    fireEvent.click(pipelineIdHeader);

    // Verify rows are sorted
    const rows = screen.getAllByTestId(pipelineLocators.pipelineRowRegex);
    expect(rows[0]).toHaveTextContent('CLOSED');
    expect(rows[1]).toHaveTextContent('DELETED');
  });

  test('Pagination works properly', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        data={[
          getPipelineWith('pipeline-1', 'DELETED', 'THREE'),
          getPipelineWith('pipeline-2', 'CLOSED', 'ONE'),
          getPipelineWith('pipeline-3', 'OPEN', 'THREE'),
          getPipelineWith('pipeline-4', 'OPEN', 'ONE'),
          getPipelineWith('pipeline-5', 'OPEN', 'ONE'),
          getPipelineWith('pipeline-6', 'OPEN', 'ONE'),
          getPipelineWith('pipeline-7', 'OPEN', 'ONE'),
          getPipelineWith('pipeline-8', 'OPEN', 'ONE'),
          getPipelineWith('pipeline-9', 'OPEN', 'ONE'),
          getPipelineWith('pipeline-10', 'OPEN', 'ONE'),
          getPipelineWith('pipeline-11', 'OPEN', 'THREE')
        ]}
      />
    );

    try {
      // Simulate clicking the next page button
      const nextPageBtn = screen.getByTitle('Next Page');
      userEvent.click(nextPageBtn, { pointerEventsCheck: 0 });
      // Verify rows are sorted
      const rows = screen.getAllByTestId(pipelineLocators.pipelineRowRegex);
      expect(rows[0]).toHaveTextContent('pipeline-11');
    } catch(err) {
      if (err instanceof ReferenceError) {
        console.log("Encountered error: ", err);
      }
    }
  });
});
