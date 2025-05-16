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
import {rest} from 'msw';
import {vi} from 'vitest';

import Pipelines from '@/v2/pages/pipelines/pipelines';
import * as commonUtils from '@/utils/common';
import {pipelineServer} from '@tests/mocks/pipelineMocks/pipelinesServer';
import {pipelineLocators, searchInputLocator} from '@tests/locators/locators';
import {waitForPipelineTable} from '@tests/utils/pipelines.utils';

// Mock utility functions
vi.spyOn(commonUtils, 'showDataFetchError');

vi.mock('@/components/autoReloadPanel/autoReloadPanel', () => ({
  default: () => <div data-testid="auto-reload-panel" />,
}));
vi.mock('@/v2/components/select/multiSelect.tsx', () => ({
  default: ({ onChange }: { onChange: Function }) => (
    <select data-testid="multi-select" onChange={(e) => onChange(e.target.value)}>
      <option value="pipelineId">Pipeline ID</option>
      <option value="replicationType">Replication Type & Factor</option>
      <option value="status">Status</option>
    </select>
  ),
}));

describe('Pipelines Component', () => {
  // Start and stop MSW server before and after all tests
  beforeAll(() => pipelineServer.listen());
  afterEach(async () => pipelineServer.resetHandlers());
  afterAll(() => pipelineServer.close());

  test('renders component correctly', () => {
    render(<Pipelines />);

    // Verify core components are rendered
    expect(screen.getByText(/Pipelines/)).toBeInTheDocument();
    expect(screen.getByTestId('auto-reload-panel')).toBeInTheDocument();
    expect(screen.getByTestId('multi-select')).toBeInTheDocument();
    expect(screen.getByTestId(searchInputLocator)).toBeInTheDocument();
  });

  test('Loads data on mount', async () => {
    render(<Pipelines />);

    // Wait for the data to load into the table
    const table = await waitForPipelineTable();
    expect(table).toBeInTheDocument();

    // Verify data is displayed, from pipelineMocks
    expect(table).toHaveTextContent('pipeline-1');
    expect(table).toHaveTextContent('CLOSED');
  });

  test('Renders table with correct number of rows', async () => {
    render(<Pipelines />);

    // Wait for the data to load into the table
    const rows = await waitFor(() => screen.getAllByTestId(pipelineLocators.pipelineRowRegex));
    expect(rows).toHaveLength(3); // Based on the mocked Pipeline response
  });

  test('Displays no data message if the pipelines API returns an empty array', async () => {
    pipelineServer.use(
      rest.get('api/v1/pipelines', (req, res, ctx) => {
        return res(ctx.status(200), ctx.json({ totalCount: 0, pipelines: [] }));
      })
    );

    render(<Pipelines />);

    // Wait for the no data message
    await waitFor(() => expect(screen.getByText('No Data')).toBeInTheDocument());
  });

  test('Handles search input change', async () => {
    render(<Pipelines />);
    await waitForPipelineTable();

    const searchInput = screen.getByTestId(searchInputLocator);
    fireEvent.change(searchInput, {
      target: { value: 'pipeline-1' }
    });
    // Sleep for 310ms to allow debounced search to take effect
    await new Promise((r) => { setTimeout(r, 310) });
    const rows = await waitFor(() => screen.getAllByTestId(pipelineLocators.pipelineRowRegex));
    await waitFor(() => expect(rows).toHaveLength(1));
  });

  test('Displays a message when no results match the search term', async () => {
    render(<Pipelines />);
    const searchInput = screen.getByTestId(searchInputLocator);

    // Type a term that doesn't match any datanode
    fireEvent.change(searchInput, {
      target: { value: 'nonexistent-pipeline' }
    });

    // Verify that no results message is displayed
    await waitFor(() => expect(screen.getByText('No Data')).toBeInTheDocument());
  });

  test('Handles API errors gracefully by showing error message', async () => {
    // Set up MSW to return an error for the datanode API
    pipelineServer.use(
      rest.get('api/v1/pipelines', (req, res, ctx) => {
        return res(ctx.status(500), ctx.json({ error: 'Internal Server Error' }));
      })
    );

    render(<Pipelines />);

    // Wait for the error to be handled
    await waitFor(() =>
      expect(commonUtils.showDataFetchError).toHaveBeenCalledWith('AxiosError: Request failed with status code 500')
    );
  });
});
