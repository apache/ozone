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
import userEvent from '@testing-library/user-event';
import {rest} from "msw";
import {vi} from 'vitest';

import Datanodes from '@/v2/pages/datanodes/datanodes';
import * as commonUtils from '@/utils/common';
import {datanodeServer} from '@tests/mocks/datanodeMocks/datanodeServer';
import {datanodeLocators, searchInputLocator} from '@tests//locators/locators';
import {waitForDNTable} from '@tests/utils/datanodes.utils';

// Mock utility functions
vi.spyOn(commonUtils, 'showDataFetchError');

vi.mock('@/components/autoReloadPanel/autoReloadPanel', () => ({
  default: () => <div data-testid="auto-reload-panel" />,
}));
vi.mock('@/v2/components/select/multiSelect.tsx', () => ({
  default: ({ onChange }: { onChange: Function }) => (
    <select data-testid="multi-select" onChange={(e) => onChange(e.target.value)}>
      <option value="hostname">Hostname</option>
      <option value="uuid">UUID</option>
    </select>
  ),
}));

describe('Datanodes Component', () => {
  // Start and stop MSW server before and after all tests
  beforeAll(() => datanodeServer.listen());
  afterEach(() => datanodeServer.resetHandlers());
  afterAll(() => datanodeServer.close());

  test('renders component correctly', () => {
    render(<Datanodes />);

    expect(screen.getByText(/Datanodes/)).toBeInTheDocument();
    expect(screen.getByTestId('auto-reload-panel')).toBeInTheDocument();
    expect(screen.getByTestId('multi-select')).toBeInTheDocument();
    expect(screen.getByTestId(searchInputLocator)).toBeInTheDocument();
  });

  test('Renders table with correct number of rows', async () => {
    render(<Datanodes />);

    // Wait for the data to load
    const rows = await waitFor(() => screen.getAllByTestId(datanodeLocators.datanodeRowRegex));
    expect(rows).toHaveLength(5); // Based on the mocked DatanodeResponse
  });

  test('Loads data on mount', async () => {
    render(<Datanodes />);
    // Wait for the data to be loaded into the table
    const dnTable = await waitForDNTable();

    // Ensure the correct data is displayed in the table
    expect(dnTable).toHaveTextContent('ozone-datanode-1.ozone_default');
    expect(dnTable).toHaveTextContent('HEALTHY');
  });

  test('Displays no data message if the datanodes API returns an empty array', async () => {
    datanodeServer.use(
      rest.get('api/v1/datanodes', (req, res, ctx) => {
        return res(ctx.status(200), ctx.json({ totalCount: 0, datanodes: [] }));
      })
    );

    render(<Datanodes />);

    // Wait for the no data message
    await waitFor(() => expect(screen.getByText('No Data')).toBeInTheDocument());
  });

  test('Handles search input change', async () => {
    render(<Datanodes />);
    await waitForDNTable();

    const searchInput = screen.getByTestId(searchInputLocator);
    fireEvent.change(searchInput, {
      target: { value: 'ozone-datanode-1' }
    });
    // Sleep for 310ms to allow debounced search to take effect
    await new Promise((r) => { setTimeout(r, 310) });
    const rows = await waitFor(() => screen.getAllByTestId(datanodeLocators.datanodeRowRegex));
    await waitFor(() => expect(rows).toHaveLength(1));
  });

  test('Handles case-sensitive search', async () => {
    render(<Datanodes />);
    await waitForDNTable();

    const searchInput = screen.getByTestId(searchInputLocator);
    fireEvent.change(searchInput, {
      target: { value: 'DataNode' }
    });
    await waitFor(() => expect(searchInput).toHaveValue('DataNode'));
    // Sleep for 310ms to allow debounced search to take effect
    await new Promise((r) => { setTimeout(r, 310) })

    const rows = await waitFor(() => screen.getAllByTestId(datanodeLocators.datanodeRowRegex));
    expect(rows).toHaveLength(1);
  })

  test('Displays a message when no results match the search term', async () => {
    render(<Datanodes />);
    const searchInput = screen.getByTestId(searchInputLocator);

    // Type a term that doesn't match any datanode
    fireEvent.change(searchInput, {
      target: { value: 'nonexistent-datanode' }
    });

    // Verify that no results message is displayed
    await waitFor(() => expect(screen.getByText('No Data')).toBeInTheDocument());
  });

  // Since this is a static response, even if we remove we will not get the truncated response from backend
  // i.e response with the removed DN. So the table will always have the value even if we remove it
  // causing this test to fail
  test.skip('Shows modal on row selection and confirms removal', async () => {
    render(<Datanodes />);

    // Wait for the data to be loaded into the table
    await waitForDNTable();

    // Simulate selecting a row
    // The first checkbox is for the table header "Select All" checkbox -> idx 0
    // Second checkbox is for the healthy DN                            -> idx 1
    // Third checkbox is the active one for Dead DN                     -> idx 2
    const checkbox = document.querySelectorAll('input.ant-checkbox-input');
    userEvent.click(checkbox[0]);
    // Click the "Remove" button to open the modal
    await waitFor(() => {
      // Wait for the button to appear in screen
      screen.getByTestId(datanodeLocators.datanodeRemoveButton);
    }).then(() => {
      userEvent.click(screen.getByText(/Remove/));
    })

    // Confirm removal in the modal
    await waitFor(() => {
      // Wait for the button to appear in screen
      screen.getByTestId(datanodeLocators.datanodeRemoveModal);
    }).then(() => {
      userEvent.click(screen.getByText(/OK/));
    })

    // Wait for the removal operation to complete
    await waitFor(() =>
      expect(screen.queryByText('ozone-datanode-3.ozone_default')).not.toBeInTheDocument()
    );
  });

  test('Handles API errors gracefully by showing error message', async () => {
    // Set up MSW to return an error for the datanode API
    datanodeServer.use(
      rest.get('api/v1/datanodes', (req, res, ctx) => {
        return res(ctx.status(500), ctx.json({ error: 'Internal Server Error' }));
      })
    );

    render(<Datanodes />);

    // Wait for the error to be handled
    await waitFor(() =>
      expect(commonUtils.showDataFetchError).toHaveBeenCalledWith('AxiosError: Request failed with status code 500')
    );
  });
});
