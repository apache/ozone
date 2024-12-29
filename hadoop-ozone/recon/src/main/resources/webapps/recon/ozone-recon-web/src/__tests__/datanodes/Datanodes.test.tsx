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
import {
  render,
  screen,
  fireEvent,
  waitFor,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { rest } from "msw";
import { vi } from 'vitest';

import Datanodes from '@/v2/pages/datanodes/datanodes';
import * as commonUtils from '@/utils/common';
import { datanodeServer } from '@/__tests__/mocks/datanodeMocks/datanodeServer';
import DatanodesTable from '@/v2/components/tables/datanodesTable';

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
    expect(screen.getByTestId('search-input')).toBeInTheDocument();
  });

  test('loads data on mount', async () => {
    render(<Datanodes />);
    // Wait for the data to be loaded into the table
    const dnTable = await waitFor(() => screen.getByTestId('dn-table'));

    // Ensure the correct data is displayed in the table
    expect(dnTable).toHaveTextContent('ozone-datanode-1.ozone_default');
    expect(dnTable).toHaveTextContent('HEALTHY');
  });

  test('handles search input change', async () => {
    render(<Datanodes />);
    await waitFor(() => screen.getByTestId('dn-table'));

    const searchInput = screen.getByTestId('search-input');
    userEvent.type(searchInput, 'ozone-datanode-1');
    await waitFor(() => expect(searchInput).toHaveValue('ozone-datanode-1'));
  });

  // Since this is a static response, even if we remove we will not get the truncated response from backend
  // i.e response with the removed DN. So the table will always have the value even if we remove it
  // causing this test to fail
  test.skip('shows modal on row selection and confirms removal', async () => {
    render(<Datanodes />);

    // Wait for the data to be loaded into the table
    await waitFor(() => screen.getByTestId('dn-table'));

    // Simulate selecting a row
    // The first checkbox is for the table header "Select All" checkbox -> idx 0
    // Second checkbox is for the healthy DN                            -> idx 1
    // Third checkbox is the active one for Dead DN                     -> idx 2
    const checkbox = document.querySelectorAll('input.ant-checkbox-input');
    userEvent.click(checkbox[0]);
    // Click the "Remove" button to open the modal
    await waitFor(() => {
      // Wait for the button to appear in screen
      screen.getByText(/Remove/);
    }).then(() => {
      userEvent.click(screen.getByText(/Remove/));
    })

    // Confirm removal in the modal
    await waitFor(() => {
      // Wait for the button to appear in screen
      screen.getByText(/OK/);
    }).then(() => {
      userEvent.click(screen.getByText(/OK/));
    })

    // Wait for the removal operation to complete
    await waitFor(() =>
      expect(screen.queryByText('ozone-datanode-3.ozone_default')).not.toBeInTheDocument()
    );
  });

  test('handles API errors gracefully', async () => {
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