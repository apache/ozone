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
import { BrowserRouter } from 'react-router-dom';

/**
 * The dont-cleanup-after-each is imported to prevent autoCleanup of rendered
 * component after each test.
 * Since we are needing to set a timeout everytime the component is rendered
 * and we would be verifying whether the UI is correct, we can skip the cleanup
 * and leave it for after all the tests run - saving test time
 */
import '@testing-library/react/dont-cleanup-after-each';
import { cleanup, render, screen } from '@testing-library/react';

import { overviewLocators } from '@tests/locators/locators';
import { faultyOverviewServer, overviewServer } from '@tests/mocks/overviewMocks/overviewServer';
import Overview from '@/v2/pages/overview/overview';

const WrappedOverviewComponent = () => {
  return (
    <BrowserRouter>
      <Overview />
    </BrowserRouter>
  )
}

/**
 * We need to mock the EChart component as in the virtual DOM
 * it cannot access the DOM height and width and will throw errors
 * Hence we intercept and mock the import to return an empty
 * React fragment
 */
vi.mock('@/v2/components/eChart/eChart', () => ({
  default: () => (<></>)
}))

describe.each([
  true,
  false
])('Overview Tests - Data is present = %s', (scenario) => {
  beforeAll(async () => {
    (scenario) ? overviewServer.listen() : faultyOverviewServer.listen();
    render(
      <WrappedOverviewComponent />
    );
    //Setting a timeout of 100ms to allow requests to be resolved and states to be set
    await new Promise((r) => { setTimeout(r, 100) })
  });

  afterAll(() => {
    (scenario) ? overviewServer.close() : faultyOverviewServer.close();
    vi.clearAllMocks();
    /**
     * Need to cleanup the DOM after one suite has completely run
     * Otherwise we will get duplicate elements
     */
    cleanup();
  })

  // Tests begin here
  // All the data is being mocked by MSW, so we have a fixed data that we can verify
  // the content against
  it('Datanode row has the correct count of Datanodes', () => {
    const datanodeRow = screen.getByTestId(overviewLocators.datanodeRow);
    expect(datanodeRow).toBeVisible();
    expect(datanodeRow).toHaveTextContent((scenario) ? '3/5' : 'N/A');
  });

  it('Containers row has the correct count of containers', () => {
    const containerRow = screen.getByTestId(overviewLocators.containersRow);
    expect(containerRow).toBeVisible();
    expect(containerRow).toHaveTextContent((scenario) ? '20' : 'N/A');
  });

  it('Capacity card has the correct capacity data', () => {
    const capacityOzoneUsed = screen.getByTestId(overviewLocators.capacityOzoneUsed);
    const capacityNonOzoneUsed = screen.getByTestId(overviewLocators.capacityNonOzoneUsed);
    const capacityRemaining = screen.getByTestId(overviewLocators.capacityRemaining);
    const capacityPreAllocated = screen.getByTestId(overviewLocators.capacityPreAllocated);

    expect(capacityOzoneUsed).toBeVisible();
    expect(capacityNonOzoneUsed).toBeVisible();
    expect(capacityRemaining).toBeVisible();
    expect(capacityPreAllocated).toBeVisible();

    expect(capacityOzoneUsed).toHaveTextContent(
      (scenario)
        ? /Ozone Used\s*784.7 MB/
        : /Ozone Used\s*0 B/
    );
    expect(capacityNonOzoneUsed).toHaveTextContent(
      (scenario)
        ? /Non Ozone Used\s*263.1 GB/
        : /Non Ozone Used\s*0 B/
    );
    expect(capacityRemaining).toHaveTextContent(
      (scenario)
        ? /Remaining\s*995.4 GB/
        : /Remaining\s*0 B/
    );
    expect(capacityPreAllocated).toHaveTextContent(
      (scenario)
        ? /Container Pre-allocated\s*11.2 GB/
        : /Container Pre-allocated\s*0 B/
    );
  });

  it('Volumes card has the correct number of volumes', () => {
    const volumeCard = screen.getByTestId(overviewLocators.volumesCard);
    expect(volumeCard).toBeVisible();
    expect(volumeCard).toHaveTextContent((scenario) ? '2' : 'N/A');
  });

  it('Buckets card has the correct number of buckets', () => {
    const bucketsCard = screen.getByTestId(overviewLocators.bucketsCard);
    expect(bucketsCard).toBeVisible();
    expect(bucketsCard).toHaveTextContent((scenario) ? '24' : 'N/A');
  });

  it('Keys card has the correct number of keys', () => {
    const keysCard = screen.getByTestId(overviewLocators.keysCard);
    expect(keysCard).toBeVisible();
    expect(keysCard).toHaveTextContent((scenario) ? '1424' : 'N/A');
  });

  it('Pipelines card has the correct count of Pipelines', () => {
    const pipelinesCard = screen.getByTestId(overviewLocators.pipelinesCard);
    expect(pipelinesCard).toBeVisible();
    expect(pipelinesCard).toHaveTextContent((scenario) ? '7' : 'N/A');
  });

  it('Deleted Containers card has the correct count of deleted containers', () => {
    const deletedContainersCard = screen.getByTestId(overviewLocators.deletedContainersCard);
    expect(deletedContainersCard).toBeVisible();
    expect(deletedContainersCard).toHaveTextContent((scenario) ? '10' : 'N/A')
  })

  it('Delete Pending Summary has the correct data', () => {
    const deletePendingReplicatedData = screen.getByTestId(
      overviewLocators.deletePendingTotalReplicatedData
    );
    const deletePendingUnreplicatedData = screen.getByTestId(
      overviewLocators.deletePendingTotalUnreplicatedData
    );
    const deletePendingKeys = screen.getByTestId(overviewLocators.deletePendingKeys);

    expect(deletePendingReplicatedData).toBeVisible();
    expect(deletePendingUnreplicatedData).toBeVisible();
    expect(deletePendingKeys).toBeVisible();

    expect(deletePendingReplicatedData).toHaveTextContent(
      (scenario)
        ? /Total Replicated Data\s*1 KB/
        : /Total Replicated Data\s*N\/A/
    );
    expect(deletePendingUnreplicatedData).toHaveTextContent(
      (scenario)
        ? /Total Unreplicated Data\s*4 KB/
        : /Total Unreplicated Data\s*N\/A/
    );
    expect(deletePendingKeys).toHaveTextContent(
      (scenario)
        ? /Delete Pending Keys\s*3/
        : /Delete Pending Keys\s*N\/A/
    );
  });

  it('Open Keys summary has the correct data', () => {
    const openKeysReplicatedData = screen.getByTestId(
      overviewLocators.openTotalReplicatedData
    );
    const openKeysUnreplicatedData = screen.getByTestId(
      overviewLocators.openTotalUnreplicatedData
    );
    const openKeys = screen.getByTestId(overviewLocators.openKeys);

    expect(openKeysReplicatedData).toBeVisible();
    expect(openKeysUnreplicatedData).toBeVisible();
    expect(openKeys).toBeVisible();

    expect(openKeysReplicatedData).toHaveTextContent(
      (scenario)
        ? /Total Replicated Data\s*1 KB/
        : /Total Replicated Data\s*N\/A/
    );
    expect(openKeysUnreplicatedData).toHaveTextContent(
      (scenario)
        ? /Total Unreplicated Data\s*4 KB/
        : /Total Unreplicated Data\s*N\/A/
    );
    expect(openKeys).toHaveTextContent(
      (scenario)
        ? /Open Keys\s*10/
        : /Open Keys\s*N\/A/
    );
  });
})
