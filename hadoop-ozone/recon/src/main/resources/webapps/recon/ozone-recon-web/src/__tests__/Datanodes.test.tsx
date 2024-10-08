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
import { cleanup, fireEvent, render, screen } from '@testing-library/react';

import { datanodeLocators } from '@tests/locators/locators';
import { datanodeServer, nullDatanodeServer, nullDatanodeResponseServer } from '@tests/mocks/datanodeMocks/datanodeServer';
import Datanodes from '@/v2/pages/datanodes/datanodes';
import { SetupServer } from 'msw/lib/node';

const WrappedDatanodeComponent = () => {
  return (
    <BrowserRouter>
      <Datanodes />
    </BrowserRouter>
  )
}

function getServerListener(scenarioType: string): SetupServer {
  if (scenarioType === 'healthy') { return datanodeServer }
  else if (scenarioType === 'null') { return nullDatanodeServer }
  else { return nullDatanodeResponseServer }
}

describe('Datanodes Tests - Server returns proper response', (scenario) => {
  beforeAll(async () => {
    datanodeServer.listen();

    render(
      <WrappedDatanodeComponent />
    )
    //Setting a timeout of 100ms to allow requests to be resolved and states to be set
    await new Promise((r) => { setTimeout(r, 100) })
  });

  afterAll(() => {
    datanodeServer.close();

    /**
     * Need to cleanup the DOM after one suite has completely run
     * Otherwise we will get duplicate elements
     */
    cleanup();
  });

  // Tests begin
  it('Datanode table gives proper state and operational state for the datanodes', () => {
    const datanodes = [
      screen.getByTestId(datanodeLocators.datanodeTableRow('1')),
      screen.getByTestId(datanodeLocators.datanodeTableRow('2')),
      screen.getByTestId(datanodeLocators.datanodeTableRow('3')),
      screen.getByTestId(datanodeLocators.datanodeTableRow('4')),
      screen.getByTestId(datanodeLocators.datanodeTableRow('5'))
    ];

    // The following have the 3rd child as the HEALTH cell and the 4th child as the Operation state cell
    
    // First datanode state and operational state should be healthy and IN_SERVICE
    expect(datanodes[0].children.item(2)).toHaveTextContent('HEALTHY');
    expect(datanodes[0].children.item(3)).toHaveTextContent('IN_SERVICE');

    // Second datanode state and operational state should be STALE and IN_SERVICE
    expect(datanodes[1].children.item(2)).toHaveTextContent('STALE');
    expect(datanodes[1].children.item(3)).toHaveTextContent('IN_SERVICE');

    // Third datanode state and operational state should be DEAD and IN_SERVICE
    expect(datanodes[2].children.item(2)).toHaveTextContent('DEAD');
    expect(datanodes[2].children.item(3)).toHaveTextContent('IN_SERVICE');
    
    // Fourth datanode state and operational state should be HEALTHY and DECOMMISSIONING
    expect(datanodes[3].children.item(2)).toHaveTextContent('HEALTHY');
    expect(datanodes[3].children.item(3)).toHaveTextContent('DECOMMISSIONING');
    
    // Fifth datanode state and operational state should be DEAD and DECOMMISSIONED
    expect(datanodes[4].children.item(2)).toHaveTextContent('DEAD');
    expect(datanodes[4].children.item(3)).toHaveTextContent('DECOMMISSIONED');
  })

  /**
   * In this test the numbers represent the column index
   * 0th column is the checkbox so we do not start at 0
   * So 1 would be the Hostname column
   * 2 would be the State column
   * 3 would be Operational State column and so on
   * 
   * Currently firing the click event on the sorter gives error
   * AntD table tries to scroll to top after the sort button is clicked
   * But since this is on a virtual DOM it will throw error
   * Also as we are only having 5 elements in the table there is no need to trigger
   * this scroll behaviour on sort as the table will always be in complete view
   * Hence the tests are working but disabled to check for workaround as it will passively throw error
  */
  // it.each([1, 2, 3, 4])('Sorting works properly for the %s-th column', (idx: number) => {
  //   const dnTable = screen.getByTestId(datanodeLocators.datanodeTable);
  //   const columnSorter = dnTable.querySelector('thead > tr')
  //     ?.children.item(idx)
  //     ?.querySelector('.ant-table-column-sorter');

  //   try {
  //     // 
  //     fireEvent.click(columnSorter!);
  //   } catch(e) {
  //     console.log("caught");
  //     // console.warn("Was not able to scroll the DOM\nEncountered: ");
  //   }

  //   let rows = dnTable.querySelector<HTMLElement>('tbody')?.children ?? new HTMLCollection();
  //   let content = [];
  //   for(let i = 1; i < rows?.length; i++) {
  //     //Skip first row since it is an internal antd row
  //     content.push(rows.item(i)?.children.item(idx)?.textContent ?? "");
  //   }

  //   // First click should cause it to be descending order
  //   // if the column is Hostname i.e 1st column
  //   // else ascending for other columns
  //   if (idx === 1) {
  //     // Shallow copy to not modify the original array
  //     const sortedContent = [...content].sort((a, b) => b.localeCompare(a));
  //     console.log(sortedContent.toString());
  //     console.log(content.toString());
  //     assert(sortedContent.toString() === content.toString(), "Table content not equal to expected sorting order");
  //   } else {
  //     const sortedContent = [...content].sort((a, b) => a.localeCompare(b));
  //     console.log(sortedContent.toString());
  //     console.log(content.toString());
  //     assert(sortedContent.toString() === content.toString(), "Table content not equal to expected sorting order");
  //   }
  // })

  // Should iterate over the search options .each(['Hostname', 'UUID'])
  // after finding workaround for clicking Select options from dropdown
  // which doesn't work currently. Refer to comment in test for more info
  it('Datanode search works properly', () => {
    const searchDropdown = screen.getByTestId(datanodeLocators.datanodeSearchcDropdown);
    const inputField = screen.getByTestId(datanodeLocators.datanodeSearchInput);
    // Check that we got the dropdown and the input field present
    expect(searchDropdown).toBeVisible();
    expect(inputField).toBeVisible();
    
    // The following option selector doesn't work properly
    // as the options are being rendered outside the 'root' div
    // TODO: Uncomment after finding workaround
    // fireEvent.mouseDown(searchDropdown!);
    // const option = screen.getByTestId(datanodeLocators.datanodeSearchOption(dropdownVal));
    // expect(option).toBeVisible();
    // fireEvent.click(option);

    ['1', '2', '3', '4', '5'].forEach(async (searchTerm) => {
      fireEvent.change(inputField!, { target: {value: searchTerm}});
      // Await the debounce time duration
      await new Promise((r) => { setTimeout(r, 300) })
      const table = screen.getByTestId(datanodeLocators.datanodeTable);

      // Assert that the row containing the data is visible
      expect(screen.getByTestId(datanodeLocators.datanodeTableRow(searchTerm))).toBeVisible();

      // Get all the table rows
      const tableRows = table.querySelectorAll('tbody > tr');
      // We have a hidden row for antd table so search
      // should give us 1 result row + 1 hidden row
      expect(tableRows.length).toBe(2);
    });
  });
})