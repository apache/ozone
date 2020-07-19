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
import {Input, Button, Icon} from 'antd';
import './columnSearch.less';

class ColumnSearch extends React.PureComponent {
  searchInput: Input | null = null;

  getColumnSearchProps = (dataIndex: string) => ({
    filterDropdown: ({
      setSelectedKeys,
      selectedKeys,
      confirm,
      clearFilters
    }: {
      setSelectedKeys: (keys: string[]) => void;
      selectedKeys: string[];
      confirm: () => void;
      clearFilters: () => void;
    }) => (
      <div className='column-search-container'>
        <Input
          ref={node => {
            this.searchInput = node;
          }}
          className='input-block'
          placeholder={`Search ${dataIndex}`}
          value={selectedKeys[0]}
          onChange={e =>
            setSelectedKeys(e.target.value ? [e.target.value] : [])}
          onPressEnter={() => this.handleSearch(confirm)}
        />
        <Button
          className='search-button'
          type='primary'
          icon='search'
          size='small'
          onClick={() => this.handleSearch(confirm)}
        >
          Search
        </Button>
        <Button
          size='small'
          icon='reset'
          className='reset-button'
          onClick={() => this.handleReset(clearFilters)}
        >
          Reset
        </Button>
      </div>
    ),
    filterIcon: (filtered: boolean) => (
      <Icon type='search' style={{color: filtered ? '#1890ff' : undefined}}/>
    ),
    onFilter: (value: string, record: any) =>
      record[dataIndex].toString().toLowerCase().includes(value.toLowerCase()),
    onFilterDropdownVisibleChange: (visible: boolean) => {
      if (visible) {
        setTimeout(() => {
          if (this.searchInput) {
            this.searchInput.select();
          }
        });
      }
    }
  });

  handleSearch = (confirm: () => void) => {
    confirm();
  };

  handleReset = (clearFilters: () => void) => {
    clearFilters();
  };
}

export {ColumnSearch};
