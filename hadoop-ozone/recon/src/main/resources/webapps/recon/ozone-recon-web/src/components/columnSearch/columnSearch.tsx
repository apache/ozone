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

class ColumnSearch extends React.Component {
  getColumnSearchProps = (dataIndex: string) => ({
    filterDropdown: ({setSelectedKeys, selectedKeys, confirm, clearFilters}) => (
      <div style={{padding: 8}}>
        <Input
          ref={node => {
            this.searchInput = node;
          }}
          placeholder={`Search ${dataIndex}`}
          value={selectedKeys[0]}
          style={{width: 188, marginBottom: 8, display: 'block'}}
          onChange={e => setSelectedKeys(e.target.value ? [e.target.value] : [])}
          onPressEnter={() => this.handleSearch(selectedKeys, confirm)}
        />
        <Button
          type='primary'
          icon='search'
          size='small'
          style={{width: 90, marginRight: 8}}
          onClick={() => this.handleSearch(selectedKeys, confirm)}
        >
          Search
        </Button>
        <Button size='small' style={{width: 90}} onClick={() => this.handleReset(clearFilters)}>
          Reset
        </Button>
      </div>
    ),
    filterIcon: filtered => (
      <Icon type='search' style={{color: filtered ? '#1890ff' : undefined}}/>
    ),
    onFilter: (value, record) =>
      record[dataIndex]
        .toString()
        .toLowerCase()
        .includes(value.toLowerCase()),
    onFilterDropdownVisibleChange: visible => {
      if (visible) {
        setTimeout(() => this.searchInput.select());
      }
    }
  });

  handleSearch = (selectedKeys, confirm) => {
    confirm();
  };

  handleReset = clearFilters => {
    clearFilters();
  };
}

export {ColumnSearch};
