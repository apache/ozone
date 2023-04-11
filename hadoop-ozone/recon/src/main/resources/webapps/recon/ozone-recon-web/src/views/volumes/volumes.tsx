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
import axios from 'axios';
import {Table} from 'antd';
import {PaginationConfig} from 'antd/lib/pagination';
import moment from 'moment';
import './volumes.less';
import {AutoReloadHelper} from 'utils/autoReloadHelper';
import AutoReloadPanel from 'components/autoReloadPanel/autoReloadPanel';
import {MultiSelect, IOption} from 'components/multiSelect/multiSelect';
import {ActionMeta, ValueType} from 'react-select';
import {byteToSize, showDataFetchError} from 'utils/common';
import {ColumnSearch} from 'utils/columnSearch';
import {IAcl, IVolume} from "types/om.types";
import {AclPanel} from "../../components/aclDrawer/aclDrawer";
import {ColumnProps} from "antd/es/table";

interface IVolumeResponse {
  volume: string;
  owner: string;
  admin: string;
  creationTime: string;
  modificationTime: string;
  quotaInBytes: number;
  quotaInNamespace: number;
  usedNamespace: number;
  acls: IAcl[];
}

type VolumnTableColumn = ColumnProps<any> & any

interface IVolumesResponse {
  totalCount: number;
  volumes: IVolumeResponse[];
}

interface IVolumesState {
  loading: boolean;
  dataSource: IVolume[];
  totalCount: number;
  lastUpdated: number;
  selectedColumns: IOption[];
  columnOptions: IOption[];
  currentRow?: IVolume;
  showPanel: boolean;
  columns: VolumnTableColumn[]
}

const COLUMNS: VolumnTableColumn[] = [
  {
    title: 'Volume',
    dataIndex: 'volume',
    key: 'volume',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IVolume, b: IVolume) => a.volume.localeCompare(b.volume),
    defaultSortOrder: 'ascend' as const,
    fixed: 'left'
  },
  {
    title: 'Owner',
    dataIndex: 'owner',
    key: 'owner',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IVolume, b:IVolume) => a.owner.localeCompare(b.owner),
  },
  {
    title: 'Admin',
    dataIndex: 'admin',
    key: 'admin',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IVolume, b:IVolume) => a.admin.localeCompare(b.admin),
  },
  {
    title: 'Creation Time',
    dataIndex: 'creationTime',
    key: 'creationTime',
    isVisible: true,
    render: (creationTime: string) => {
      return creationTime && creationTime.length > 0 ? moment(creationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Modification Time',
    dataIndex: 'modificationTime',
    key: 'modificationTime',
    isVisible: true,
    render: (modificationTime: string) => {
      return modificationTime && modificationTime.length > 0 ? moment(modificationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Quota (Size)',
    dataIndex: 'quotaInBytes',
    key: 'quotaInBytes',
    isVisible: true,
    render: (quotaInBytes: number) => {
      return quotaInBytes && quotaInBytes !== -1 ? byteToSize(quotaInBytes, 3) : 'NA';
    }
  },
  {
    title: 'Quota (Count)',
    dataIndex: 'quotaInNamespace',
    key: 'quotaInNamespace',
    isVisible: true,
    render: (quotaInNamespace: number) => {
      return quotaInNamespace && quotaInNamespace !== -1 ? quotaInNamespace: 'NA';
    }
  },
  {
    title: 'Used Namespace',
    dataIndex: 'usedNamespace',
    key: 'usedNamespace',
    isVisible: true,
    render: (usedNamespace: number) => {
      return usedNamespace && usedNamespace > 0 ? usedNamespace : 'NA';
    }
  },
];

const allColumnsOption: IOption = {
  label: 'Select all',
  value: '*'
};

const defaultColumns: IOption[] = COLUMNS.map(column => ({
  label: column.key,
  value: column.key
}));


export class Volumes extends React.Component<Record<string, object>, IVolumesState> {
  autoReload: AutoReloadHelper;

  constructor(props = {}) {
    super(props);
    this._addAclColumn()
    this.state = {
      loading: false,
      dataSource: [],
      totalCount: 0,
      lastUpdated: 0,
      selectedColumns: [],
      columnOptions: defaultColumns,
      showPanel: false,
      currentRow: {},
    };
    this.autoReload = new AutoReloadHelper(this._loadData);
  }

  _addAclColumn = () => {
    // Inside the class component to access the React internal state
    const aclLinkColumn: VolumnTableColumn = {
      title: 'ACLs',
      dataIndex: 'acls',
      key: 'acls',
      isVisible: true,
      render: (_, record: IVolume) => {
        // eslint-disable-next-line jsx-a11y/anchor-is-valid
        return <a
            key="acl"
            onClick={() => this._handleAclLinkClick(record)}>
          Show ACL
        </a>
      }
    }

    if (COLUMNS.length > 0 && COLUMNS[COLUMNS.length - 1].key !== 'acls') {
      // Push the ACL column for initial
      COLUMNS.push(aclLinkColumn)
    } else {
      // Replace old ACL column with new ACL column with correct reference
      // e.g. After page is reloaded / redirect from other page
      COLUMNS[COLUMNS.length - 1] = aclLinkColumn
    }

    if (defaultColumns.length > 0 && defaultColumns[defaultColumns.length - 1].label !== 'acls') {
      defaultColumns.push({
        label: aclLinkColumn.key,
        value: aclLinkColumn.key,
      })
    }
  }

  _handleColumnChange = (selected: ValueType<IOption>, _action: ActionMeta<IOption>) => {
    const selectedColumns = (selected as IOption[]);
    this.setState({
      selectedColumns,
      showPanel: false,
    });
  };

  _getSelectedColumns = (selected: IOption[]) => {
    const selectedColumns = selected.length > 0 ? selected : COLUMNS.filter(column => column.isVisible).map(column => ({
      label: column.key,
      value: column.key
    }));
    return selectedColumns;
  };

  _handleAclLinkClick = (volume: IVolume) => {
    this.setState({
      showPanel: true,
      currentRow: volume,
    })
  }

  _loadData = () => {
    this.setState(prevState => ({
      loading: true,
      selectedColumns: this._getSelectedColumns(prevState.selectedColumns),
      showPanel: false,
    }));
    axios.get('/api/v1/om/volumes').then(response => {
      const volumesResponse: IVolumesResponse = response.data;
      const totalCount = volumesResponse.totalCount;
      const volumes: IVolumeResponse[] = volumesResponse.volumes;
      const dataSource: IVolume[] = volumes.map(volume => {
        return {
          volume: volume.volume,
          owner: volume.owner,
          admin: volume.admin,
          creationTime: volume.creationTime,
          modificationTime: volume.modificationTime,
          quotaInBytes: volume.quotaInBytes,
          quotaInNamespace: volume.quotaInNamespace,
          usedNamespace: volume.usedNamespace,
          acls: volume.acls,
        };
      });

      this.setState({
        loading: false,
        dataSource,
        totalCount,
        lastUpdated: Number(moment()),
        showPanel: false,
      });
    }).catch(error => {
      this.setState({
        loading: false,
        showPanel: false
      });
      showDataFetchError(error.toString());
    });
  };

  componentDidMount(): void {
    // Fetch volumes on component mount
    this._loadData();
    this.autoReload.startPolling();
  }

  componentWillUnmount(): void {
    this.autoReload.stopPolling();
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    console.log(current, pageSize);
  };

  render() {
    const {dataSource, loading, totalCount, lastUpdated, selectedColumns, columnOptions, showPanel, currentRow} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} volumes`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
      <div className='volumes-container'>
        <div className='page-header'>
          Volumes ({totalCount})
          <div className='filter-block'>
            <MultiSelect
              allowSelectAll
              isMulti
              maxShowValues={3}
              className='multi-select-container'
              options={columnOptions}
              closeMenuOnSelect={false}
              hideSelectedOptions={false}
              value={selectedColumns}
              allOption={allColumnsOption}
              onChange={this._handleColumnChange}
            /> Columns
          </div>
          <AutoReloadPanel
            isLoading={loading}
            lastUpdated={lastUpdated}
            togglePolling={this.autoReload.handleAutoReloadToggle}
            onReload={this._loadData}
          />
        </div>

        <div className='content-div'>
          <Table
            dataSource={dataSource}
            columns={COLUMNS.reduce<any[]>((filtered, column) => {
              if (selectedColumns.some(e => e.value === column.key)) {
                if (column.isSearchable) {
                  const newColumn = {
                    ...column,
                    ...new ColumnSearch(column).getColumnSearchProps(column.dataIndex)
                  };
                  filtered.push(newColumn);
                } else {
                  filtered.push(column);
                }
              }

              return filtered;
            }, [])}
            loading={loading}
            pagination={paginationConfig}
            rowKey='volume'
            scroll={{x: true, y: false, scrollToFirstRowOnChange: true}}
          />
        </div>
        <AclPanel visible={showPanel} acls={currentRow.acls} objName={currentRow.volume} objType={"Volume"}/>
      </div>
    );
  }
}
