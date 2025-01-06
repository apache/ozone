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

import React, { HTMLAttributes } from 'react';
import moment from 'moment';
import { Popover, Tooltip } from 'antd'
import {
  CheckCircleFilled,
  CloseCircleFilled,
  HourglassFilled,
  InfoCircleOutlined,
  WarningFilled
} from '@ant-design/icons';
import Table, {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import { TableRowSelection } from 'antd/es/table/interface';

import StorageBar from '@/v2/components/storageBar/storageBar';
import DecommissionSummary from '@/v2/components/decommissioningSummary/decommissioningSummary';

import { ReplicationIcon } from '@/utils/themeIcons';
import { getTimeDiffFromTimestamp } from '@/v2/utils/momentUtils';

import {
  Datanode,
  DatanodeOpState,
  DatanodeOpStateList,
  DatanodeState,
  DatanodeStateList,
  DatanodeTableProps
} from '@/v2/types/datanode.types';
import { Pipeline } from '@/v2/types/pipelines.types';


let decommissioningUuids: string | string[] = [];

const headerIconStyles: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center'
}

const renderDatanodeState = (state: DatanodeState) => {
  const stateIconMap = {
    HEALTHY: <CheckCircleFilled twoToneColor='#1da57a' className='icon-success' />,
    STALE: <HourglassFilled className='icon-warning' />,
    DEAD: <CloseCircleFilled className='icon-failure' />
  };
  const icon = state in stateIconMap ? stateIconMap[state] : '';
  return <span>{icon} {state}</span>;
};

const renderDatanodeOpState = (opState: DatanodeOpState) => {
  const opStateIconMap = {
    IN_SERVICE: <CheckCircleFilled twoToneColor='#1da57a' className='icon-success' />,
    DECOMMISSIONING: <HourglassFilled className='icon-warning' />,
    DECOMMISSIONED: <WarningFilled className='icon-warning' />,
    ENTERING_MAINTENANCE: <HourglassFilled className='icon-warning' />,
    IN_MAINTENANCE: <WarningFilled className='icon-warning' />
  };
  const icon = opState in opStateIconMap ? opStateIconMap[opState] : '';
  return <span>{icon} {opState}</span>;
};

export const COLUMNS: ColumnsType<Datanode> = [
  {
    title: 'Hostname',
    dataIndex: 'hostname',
    key: 'hostname',
    sorter: (a: Datanode, b: Datanode) => a.hostname.localeCompare(
      b.hostname, undefined, { numeric: true }
    ),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'State',
    dataIndex: 'state',
    key: 'state',
    filterMultiple: true,
    filters: DatanodeStateList.map(state => ({ text: state, value: state })),
    onFilter: (value, record: Datanode) => record.state === value,
    render: (text: DatanodeState) => renderDatanodeState(text),
    sorter: (a: Datanode, b: Datanode) => a.state.localeCompare(b.state)
  },
  {
    title: 'Operational State',
    dataIndex: 'opState',
    key: 'opState',
    filterMultiple: true,
    filters: DatanodeOpStateList.map(state => ({ text: state, value: state })),
    onFilter: (value, record: Datanode) => record.opState === value,
    render: (text: DatanodeOpState) => renderDatanodeOpState(text),
    sorter: (a: Datanode, b: Datanode) => a.opState.localeCompare(b.opState)
  },
  {
    title: 'UUID',
    dataIndex: 'uuid',
    key: 'uuid',
    sorter: (a: Datanode, b: Datanode) => a.uuid.localeCompare(b.uuid),
    defaultSortOrder: 'ascend' as const,
    render: (uuid: string, record: Datanode) => {
      return (
        //1. Compare Decommission Api's UUID with all UUID in table and show Decommission Summary
        (decommissioningUuids && decommissioningUuids.includes(record.uuid) && record.opState !== 'DECOMMISSIONED') ?
          <DecommissionSummary uuid={uuid} /> : <span>{uuid}</span>
      );
    }
  },
  {
    title: 'Storage Capacity',
    dataIndex: 'storageUsed',
    key: 'storageUsed',
    sorter: (a: Datanode, b: Datanode) => a.storageRemaining - b.storageRemaining,
    render: (_: string, record: Datanode) => (
      <StorageBar
        strokeWidth={6}
        capacity={record.storageTotal}
        used={record.storageUsed}
        remaining={record.storageRemaining}
        committed={record.storageCommitted} />
    )
  },
  {
    title: 'Last Heartbeat',
    dataIndex: 'lastHeartbeat',
    key: 'lastHeartbeat',
    sorter: (a: Datanode, b: Datanode) => moment(a.lastHeartbeat).unix() - moment(b.lastHeartbeat).unix(),
    render: (heartbeat: number) => {
      return heartbeat > 0 ? getTimeDiffFromTimestamp(heartbeat) : 'NA';
    }
  },
  {
    title: 'Pipeline ID(s)',
    dataIndex: 'pipelines',
    key: 'pipelines',
    render: (pipelines: Pipeline[], record: Datanode) => {
      const renderPipelineIds = (pipelineIds: Pipeline[]) => {
        return pipelineIds?.map((pipeline: any, index: any) => (
          <div key={index} className='pipeline-container-v2'>
            <ReplicationIcon
              replicationFactor={pipeline.replicationFactor}
              replicationType={pipeline.replicationType}
              leaderNode={pipeline.leaderNode}
              isLeader={pipeline.leaderNode === record.hostname} />
            {pipeline.pipelineID}
          </div >
        ))
      }

      return (
        <Popover
          content={
            renderPipelineIds(pipelines)
          }
          title="Related Pipelines"
          placement="bottomLeft"
          trigger="hover">
          <strong>{pipelines.length}</strong> pipelines
        </Popover>
      );
    }
  },
  {
    title: () => (
      <span style={headerIconStyles} >
        Leader Count
        <Tooltip
          title='The number of Ratis Pipelines in which the given datanode is elected as a leader.' >
          <InfoCircleOutlined style={{ paddingLeft: '4px' }} />
        </Tooltip>
      </span>
    ),
    dataIndex: 'leaderCount',
    key: 'leaderCount',
    sorter: (a: Datanode, b: Datanode) => a.leaderCount - b.leaderCount
  },
  {
    title: 'Containers',
    dataIndex: 'containers',
    key: 'containers',
    sorter: (a: Datanode, b: Datanode) => a.containers - b.containers
  },
  {
    title: () => (
      <span style={headerIconStyles}>
        Open Container
        <Tooltip title='The number of open containers per pipeline.'>
          <InfoCircleOutlined style={{ paddingLeft: '4px' }} />
        </Tooltip>
      </span>
    ),
    dataIndex: 'openContainers',
    key: 'openContainers',
    sorter: (a: Datanode, b: Datanode) => a.openContainers - b.openContainers
  },
  {
    title: 'Version',
    dataIndex: 'version',
    key: 'version',
    sorter: (a: Datanode, b: Datanode) => a.version.localeCompare(b.version),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Setup Time',
    dataIndex: 'setupTime',
    key: 'setupTime',
    sorter: (a: Datanode, b: Datanode) => a.setupTime - b.setupTime,
    render: (uptime: number) => {
      return uptime > 0 ? moment(uptime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Revision',
    dataIndex: 'revision',
    key: 'revision',
    sorter: (a: Datanode, b: Datanode) => a.revision.localeCompare(b.revision),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Build Date',
    dataIndex: 'buildDate',
    key: 'buildDate',
    sorter: (a: Datanode, b: Datanode) => a.buildDate.localeCompare(b.buildDate),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Network Location',
    dataIndex: 'networkLocation',
    key: 'networkLocation',
    sorter: (a: Datanode, b: Datanode) => a.networkLocation.localeCompare(b.networkLocation),
    defaultSortOrder: 'ascend' as const
  }
];

const DatanodesTable: React.FC<DatanodeTableProps> = ({
  data,
  handleSelectionChange,
  decommissionUuids,
  selectedColumns,
  loading = false,
  selectedRows = [],
  searchColumn = 'hostname',
  searchTerm = ''
}) => {

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    );
  }

  function getFilteredData(data: Datanode[]) {
    return data?.filter(
      (datanode: Datanode) => datanode[searchColumn].includes(searchTerm)
    ) ?? [];
  }

  function isSelectable(record: Datanode) {
    // Disable checkbox for any datanode which is not DEAD to prevent removal
    return record.state !== 'DEAD' && true;
  }

  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => (
      `${range[0]}-${range[1]} of ${total} Datanodes`
    ),
    showSizeChanger: true
  };

  const rowSelection: TableRowSelection<Datanode> = {
    selectedRowKeys: selectedRows,
    onChange: (rows: React.Key[]) => { handleSelectionChange(rows) },
    getCheckboxProps: (record: Datanode) => ({
      disabled: isSelectable(record)
    }),
  };

  React.useEffect(() => {
    decommissioningUuids = decommissionUuids;
  }, [decommissionUuids])

  return (
    <div>
      <Table
        rowSelection={rowSelection}
        dataSource={getFilteredData(data)}
        columns={filterSelectedColumns()}
        loading={loading}
        rowKey='uuid'
        pagination={paginationConfig}
        scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
        locale={{ filterTitle: '' }}
        onRow={(record: Datanode) => ({
          'data-testid': `dntable-${record.uuid}`
        } as HTMLAttributes<HTMLElement>)}
        data-testid='dn-table' />
    </div>
  );
}

export default DatanodesTable;
