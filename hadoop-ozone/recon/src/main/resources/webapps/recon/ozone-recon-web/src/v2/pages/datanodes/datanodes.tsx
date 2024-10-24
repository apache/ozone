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

import React, {
  useEffect,
  useRef,
  useState
} from 'react';
import moment from 'moment';
import { AxiosError } from 'axios';
import {
  Button,
  Modal
} from 'antd';
import {
  DeleteOutlined,
  WarningFilled,
} from '@ant-design/icons';
import { ValueType } from 'react-select';

import Search from '@/v2/components/search/search';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';
import DatanodesTable, { COLUMNS } from '@/v2/components/tables/datanodesTable';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import { showDataFetchError } from '@/utils/common';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import {
  AxiosGetHelper,
  AxiosPutHelper,
  cancelRequests
} from '@/utils/axiosRequestHelper';

import { useDebounce } from '@/v2/hooks/debounce.hook';
import {
  Datanode,
  DatanodeDecomissionInfo,
  DatanodeResponse,
  DatanodesResponse,
  DatanodesState
} from '@/v2/types/datanode.types';

import './datanodes.less'


const defaultColumns = COLUMNS.map(column => ({
  label: (typeof column.title === 'string')
    ? column.title
    : (column.title as Function)().props.children[0],
  value: column.key as string
}));

const SearchableColumnOpts = [{
  label: 'Hostname',
  value: 'hostname'
}, {
  label: 'UUID',
  value: 'uuid'
}, {
  label: 'Version',
  value: 'version'
}];

let decommissionUuids: string | string[] = [];
const COLUMN_UPDATE_DECOMMISSIONING = 'DECOMMISSIONING';

const Datanodes: React.FC<{}> = () => {

  const cancelSignal = useRef<AbortController>();
  const cancelDecommissionSignal = useRef<AbortController>();

  const [state, setState] = useState<DatanodesState>({
    lastUpdated: 0,
    columnOptions: defaultColumns,
    dataSource: []
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [selectedRows, setSelectedRows] = useState<React.Key[]>([]);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [searchColumn, setSearchColumn] = useState<'hostname' | 'uuid' | 'version' | 'revision'>('hostname');
  const [modalOpen, setModalOpen] = useState<boolean>(false);

  const debouncedSearch = useDebounce(searchTerm, 300);

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  async function loadDecommisionAPI() {
    decommissionUuids = [];
    const { request, controller } = await AxiosGetHelper(
      '/api/v1/datanodes/decommission/info',
      cancelDecommissionSignal.current
    );
    cancelDecommissionSignal.current = controller;
    return request
  };

  async function loadDataNodeAPI() {
    const { request, controller } = await AxiosGetHelper(
      '/api/v1/datanodes',
      cancelSignal.current
    );
    cancelSignal.current = controller;
    return request;
  };

  async function removeDatanode(selectedRowKeys: string[]) {
    setLoading(true);
    const { request, controller } = await AxiosPutHelper(
      '/api/v1/datanodes/remove',
      selectedRowKeys,
      cancelSignal.current
    );
    cancelSignal.current = controller;
    request.then(() => {
      loadData();
    }).catch((error) => {
      showDataFetchError(error.toString());
    }).finally(() => {
      setLoading(false);
      setSelectedRows([]);
    });
  }

  const loadData = async () => {
    setLoading(true);
    // Need to call decommission API on each interval to get updated status
    // before datanode API call to compare UUID's
    // update 'Operation State' column in table manually before rendering
    try {
      let decomissionResponse = await loadDecommisionAPI();
      decommissionUuids = decomissionResponse.data?.DatanodesDecommissionInfo?.map(
        (item: DatanodeDecomissionInfo) => item.datanodeDetails.uuid
      );
    } catch (error) {
      decommissionUuids = [];
      showDataFetchError((error as AxiosError).toString());
    }

    try {
      const datanodesAPIResponse = await loadDataNodeAPI();
      const datanodesResponse: DatanodesResponse = datanodesAPIResponse.data;
      const datanodes: DatanodeResponse[] = datanodesResponse.datanodes;
      const dataSource: Datanode[] = datanodes?.map(
        (datanode) => ({
          hostname: datanode.hostname,
          uuid: datanode.uuid,
          state: datanode.state,
          opState: (decommissionUuids?.includes(datanode.uuid) && datanode.opState !== 'DECOMMISSIONED')
            ? COLUMN_UPDATE_DECOMMISSIONING
            : datanode.opState,
          lastHeartbeat: datanode.lastHeartbeat,
          storageUsed: datanode.storageReport.used,
          storageTotal: datanode.storageReport.capacity,
          storageCommitted: datanode.storageReport.committed,
          storageRemaining: datanode.storageReport.remaining,
          pipelines: datanode.pipelines,
          containers: datanode.containers,
          openContainers: datanode.openContainers,
          leaderCount: datanode.leaderCount,
          version: datanode.version,
          setupTime: datanode.setupTime,
          revision: datanode.revision,
          buildDate: datanode.buildDate,
          networkLocation: datanode.networkLocation
        })
      );
      setLoading(false);
      setState({
        ...state,
        dataSource: dataSource,
        lastUpdated: Number(moment())
      });
    } catch (error) {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString())
    }
  }

  const autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  useEffect(() => {
    autoReloadHelper.startPolling();
    loadData();

    return (() => {
      autoReloadHelper.stopPolling();
      cancelRequests([
        cancelSignal.current!,
        cancelDecommissionSignal.current!
      ]);
    });
  }, []);

  function handleSelectionChange(rows: React.Key[]) {
    setSelectedRows(rows);
  }

  function handleModalOk() {
    setModalOpen(false);
    removeDatanode(selectedRows as string[])
  };

  function handleModalCancel() {
    setModalOpen(false);
    setSelectedRows([]);
  };

  const { dataSource, lastUpdated, columnOptions } = state;

  return (
    <>
      <div className='page-header-v2'>
        Datanodes
        <AutoReloadPanel
          isLoading={loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReloadHelper.handleAutoReloadToggle}
          onReload={loadData} />
      </div>
      <div className='data-container'>
        <div className='content-div'>
          <div className='table-header-section'>
            <div className='table-filter-section'>
              <MultiSelect
                options={columnOptions}
                defaultValue={selectedColumns}
                selected={selectedColumns}
                placeholder='Columns'
                onChange={handleColumnChange}
                onTagClose={() => { }}
                fixedColumn='hostname'
                columnLength={columnOptions.length} />
              {selectedRows.length > 0 &&
                <Button
                  type="primary"
                  icon={<DeleteOutlined />}
                  style={{
                    background: '#FF4D4E',
                    borderColor: '#FF4D4E'
                  }}
                  loading={loading}
                  onClick={() => { setModalOpen(true) }}> Remove
                </Button>
              }
            </div>
            <Search
              disabled={dataSource?.length < 1}
              searchOptions={SearchableColumnOpts}
              searchInput={searchTerm}
              searchColumn={searchColumn}
              onSearchChange={
                (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
              }
              onChange={(value) => {
                setSearchTerm('');
                setSearchColumn(value as 'hostname' | 'uuid' | 'version' | 'revision')
              }} />
          </div>
          <DatanodesTable
            loading={loading}
            data={dataSource}
            selectedColumns={selectedColumns}
            selectedRows={selectedRows}
            searchColumn={searchColumn}
            searchTerm={debouncedSearch}
            handleSelectionChange={handleSelectionChange}
            decommissionUuids={decommissionUuids} />
        </div>
      </div>
      <Modal
          title=''
          centered={true}
          visible={modalOpen}
          onOk={handleModalOk}
          onCancel={handleModalCancel}
          closable={false}
          width={400} >
            <div style={{
              margin: '0px 0px 5px 0px',
              fontSize: '16px',
              fontWeight: 'bold'
            }}>
              <WarningFilled className='icon-warning' style={{paddingRight: '8px'}}/>
              Stop Tracking Datanode
            </div>
            Are you sure, you want recon to stop tracking the selected <strong>{selectedRows.length}</strong> datanode(s)?
        </Modal>
    </>
  );
}

export default Datanodes;