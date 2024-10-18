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
import { ValueType } from 'react-select';

import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import Search from '@/v2/components/search/search';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';
import PipelinesTable, { COLUMNS } from '@/v2/components/tables/pipelinesTable';
import { showDataFetchError } from '@/utils/common';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';
import { useDebounce } from '@/v2/hooks/debounce.hook';

import {
  Pipeline,
  PipelinesResponse,
  PipelinesState
} from '@/v2/types/pipelines.types';

import './pipelines.less';


const defaultColumns = COLUMNS.map(column => ({
  label: (typeof column.title === 'string')
    ? column.title
    : (column.title as Function)().props.children[0],
  value: column.key as string,
}));

const Pipelines: React.FC<{}> = () => {
  const cancelSignal = useRef<AbortController>();

  const [state, setState] = useState<PipelinesState>({
    activeDataSource: [],
    columnOptions: defaultColumns,
    lastUpdated: 0,
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [searchTerm, setSearchTerm] = useState<string>('');

  const debouncedSearch = useDebounce(searchTerm, 300);

  const loadData = () => {
    setLoading(true);
    //Cancel any previous requests
    cancelRequests([cancelSignal.current!]);

    const { request, controller } = AxiosGetHelper(
      '/api/v1/pipelines',
      cancelSignal.current
    );

    cancelSignal.current = controller;
    request.then(response => {
      const pipelinesResponse: PipelinesResponse = response.data;
      const pipelines: Pipeline[] = pipelinesResponse?.pipelines ?? {};
      setState({
        ...state,
        activeDataSource: pipelines,
        lastUpdated: Number(moment())
      })
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError(error.toString());
    })
  }

  const autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  useEffect(() => {
    autoReloadHelper.startPolling();
    loadData();
    return (() => {
      autoReloadHelper.stopPolling();
      cancelRequests([cancelSignal.current!]);
    })
  }, []);

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  const {
    activeDataSource,
    columnOptions,
    lastUpdated
  } = state;

  return (
    <>
      <div className='page-header-v2'>
        Pipelines
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
                fixedColumn='pipelineId'
                columnLength={COLUMNS.length} />
            </div>
            <Search
              disabled={activeDataSource?.length < 1}
              searchOptions={[{
                label: 'Pipeline ID',
                value: 'pipelineId'
              }]}
              searchInput={searchTerm}
              searchColumn={'pipelineId'}
              onSearchChange={
                (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
              }
              onChange={() => { }} />
          </div>
          <PipelinesTable
            loading={loading}
            data={activeDataSource}
            selectedColumns={selectedColumns}
            searchTerm={debouncedSearch} />
        </div>
      </div>
    </>
  );
}
export default Pipelines;