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

import React, { useState } from 'react';
import { LogResponse } from '@/v2/types/logs.types';
import { AxiosGetHelper, AxiosPostHelper, cancelRequests } from '@/utils/axiosRequestHelper';
import { showDataFetchError } from '@/utils/common';
import LogsTable from '@/v2/components/tables/logTable';
import { Pagination } from 'antd';
import { response } from 'msw';
import { AxiosError } from 'axios';

const LogViewer: React.FC<{}> = () => {
  const cancelSignal = React.useRef<AbortController>();

  const [state, setState] = useState<LogResponse>({
    logs: [],
    firstOffset: 0,
    lastOffset: 0,
    status: ''  
  });
  const [loading, setLoading] = useState<boolean>(false);
  
  function loadData() {
    setLoading(true);

    // Cancel any previous pending requests
    cancelRequests([cancelSignal.current!]);

    const { request, controller } = AxiosGetHelper(
      '/api/v1/log/read',
      cancelSignal.current,
    );

    cancelSignal.current = controller;
    request.then(response => {
      const data: LogResponse = response.data;
      setState({
        logs: data.logs,
        firstOffset: data.firstOffset,
        lastOffset: data.lastOffset,
        status: data.status
      })
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    })
  }

  function handlePaginationChange(offset: number, direction: string) {
    setLoading(true);

    // Cancel any previous pending requests
    cancelRequests([cancelSignal.current!]);

    const { request, controller } = AxiosPostHelper(
      '/api/v1/log/read',
      {
        offset: offset,
        lines: 100,
        direction: direction
      },
      cancelSignal.current,
    );

    cancelSignal.current = controller;
    request.then(response => {
      const data: LogResponse = response.data;
      setState({
        logs: data.logs,
        firstOffset: data.firstOffset,
        lastOffset: data.lastOffset,
        status: data.status
      });
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    });
  }

  React.useEffect(() => {
    loadData();

    return (() => {
      cancelRequests([cancelSignal.current!]);
    })
  }, []);

  const pagination = (
    <Pagination
      onChange={}
  )

  return (
    <>
      <div className='page-header-v2'>
        Logs
      </div>
      <div style={{ padding: '24px' }}>
        <div className='content-div'>
          <LogsTable
            loading={loading}
            data={state.logs}/>
        </div>
      </div>
    </>
  )
};

export default LogViewer;