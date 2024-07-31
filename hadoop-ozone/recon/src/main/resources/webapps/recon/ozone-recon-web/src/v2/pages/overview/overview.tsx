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

import React, { useEffect, useState } from 'react';
import axios, { CanceledError } from 'axios';
import moment from 'moment';
import filesize from 'filesize';
import { Row, Col, Card, Button } from 'antd';
import { CheckCircleFilled, WarningFilled } from '@ant-design/icons';

import OverviewCard from '@/v2/components/overviewCard/overviewCard';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import { showDataFetchError, byteToSize } from '@/utils/common';
import { AxiosGetHelper, cancelRequests, PromiseAllSettledGetHelper } from '@/utils/axiosRequestHelper';

import { ClusterStateResponse, OverviewState, StorageReport } from '@/v2/types/overview.types';

import OverviewTableCard from '@/v2/components/overviewCard/overviewTableCard';

import './overview.less';
import { Link } from 'react-router-dom';
import OverviewStorageCard from '@/v2/components/overviewCard/overviewStorageCard';


const getHealthIcon = (value: string): React.ReactElement => {
  const values = value.split('/');
  if (values.length == 2 && values[0] < values[1]) {
    return (
      <>
        <div className='icon-warning' style={{
          fontSize: '20px',
          alignItems: 'center'
        }}>
          <WarningFilled style={{
            marginRight: '5px'
          }} />
          Unhealthy
        </div>
      </>
    )
  }
  return (
    <div className='icon-success' style={{
      fontSize: '20px',
      alignItems: 'center'
    }}>
      <CheckCircleFilled style={{
        marginRight: '5px'
      }} />
      Healthy
    </div>
  )
}

const Overview: React.FC<{}> = (props = {}) => {

  let interval = 0;
  let cancelOverviewSignal: AbortController;
  let cancelOMDBSyncSignal: AbortController;

  const [state, setState] = useState<OverviewState>({
    loading: false,
    datanodes: '',
    pipelines: 0,
    containers: 0,
    volumes: 0,
    buckets: 0,
    keys: 0,
    missingContainersCount: 0,
    lastRefreshed: 0,
    lastUpdatedOMDBDelta: 0,
    lastUpdatedOMDBFull: 0,
    omStatus: '',
    openContainers: 0,
    deletedContainers: 0,
    openSummarytotalUnrepSize: 0,
    openSummarytotalRepSize: 0,
    openSummarytotalOpenKeys: 0,
    deletePendingSummarytotalUnrepSize: 0,
    deletePendingSummarytotalRepSize: 0,
    deletePendingSummarytotalDeletedKeys: 0,
    scmServiceId: '',
    omServiceId: ''
  })
  const [storageReport, setStorageReport] = useState<StorageReport>({
    capacity: 0,
    used: 0,
    remaining: 0,
    committed: 0
  })

  // Component mounted, fetch initial data
  useEffect(() => {
    loadOverviewPageData();
    autoReloadHelper.startPolling();
    return (() => {
      // Component will Un-mount
      autoReloadHelper.stopPolling();
      cancelRequests([
        cancelOMDBSyncSignal,
        cancelOverviewSignal
      ]);
    })
  }, [])

  const loadOverviewPageData = () => {
    setState({
      ...state,
      loading: true
    });

    // Cancel any previous pending requests
    cancelRequests([
      cancelOMDBSyncSignal,
      cancelOverviewSignal
    ]);

    const { requests, controller } = PromiseAllSettledGetHelper([
      '/api/v1/clusterState',
      '/api/v1/task/status',
      '/api/v1/keys/open/summary',
      '/api/v1/keys/deletePending/summary'
    ], cancelOverviewSignal);
    cancelOverviewSignal = controller;

    requests.then(axios.spread((
      clusterStateResponse: Awaited<Promise<any>>,
      taskstatusResponse: Awaited<Promise<any>>,
      openResponse: Awaited<Promise<any>>,
      deletePendingResponse: Awaited<Promise<any>>
    ) => {
      let responseError = [
        clusterStateResponse,
        taskstatusResponse,
        openResponse,
        deletePendingResponse
      ].filter((resp) => resp.status === 'rejected');

      if (responseError.length !== 0) {
        responseError.forEach((err) => {
          if (err.reason.toString().includes("CanceledError")) {
            throw new CanceledError('canceled', "ERR_CANCELED");
          }
          else {
            const reqMethod = err.reason.config.method;
            const reqURL = err.reason.config.url
            showDataFetchError(`Failed to ${reqMethod} URL ${reqURL}\n${err.reason.toString()}`);
          }
        })
      }

      const clusterState: ClusterStateResponse = clusterStateResponse.value?.data ?? {
        missingContainers: 'N/A',
        totalDatanodes: 'N/A',
        healthyDatanodes: 'N/A',
        pipelines: 'N/A',
        storageReport: {
          capacity: 0,
          used: 0,
          remaining: 0,
          committed: 0
        },
        containers: 'N/A',
        volumes: 'N/A',
        buckets: 'N/A',
        keys: 'N/A',
        openContainers: 'N/A',
        deletedContainers: 'N/A',
        keysPendingDeletion: 'N/A',
        scmServiceId: 'N/A',
        omServiceId: 'N/A',
      };
      const taskStatus = taskstatusResponse.value?.data ?? [{
        taskName: 'N/A',
        lastUpdatedTimestamp: 0,
        lastUpdatedSeqNumber: 0
      }];
      const missingContainersCount = clusterState.missingContainers;
      const omDBDeltaObject = taskStatus && taskStatus.find((item: any) => item.taskName === 'OmDeltaRequest');
      const omDBFullObject = taskStatus && taskStatus.find((item: any) => item.taskName === 'OmSnapshotRequest');

      setState({
        ...state,
        loading: false,
        datanodes: `${clusterState.healthyDatanodes}/${clusterState.totalDatanodes}`,
        pipelines: clusterState.pipelines,
        containers: clusterState.containers,
        volumes: clusterState.volumes,
        buckets: clusterState.buckets,
        keys: clusterState.keys,
        missingContainersCount: missingContainersCount,
        openContainers: clusterState.openContainers,
        deletedContainers: clusterState.deletedContainers,
        lastRefreshed: Number(moment()),
        lastUpdatedOMDBDelta: omDBDeltaObject?.lastUpdatedTimestamp,
        lastUpdatedOMDBFull: omDBFullObject?.lastUpdatedTimestamp,
        openSummarytotalUnrepSize: openResponse?.data?.totalUnreplicatedDataSize,
        openSummarytotalRepSize: openResponse?.data?.totalReplicatedDataSize,
        openSummarytotalOpenKeys: openResponse?.data?.totalOpenKeys,
        deletePendingSummarytotalUnrepSize: deletePendingResponse?.data?.totalUnreplicatedDataSize,
        deletePendingSummarytotalRepSize: deletePendingResponse?.data?.totalReplicatedDataSize,
        deletePendingSummarytotalDeletedKeys: deletePendingResponse?.data?.totalDeletedKeys,
        scmServiceId: clusterState?.scmServiceId,
        omServiceId: clusterState?.omServiceId
      });
      setStorageReport({
        ...storageReport,
        ...clusterState.storageReport
      });
    })).catch((error: Error) => {
      setState({
        ...state,
        loading: false
      });
      showDataFetchError(error.toString());
    });
  }

  let autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadOverviewPageData);

  const syncOmData = () => {
    setState({
      ...state,
      loading: true
    });

    const { request, controller } = AxiosGetHelper(
      '/api/v1/triggerdbsync/om',
      cancelOMDBSyncSignal,
      'OM-DB Sync request cancelled because data was updated'
    );
    cancelOMDBSyncSignal = controller;

    request.then(omStatusResponse => {
      const omStatus = omStatusResponse.data;
      setState({
        ...state,
        loading: false,
        omStatus: omStatus
      });
    }).catch((error: Error) => {
      setState({
        ...state,
        loading: false
      });
      showDataFetchError(error.toString());
    });
  };

  const {
    loading, datanodes, pipelines,
    containers, volumes, buckets,
    openSummarytotalUnrepSize,
    openSummarytotalRepSize,
    openSummarytotalOpenKeys,
    deletePendingSummarytotalUnrepSize,
    deletePendingSummarytotalRepSize,
    deletePendingSummarytotalDeletedKeys,
    keys, missingContainersCount,
    lastRefreshed, lastUpdatedOMDBDelta,
    lastUpdatedOMDBFull,
    omStatus, openContainers,
    deletedContainers, scmServiceId, omServiceId
  } = state;

  const healthCardIndicators = (
    <>
      <Col span={14}>
        Datanodes
        {getHealthIcon(datanodes)}
      </Col>
      <Col span={10}>
        Containers
        {getHealthIcon(`${(containers - missingContainersCount)}/${containers}`)}
      </Col>
    </>
  )

  const datanodesLink = (
    <Button
      type='link'
      size='small'>
      <Link to='/Datanodes'> View More </Link>
    </Button>
  )

  const containersLink = (missingContainersCount > 0)
    ? (
      <Button
        type='link'
        size='small'>
        <Link to='/MissingContainers'> View More</Link>
      </Button>
    ) : (
      <Button
        type='link'
        size='small'>
        <Link to='/Containers'> View More</Link>
      </Button>
    )
  return (
    <>
      <div className='page-header'>
        Overview
        <AutoReloadPanel isLoading={loading} lastRefreshed={lastRefreshed}
          lastUpdatedOMDBDelta={lastUpdatedOMDBDelta} lastUpdatedOMDBFull={lastUpdatedOMDBFull}
          togglePolling={autoReloadHelper.handleAutoReloadToggle} onReload={loadOverviewPageData} omSyncLoad={syncOmData} omStatus={omStatus} />
      </div>
      <div className='overview-content'>
        <Row
          align='stretch'
          gutter={[
            {
              xs: 24,
              sm: 24,
              md: 16,
              lg: 16,
              xl: 16
            }, 20]}>
          <Col span={10}>
            <OverviewTableCard
              title='Health'
              data={healthCardIndicators}
              columns={[
                {
                  title: 'Name',
                  dataIndex: 'name',
                  key: 'name'
                },
                {
                  title: 'Value',
                  dataIndex: 'value',
                  key: 'value',
                  align: 'right'
                },
                {
                  title: 'Action',
                  dataIndex: 'action',
                  key: 'action',
                  align: 'right'
                }
              ]}
              tableData={[
                {
                  key: 'datanodes',
                  name: 'Datanodes',
                  value: datanodes,
                  action: datanodesLink
                },
                {
                  key: 'containers',
                  name: 'Containers',
                  value: `${(containers - missingContainersCount)}/${containers}`,
                  action: containersLink
                }
              ]}
            />
          </Col>
          <Col span={14}>
            <OverviewStorageCard storageReport={storageReport} loading={loading} />
          </Col>
        </Row>
        <Row gutter={[
          {
            xs: 24,
            sm: 24,
            md: 16,
            lg: 16,
            xl: 16
          }, 20]}>
          <Col flex="1 0 20%">
            <Card> A </Card>
          </Col>
          <Col flex="1 0 20%">
            <Card> A </Card>
          </Col>
          <Col flex="1 0 20%">
            <Card> A </Card>
          </Col>
          <Col flex="1 0 20%">
            <Card> A </Card>
          </Col>
          <Col flex="1 0 20%">
            <Card> A </Card>
          </Col>
        </Row>
        <Row gutter={[
          {
            xs: 24,
            sm: 24,
            md: 16,
            lg: 16,
            xl: 16
          }, 20]}>
          <Col flex="1 0 50%">
            <Card>Some card</Card>
          </Col>
          <Col flex="1 0 50%">
            <Card>Some card</Card>
          </Col>
        </Row>
      </div>
    </>
  );
}

export default Overview;