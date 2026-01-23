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
import axios, { CanceledError } from 'axios';
import moment from 'moment';
import filesize from 'filesize';
import { Row, Col, Tooltip } from 'antd';
import { CheckCircleFilled, ExclamationCircleFilled } from '@ant-design/icons';

import { IStorageReport } from '@/types/datanode.types';
import OverviewCard from '@/components/overviewCard/overviewCard';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import { showDataFetchError, byteToSize } from '@/utils/common';
import { AxiosGetHelper, cancelRequests, PromiseAllSettledGetHelper } from '@/utils/axiosRequestHelper';

import './overview.less';

const size = filesize.partial({ round: 1 });

interface IClusterStateResponse {
  missingContainers: number | string;
  totalDatanodes: number | string;
  healthyDatanodes: number | string;
  pipelines: number | string;
  storageReport: IStorageReport;
  containers: number | string;
  volumes: number | string;
  buckets: number | string;
  keys: number | string;
  openContainers: number | string;
  deletedContainers: number | string;
  keysPendingDeletion: number | string;
  scmServiceId: string;
  omServiceId: string;
}

interface IOverviewState {
  loading: boolean;
  datanodes: string;
  pipelines: number | string;
  storageReport: IStorageReport;
  containers: number | string;
  volumes: number | string;
  buckets: number | string;
  keys: number | string;
  missingContainersCount: number | string;
  lastRefreshed: number | string;
  lastUpdatedOMDBDelta: number | string;
  lastUpdatedOMDBFull: number | string;
  omStatus: string;
  openContainers: number | string;
  deletedContainers: number | string;
  openSummarytotalUnrepSize: number | string;
  openSummarytotalRepSize: number | string;
  openSummarytotalOpenKeys: number | string;
  deletePendingSummarytotalUnrepSize: number | string;
  deletePendingSummarytotalRepSize: number | string;
  deletePendingSummarytotalDeletedKeys: number | string;
  decommissionInfoCount: number | string;
  scmServiceId: string;
  omServiceId: string;
}

let cancelOverviewSignal: AbortController;
let cancelOMDBSyncSignal: AbortController;

export class Overview extends React.Component<Record<string, object>, IOverviewState> {
  interval = 0;
  autoReload: AutoReloadHelper;

  constructor(props = {}) {
    super(props);
    this.state = {
      loading: false,
      datanodes: '',
      pipelines: 0,
      storageReport: {
        capacity: 0,
        used: 0,
        remaining: 0
      },
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
      decommissionInfoCount: 0
    };
    this.autoReload = new AutoReloadHelper(this._loadData);
  }

  _loadData = () => {
    this.setState({
      loading: true
    });

    //cancel any previous pending requests
    cancelRequests([
      cancelOMDBSyncSignal,
      cancelOverviewSignal
    ]);

    const { requests, controller } = PromiseAllSettledGetHelper([
      '/api/v1/clusterState',
      '/api/v1/task/status',
      '/api/v1/keys/open/summary',
      '/api/v1/keys/deletePending/summary',
      '/api/v1/datanodes/decommission/info'
    ], cancelOverviewSignal);
    cancelOverviewSignal = controller;

    requests.then(axios.spread((
      clusterStateResponse: Awaited<Promise<any>>,
      taskstatusResponse: Awaited<Promise<any>>,
      openResponse: Awaited<Promise<any>>,
      deletePendingResponse: Awaited<Promise<any>>,
      decommissionResponse: Awaited<Promise<any>>
    ) => {
      let responseError = [
        clusterStateResponse,
        taskstatusResponse,
        openResponse,
        deletePendingResponse,
        decommissionResponse
      ].filter((resp) => resp.status === 'rejected');

      if (responseError.length !== 0) {
        responseError.forEach((err) => {
          if (err.reason.toString().includes("CanceledError")){
            throw new CanceledError('canceled', "ERR_CANCELED");
          }
          else {
            const reqMethod = err.reason.config.method;
            const reqURL = err.reason.config.url
            showDataFetchError(`Failed to ${reqMethod} URL ${reqURL}\n${err.reason.toString()}`);
          }
        })
      }

      const clusterState: IClusterStateResponse = clusterStateResponse.value?.data ?? {
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
        taskName: 'N/A', lastUpdatedTimestamp: 0, lastUpdatedSeqNumber: 0
      }];
      const missingContainersCount = clusterState.missingContainers;
      const omDBDeltaObject = taskStatus && taskStatus.find((item:any) => item.taskName === 'OmDeltaRequest');
      const omDBFullObject = taskStatus && taskStatus.find((item:any) => item.taskName === 'OmSnapshotRequest');

      this.setState({
        loading: false,
        datanodes: clusterState.healthyDatanodes !== 'N/A'
          ? `${clusterState.healthyDatanodes}/${clusterState.totalDatanodes}`
          : `N/A`,
        storageReport: clusterState.storageReport,
        pipelines: clusterState.pipelines,
        containers: clusterState.containers,
        volumes: clusterState.volumes,
        buckets: clusterState.buckets,
        keys: clusterState.keys,
        missingContainersCount,
        openContainers: clusterState.openContainers,
        deletedContainers: clusterState.deletedContainers,
        lastRefreshed: Number(moment()),
        lastUpdatedOMDBDelta: omDBDeltaObject && omDBDeltaObject.lastUpdatedTimestamp,
        lastUpdatedOMDBFull: omDBFullObject && omDBFullObject.lastUpdatedTimestamp,
        openSummarytotalUnrepSize: openResponse.value?.data?.totalUnreplicatedDataSize,
        openSummarytotalRepSize: openResponse.value?.data?.totalReplicatedDataSize,
        openSummarytotalOpenKeys: openResponse.value?.data?.totalOpenKeys,
        deletePendingSummarytotalUnrepSize: deletePendingResponse.value?.data?.totalUnreplicatedDataSize,
        deletePendingSummarytotalRepSize: deletePendingResponse.value?.data?.totalReplicatedDataSize,
        deletePendingSummarytotalDeletedKeys: deletePendingResponse.value?.data?.totalDeletedKeys,
        decommissionInfoCount: decommissionResponse.value?.data?.DatanodesDecommissionInfo.length,
        scmServiceId: clusterState.scmServiceId,
        omServiceId: clusterState.omServiceId
      });
    })).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error);
    });
  };


  omSyncData = () => {
    this.setState({
      loading: true
    });

    const { request, controller } = AxiosGetHelper(
      '/api/v1/triggerdbsync/om',
      cancelOMDBSyncSignal,
      'OM-DB Sync request cancelled because data was updated'
    );
    cancelOMDBSyncSignal = controller;

    request.then(omstatusResponse => {
      const omStatus = omstatusResponse.data;
      this.setState({
        loading: false,
        omStatus: omStatus
      });
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error);
    });
  };

  componentDidMount(): void {
    this._loadData();
    this.autoReload.startPolling();
  }

  componentWillUnmount(): void {
    this.autoReload.stopPolling();
    cancelRequests([
      cancelOMDBSyncSignal,
      cancelOverviewSignal
    ]);
  }

  render() {
    const { loading, datanodes, pipelines, storageReport, containers, volumes, buckets, openSummarytotalUnrepSize, openSummarytotalRepSize, openSummarytotalOpenKeys,
      deletePendingSummarytotalUnrepSize, deletePendingSummarytotalRepSize, deletePendingSummarytotalDeletedKeys,
      keys, missingContainersCount, lastRefreshed, lastUpdatedOMDBDelta, lastUpdatedOMDBFull,
      omStatus, openContainers, deletedContainers, scmServiceId, omServiceId, decommissionInfoCount } = this.state;

    let openKeysError: boolean = false;
    let pendingDeleteKeysError: boolean = false;
    let decommissionInfoError: boolean = false;

    if ([
      openSummarytotalRepSize,
      openSummarytotalUnrepSize,
      openSummarytotalOpenKeys].some(
        (data) => data === undefined
      )) {
      openKeysError = true;
    }

    if ([decommissionInfoCount].some(
        (data) => data === undefined
      )) {
      decommissionInfoError = true;
    }

    if ([
      deletePendingSummarytotalRepSize,
      deletePendingSummarytotalUnrepSize,
      deletePendingSummarytotalDeletedKeys].some(
        (data) => data === undefined
      )) {
        pendingDeleteKeysError = true;
      }
    const datanodesElement = datanodes !== 'N/A'
      ? (
        <span>
          <CheckCircleFilled className='icon-success icon-small' /> {datanodes} <span className='ant-card-meta-description meta'>HEALTHY</span>
        </span>
      )
      : (
        <span>
          <Tooltip
            placement='bottom'
            title={'Failed to fetch Datanodes count'}>
            <ExclamationCircleFilled className='icon-failure icon-small' />
            <span className='padded-text'>{datanodes}</span>
            <span className='ant-card-meta-description meta padded-text'>UNHEALTHY</span>
          </Tooltip>
        </span>
      )
    const openSummaryData = (
      <div>
        {openSummarytotalRepSize !== undefined ? byteToSize(openSummarytotalRepSize, 1) : 'N/A'}   <span className='ant-card-meta-description meta'>Total Replicated Data Size</span><br />
        {openSummarytotalUnrepSize !== undefined ? byteToSize(openSummarytotalUnrepSize, 1) : 'N/A'}  <span className='ant-card-meta-description meta'>Total UnReplicated Data Size</span><br />
        {openSummarytotalOpenKeys !== undefined ? openSummarytotalOpenKeys : 'N/A'}  <span className='ant-card-meta-description meta'>Total Open Keys</span>
      </div>
    );
    const deletePendingSummaryData = (
      <div>
        {deletePendingSummarytotalRepSize !== undefined ? byteToSize(deletePendingSummarytotalRepSize, 1) : 'N/A'}  <span className='ant-card-meta-description meta'>Total Replicated Data Size</span><br />
        {deletePendingSummarytotalUnrepSize !== undefined ? byteToSize(deletePendingSummarytotalUnrepSize, 1) : 'N/A'}  <span className='ant-card-meta-description meta'>Total UnReplicated Data Size</span><br />
        {deletePendingSummarytotalDeletedKeys !== undefined ? deletePendingSummarytotalDeletedKeys : 'N/A'}  <span className='ant-card-meta-description meta'>Total Pending Delete Keys</span>
      </div>
    );
    const containersTooltip = missingContainersCount === 1 ? 'container is missing' : 'containers are missing';
    const containersLink = missingContainersCount as number > 0 ? '/MissingContainers' : '/Containers';
    const volumesLink = '/Volumes';
    const bucketsLink = '/Buckets';
    const containersElement = missingContainersCount !== 'N/A'
      ? missingContainersCount as number > 0
        ? (<span>
          <Tooltip
            placement='bottom'
            title={
              missingContainersCount as number > 1000
                ? `1000+ Containers are missing. For more information, go to the Containers page.`
                : `${missingContainersCount} ${containersTooltip}`}>
            <ExclamationCircleFilled className='icon-failure icon-small' />
          </Tooltip>
          <span className='padded-text'>{(containers as number) - (missingContainersCount as number)}/{containers}</span>
        </span>
        )
        :
        (<div>
          <span>{containers.toString()}   </span>
          <Tooltip placement='bottom' title='Number of open containers'>
            <span>({openContainers})</span>
          </Tooltip>
        </div>)
      : (
        <span>
          <Tooltip placement='bottom' title={`Failed to fetch Container Count`}>
            <ExclamationCircleFilled className='icon-failure icon-small' />
            <span className='padded-text'>{missingContainersCount}</span>
          </Tooltip>
        </span>
      )
    const clusterCapacity = `${size(storageReport.capacity - storageReport.remaining)}/${size(storageReport.capacity)}`;
    return (
      <div className='overview-content'>
        <div className='page-header'>
          Overview
          <AutoReloadPanel isLoading={loading} lastRefreshed={lastRefreshed}
            lastUpdatedOMDBDelta={lastUpdatedOMDBDelta} lastUpdatedOMDBFull={lastUpdatedOMDBFull}
            togglePolling={this.autoReload.handleAutoReloadToggle} onReload={this._loadData} omSyncLoad={this.omSyncData} omStatus={omStatus} />
        </div>
        <Row gutter={[10, 20]}>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard
              hoverable loading={loading} title='Datanodes'
              data={datanodesElement} icon='cluster'
              error={datanodes === 'N/A'}
              linkToUrl='/Datanodes' />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard
              hoverable loading={loading} title='Pipelines'
              data={pipelines.toString()} icon='deployment-unit'
              linkToUrl='/Pipelines' />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard
              loading={loading} title='Cluster Capacity' data={clusterCapacity}
              icon='database'
              storageReport={storageReport} />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard
              loading={loading} title='Containers' data={containersElement}
              icon='container'
              error={missingContainersCount as number > 0 || missingContainersCount === 'N/A'} linkToUrl={containersLink} />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Volumes' data={volumes.toString()} icon='inbox' linkToUrl={volumesLink} />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Buckets' data={buckets.toString()} icon='folder-open' linkToUrl={bucketsLink} />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Keys' data={keys.toString()} icon='file-text' />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Deleted Containers' data={deletedContainers.toString()} icon='delete' />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6} className='summary-font'>
            <OverviewCard
              loading={loading}
              title='Open Keys Summary'
              data={openSummaryData}
              icon='file-text'
              linkToUrl='/Om'
              error={openKeysError} />
          </Col>
          <Col xs={24} sm={18} md={12} lg={12} xl={6} className='summary-font'>
            <OverviewCard
              loading={loading}
              title='Pending Deleted Keys Summary'
              data={deletePendingSummaryData}
              icon='delete'
              linkToUrl='/Om'
              error={pendingDeleteKeysError} />
          </Col>
          {scmServiceId &&
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard title="SCM Service" loading={loading} data={scmServiceId} icon='file-text' />
            </Col>
          }
          {omServiceId &&
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard title="Ozone Service ID" loading={loading} data={omServiceId} icon='file-text' linkToUrl='/Om' />
            </Col>
          }
          <Col xs={24} sm={18} md={12} lg={12} xl={6}>
            <OverviewCard loading={loading} title='Decommissioning Datanodes Summary' data={decommissionInfoCount !== undefined ? decommissionInfoCount.toString() : 'NA'} icon='hourglass'
              linkToUrl='/Datanodes' error={decommissionInfoError} />
          </Col>
        </Row>
      </div>
    );
  }
}
