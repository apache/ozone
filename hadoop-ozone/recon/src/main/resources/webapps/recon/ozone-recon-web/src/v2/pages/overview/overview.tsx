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

import React, { useState, useRef, useEffect } from 'react';
import { Row, Col } from 'antd';
import moment from 'moment';
import filesize from 'filesize';

import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import OverviewSimpleCard from '@/v2/components/cards/overviewSimpleCard';
import OverviewSummaryCard from '@/v2/components/cards/overviewSummaryCard';
import OverviewHealthCard from '@/v2/components/cards/overviewHealthCard';
import CapacityBreakdown from '@/v2/pages/capacity/components/CapacityBreakdown';
import WrappedInfoIcon from '@/v2/pages/capacity/components/WrappedInfoIcon';

import { AxiosGetHelper } from '@/utils/axiosRequestHelper';
import { showDataFetchError } from '@/utils/common';
import { cancelRequests } from '@/utils/axiosRequestHelper';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { useAutoReload } from '@/v2/hooks/useAutoReload.hook';

import { ClusterStateResponse, KeysSummary, OverviewState, TaskStatus } from '@/v2/types/overview.types';

import * as CONSTANTS from '@/v2/constants/overview.constants';
import { otherUsedSpaceDesc, totalCapacityDesc } from '@/v2/pages/capacity/constants/descriptions.constants';


import './overview.less';
import { Link } from 'react-router-dom';

// ------------- Helper Functions -------------- //
const size = filesize.partial({ round: 1 });

const getSummaryTableValue = (
  value: number | string | undefined | null,
  colType: 'value' | undefined = undefined
): string => {
  if (value === null || value === undefined) return 'N/A';
  if (typeof value === 'string' && value.trim() === '') return 'N/A';
  if (colType === 'value') return String(value as string)
  return size(value as number)
}

// ------------- Main Component -------------- //
const Overview: React.FC<{}> = () => {
  const cancelOMDBSyncSignal = useRef<AbortController>();
  const [state, setState] = useState<OverviewState>({
    omStatus: '',
    lastRefreshed: 0
  });

  // Individual API calls using custom hook (no auto-refresh)
  const clusterState = useApiData<ClusterStateResponse>(
    '/api/v1/clusterState',
    CONSTANTS.DEFAULT_CLUSTER_STATE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  const taskStatus = useApiData<TaskStatus[]>(
    '/api/v1/task/status',
    CONSTANTS.DEFAULT_TASK_STATUS,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  const openKeysSummary = useApiData<KeysSummary & { totalOpenKeys: number}>(
    '/api/v1/keys/open/summary',
    CONSTANTS.DEFAULT_OPEN_KEYS_SUMMARY,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  const deletePendingKeysSummary = useApiData<KeysSummary & { totalDeletedKeys: number}>(
    '/api/v1/keys/deletePending/summary',
    CONSTANTS.DEFAULT_DELETE_PENDING_KEYS_SUMMARY,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  const omDBDeltaObject = taskStatus.data?.find((item: TaskStatus) => item.taskName === 'OmDeltaRequest');
  const omDBFullObject = taskStatus.data?.find((item: TaskStatus) => item.taskName === 'OmSnapshotRequest');

  const loadOverviewPageData = () => {
    clusterState.refetch();
    taskStatus.refetch();
    openKeysSummary.refetch();
    deletePendingKeysSummary.refetch();
    setState(prev => ({ ...prev, lastRefreshed: Number(moment()) }));
  };
  
  const autoReload = useAutoReload(loadOverviewPageData);

  // OM DB Sync function
  const syncOmData = () => {
    const { request, controller } = AxiosGetHelper(
      '/api/v1/triggerdbsync/om',
      cancelOMDBSyncSignal.current,
      'OM-DB Sync request cancelled because data was updated'
    );
    cancelOMDBSyncSignal.current = controller;

    request.then(omStatusResponse => {
      const omStatus = omStatusResponse.data;
      setState(prev => ({ ...prev, omStatus }));
    }).catch((error: Error) => {
      showDataFetchError(error.toString());
    });
  };

  useEffect(() => {
    return () => {
      cancelRequests([cancelOMDBSyncSignal.current!]);
    };
  }, []);

  const capacityCardTitle = (
    <div className='card-title-div'>
      <span>
        Ozone Capacity
        <WrappedInfoIcon title="Cluster Capacity fetches data from Datanode Reports and includes the replicated data-size"/>
      </span>
      <Link
        to={{
          pathname: "/Capacity"
        }}
        style={{
          fontWeight: 400
        }}>View More</Link>
    </div>
  );

  const loading = clusterState.loading || taskStatus.loading || openKeysSummary.loading || deletePendingKeysSummary.loading;
  const {
    healthyDatanodes,
    totalDatanodes,
    containers,
    missingContainers,
    storageReport,
    volumes,
    buckets,
    keys,
    pipelines,
    deletedContainers,
    openContainers,
    omServiceId,
    scmServiceId
  } = clusterState.data;
  const {
    totalReplicatedDataSize: openSummarytotalRepSize,
    totalUnreplicatedDataSize: openSummarytotalUnrepSize,
    totalOpenKeys: openSummarytotalOpenKeys,
  } = openKeysSummary.data ?? {};
  const {
    totalReplicatedDataSize: deletePendingSummarytotalRepSize,  
    totalUnreplicatedDataSize: deletePendingSummarytotalUnrepSize,
    totalDeletedKeys: deletePendingSummarytotalDeletedKeys
  } = deletePendingKeysSummary.data ?? {};

  return (
    <>
      <div className='page-header-v2'>
        Overview
        <AutoReloadPanel isLoading={loading} lastRefreshed={state.lastRefreshed}
          lastUpdatedOMDBDelta={omDBDeltaObject?.lastUpdatedTimestamp} lastUpdatedOMDBFull={omDBFullObject?.lastUpdatedTimestamp}
          togglePolling={autoReload.handleAutoReloadToggle} onReload={loadOverviewPageData} omSyncLoad={syncOmData} omStatus={state.omStatus} />
      </div>
      <div className='data-container'>
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
          <Col xs={24} sm={24} md={24} lg={12} xl={12}>
            <OverviewHealthCard
              title='Datanodes'
              loading={clusterState.loading}
              available={healthyDatanodes ?? 'N/A'}
              total={totalDatanodes ?? 'N/A'}
              error={clusterState.error}
              linkToUrl='/Datanodes' />
          </Col>
          <Col xs={24} sm={24} md={24} lg={12} xl={12}>
            <OverviewHealthCard
              title='Containers'
              loading={clusterState.loading}
              available={(containers ?? 0) - (missingContainers ?? 0)}
              total={containers ?? 'N/A'}
              error={clusterState.error}
              linkToUrl='/Containers' />
          </Col>
          <Col xs={24} sm={24} md={24} lg={24} xl={24}>
            <CapacityBreakdown
              title={capacityCardTitle}
              loading={clusterState.loading}
              items={[{
                title: (
                  <span>
                    TOTAL
                    <WrappedInfoIcon title={totalCapacityDesc} />
                  </span>
                ),
                value: storageReport.capacity,
              }, {
                title: 'OZONE USED SPACE',
                value: storageReport.used,
                color: '#f4a233'
              }, {
                title: (
                  <span>
                    OTHER USED SPACE
                    <WrappedInfoIcon title={otherUsedSpaceDesc} />
                  </span>
                ),
                value: (
                  storageReport.capacity
                  - storageReport.remaining
                  - storageReport.used
                ),
                color: '#11073a'
              }, {
                title: 'CONTAINER PRE-ALLOCATED',
                value: storageReport.committed,
                color: '#f47b2d'
              }]}
              error={clusterState.error}
            />
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
          <Col flex="1 0 15%">
            <OverviewSimpleCard
              title='Volumes'
              icon='inbox'
              loading={clusterState.loading}
              data={volumes}
              linkToUrl='/Volumes'
              error={clusterState.error} />
          </Col>
          <Col flex="1 0 15%">
            <OverviewSimpleCard
              title='Buckets'
              icon='folder-open'
              loading={clusterState.loading}
              data={buckets}
              linkToUrl='/Buckets'
              error={clusterState.error} />
          </Col>
          <Col flex="1 0 15%">
            <OverviewSimpleCard
              title='Keys'
              icon='file-text'
              loading={clusterState.loading}
              data={keys}
              error={clusterState.error} />
          </Col>
          <Col flex="1 0 15%">
            <OverviewSimpleCard
              title='Pipelines'
              icon='deployment-unit'
              loading={clusterState.loading}
              data={pipelines}
              linkToUrl='/Pipelines'
              error={clusterState.error} />
          </Col>
          <Col flex="1 0 15%">
            <OverviewSimpleCard
              title='Deleted Containers'
              icon='delete'
              loading={clusterState.loading}
              data={deletedContainers}
              error={clusterState.error} />
          </Col>
          <Col flex="1 0 15%">
            <OverviewSimpleCard
              title='Open Containers'
              icon='container'
              loading={clusterState.loading}
              data={openContainers}
              error={clusterState.error} />
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
          <Col xs={24} sm={24} md={24} lg={12} xl={12}>
            <OverviewSummaryCard
              title='Open Keys Summary'
              loading={openKeysSummary.loading}
              columns={[
                {
                  title: 'Name',
                  dataIndex: 'name',
                  key: 'name'
                },
                {
                  title: 'Size',
                  dataIndex: 'value',
                  key: 'size',
                  align: 'right'
                }
              ]}
              tableData={[
                {
                  key: 'total-replicated-data',
                  name: 'Total Replicated Data',
                  value: getSummaryTableValue(openSummarytotalRepSize)
                },
                {
                  key: 'total-unreplicated-data',
                  name: 'Total Unreplicated Data',
                  value: getSummaryTableValue(openSummarytotalUnrepSize)
                },
                {
                  key: 'open-keys',
                  name: 'Open Keys',
                  value: getSummaryTableValue(
                    openSummarytotalOpenKeys,
                    'value'
                  )
                }
              ]}
              linkToUrl='/Om'
              state={{activeTab: '2'}}
              error={openKeysSummary.error} />
          </Col>
          <Col xs={24} sm={24} md={24} lg={12} xl={12}>
            <OverviewSummaryCard
              title='Delete Pending Keys Summary'
              loading={deletePendingKeysSummary.loading}
              columns={[
                {
                  title: 'Name',
                  dataIndex: 'name',
                  key: 'name'
                },
                {
                  title: 'Size',
                  dataIndex: 'value',
                  key: 'size',
                  align: 'right'
                }
              ]}
              tableData={[
                {
                  key: 'total-replicated-data',
                  name: 'Total Replicated Data',
                  value: getSummaryTableValue(deletePendingSummarytotalRepSize)
                },
                {
                  key: 'total-unreplicated-data',
                  name: 'Total Unreplicated Data',
                  value: getSummaryTableValue(deletePendingSummarytotalUnrepSize)
                },
                {
                  key: 'delete-pending-keys',
                  name: 'Delete Pending Keys',
                  value: getSummaryTableValue(
                    deletePendingSummarytotalDeletedKeys,
                    'value'
                  )
                }
              ]}
              linkToUrl='/Om'
              state={{activeTab: '3'}}
              error={deletePendingKeysSummary.error} />
          </Col>
        </Row>
        <span style={{ paddingLeft: '8px' }}>
          <span style={{ color: '#6E6E6E' }}>Ozone Service ID:&nbsp;</span>
          {omServiceId}
        </span>
        <span style={{ marginLeft: '12px', marginRight: '12px' }}> | </span>
        <span>
          <span style={{ color: '#6E6E6E' }}>SCM ID:&nbsp;</span>
          {scmServiceId}
        </span>
      </div>
    </>
  );
}

export default Overview;
