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

import { Popover, Tag, Typography } from 'antd';
import React from 'react';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';

import './capacity.less';
import { showDataFetchError } from '@/utils/common'
import moment from 'moment';
import CapacityBreakdown from '@/v2/pages/capacity/components/CapacityBreakdown';
import CapacityDetail from '@/v2/pages/capacity/components/CapacityDetail';
import {
  datanodesPendingDeletionDesc,
  nodeSelectorMessage,
  otherUsedSpaceDesc,
  ozoneUsedSpaceDesc,
  totalCapacityDesc
} from '@/v2/pages/capacity/constants/descriptions.constants';
import WrappedInfoIcon from '@/v2/pages/capacity/components/WrappedInfoIcon';
import filesize from 'filesize';
import { InfoCircleOutlined, WarningFilled, CheckCircleFilled } from '@ant-design/icons';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import * as CONSTANTS from '@/v2/constants/capacity.constants';
import { UtilizationResponse, SCMPendingDeletion, OMPendingDeletion, DNPendingDeletion, DataNodeUsage } from '@/v2/types/capacity.types';
import { useAutoReload } from '@/v2/hooks/useAutoReload.hook';

type CapacityState = {
  isDNPending: boolean;
  lastUpdated: number;
};

const Capacity: React.FC<object> = () => {
  const PENDING_POLL_INTERVAL = 5 * 1000;
  const DN_CSV_DOWNLOAD_URL = '/api/v1/pendingDeletion/download';
  const DN_STATUS_URL = '/api/v1/pendingDeletion?component=dn';
  const DOWNLOAD_POLL_TIMEOUT_MS = 10 * 60 * 1000;

  const [state, setState] = React.useState<CapacityState>({
    isDNPending: true,
    lastUpdated: 0
  });

  const storageDistribution = useApiData<UtilizationResponse>(
    '/api/v1/storageDistribution',
    CONSTANTS.DEFAULT_CAPACITY_UTILIZATION,
    {
      retryAttempts: 2,
      onError: (error) => showDataFetchError(error)
    }
  );
  
  const scmPendingDeletes = useApiData<SCMPendingDeletion>(
    '/api/v1/pendingDeletion?component=scm',
    CONSTANTS.DEFAULT_SCM_PENDING_DELETION,
    {
      retryAttempts: 2,
      onError: (error) => showDataFetchError(error)
    }
  );

  const omPendingDeletes = useApiData<OMPendingDeletion>(
    '/api/v1/pendingDeletion?component=om',
    CONSTANTS.DEFAULT_OM_PENDING_DELETION,
    {
      retryAttempts: 2,
      onError: (error) => showDataFetchError(error)
    }
  );

  const dnPendingDeletes = useApiData<DNPendingDeletion>(
    '/api/v1/pendingDeletion?component=dn&limit=15',
    CONSTANTS.DEFAULT_DN_PENDING_DELETION,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  const [selectedDatanode, setSelectedDatanode] = React.useState<string>(storageDistribution.data.dataNodeUsage[0]?.hostName ?? "");

  // Seed selected datanode once data loads so dependent calculations work
  React.useEffect(() => {
    const firstHost = storageDistribution.data.dataNodeUsage[0]?.hostName;
    if (!selectedDatanode && firstHost) {
      setSelectedDatanode(firstHost);
    }
  }, [selectedDatanode, storageDistribution.data.dataNodeUsage]);

  const loadData = () => {
    storageDistribution.refetch();
    scmPendingDeletes.refetch();
    omPendingDeletes.refetch();
    dnPendingDeletes.refetch();
    setState({
      isDNPending: dnPendingDeletes.data.status !== "FINISHED",
      lastUpdated: Number(moment())
    })
  }

  const loadDNData = () => {
    dnPendingDeletes.refetch();
    setState({
      isDNPending: dnPendingDeletes.data.status !== "FINISHED",
      lastUpdated: Number(moment())
    })
  } 

  const autoReload = useAutoReload(loadDNData, PENDING_POLL_INTERVAL);

  const selectedDNDetails: DataNodeUsage & { pendingBlockSize: number } = React.useMemo(() => {
    const selected = storageDistribution.data.dataNodeUsage.find(datanode => datanode.hostName === selectedDatanode)
      ?? storageDistribution.data.dataNodeUsage[0];
    return {
      ...(selected ?? {
        datanodeUuid: "unknown-uuid",
        hostName: "unknown-host",
        capacity: 0,
        used: 0,
        remaining: 0,
        committed: 0,
        minimumFreeSpace: 0,
        reserved: 0
      }),
      ...dnPendingDeletes.data.pendingDeletionPerDataNode?.find(dn => dn.hostName === (selected?.hostName ?? selectedDatanode)) ?? {
        hostName: "unknown-host",
        datanodeUuid: "unknown-uuid",
        pendingBlockSize: 0
      }
    }
  }, [selectedDatanode, storageDistribution.data.dataNodeUsage, dnPendingDeletes.data.pendingDeletionPerDataNode]);

  const waitForDnFinished = async () => {
    const startTime = Date.now();
    while (Date.now() - startTime < DOWNLOAD_POLL_TIMEOUT_MS) {
      const response = await fetch(DN_STATUS_URL);
      if (!response.ok) {
        throw new Error(`Status check failed: ${response.statusText}`);
      }
      const data = await response.json() as DNPendingDeletion;
      if (data.status === "FINISHED") {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, PENDING_POLL_INTERVAL));
    }
    throw new Error('CSV download not ready. Please try again later.');
  };

  const downloadCsv = async (url: string) => {
    try {
      await waitForDnFinished();
      const response = await fetch(url);
      if (!response.ok) {
        showDataFetchError(`CSV download failed: ${response.statusText}`);
        return;
      }
      const contentType = response.headers.get('content-type') ?? '';
      if (!contentType.includes('text/csv')) {
        showDataFetchError('CSV download not ready. Please try again later.');
        return;
      }
      const contentDisposition = response.headers.get('content-disposition');
      const filenameMatch = contentDisposition?.match(/filename="([^"]+)"/);
      if (!filenameMatch) {
        showDataFetchError('CSV download not ready. Please try again later.');
        return;
      }
      const blob = await response.blob();
      const filename = filenameMatch?.[1] ?? 'pending_deletion_stats.csv';
      const link = document.createElement('a');
      link.href = window.URL.createObjectURL(blob);
      link.download = filename;
      link.click();
      window.URL.revokeObjectURL(link.href);
    } catch (error) {
      showDataFetchError((error as Error).message);
    }
  };

  // Poll every 5s until status is FINISHED, then stop
  React.useEffect(() => {
    if (dnPendingDeletes.data.status !== "FINISHED") {
      if (!autoReload.isPolling) {
        autoReload.startPolling(PENDING_POLL_INTERVAL);
      }
      return;
    }

    if (autoReload.isPolling) {
      autoReload.stopPolling();
    }
  }, [
    dnPendingDeletes.data.status,
    autoReload.isPolling,
    autoReload.startPolling,
    autoReload.stopPolling
  ]);

  const dnReportStatus = (
    (dnPendingDeletes.data.totalNodeQueriesFailed ?? 0) > 0
    ? <Popover content={<>
      { (dnPendingDeletes.data.totalNodesQueried ?? 0)
        - (dnPendingDeletes.data.totalNodeQueriesFailed ?? 0)
      } / { (dnPendingDeletes.data.totalNodesQueried ?? 0) } DNs
      </>
    }>
      <WarningFilled style={{ color: '#f6a62eff', marginRight: 8, fontSize: 14 }} />
      Datanodes
    </Popover>
    : <Popover content={
      <>
        {dnPendingDeletes.data.totalNodesQueried ?? 0} / {dnPendingDeletes.data.totalNodesQueried ?? 0} DNs
      </>
    }>
      <CheckCircleFilled style={{ color: '#1ea57a', marginRight: 8, fontSize: 14 }} />
        Datanodes
    </Popover>
  );

  const unusedSpaceBreakdown = (
    <span>
      UNUSED
      <Popover
        title="Unused Space Breakdown"
        placement='topLeft'
        content={
          <div className='unused-space-breakdown'>
            Minimum Free Space
            <Tag color='red'>{filesize(selectedDNDetails.minimumFreeSpace, {round: 1})}</Tag>
            Remaining
            <Tag color='green'>{filesize(selectedDNDetails.remaining, { round: 1})}</Tag>
          </div>
        }
      >
        <InfoCircleOutlined style={{ color: '#2f84d8', fontSize: 12, marginLeft: 4 }} />
      </Popover>
    </span>
  );

  const dnSelectorTitle = (
    <span>
      Node Selector <WrappedInfoIcon title={nodeSelectorMessage} />
    </span>
  );

const openKeyUsageBreakdown = (
    <span>
      OPEN KEYS
      <Popover
        title="Open Key Breakdown"
        placement='topLeft'
        content={
          <div className='openkeys-space-breakdown'>
            Open Key/File Bytes
            <Tag color='red'>{filesize(storageDistribution.data.usedSpaceBreakdown.openKeyBytes?.openKeyAndFileBytes ?? 0, {round: 1})}</Tag>
            Multipart Key Bytes
            <Tag color='green'>{filesize(storageDistribution.data.usedSpaceBreakdown.openKeyBytes?.multipartOpenKeyBytes ?? 0, {round: 1})}</Tag>
          </div>
        }
      >
        <InfoCircleOutlined style={{ color: '#2f84d8', fontSize: 12, marginLeft: 2 }} />
      </Popover>
    </span>
   );

  return (
    <>
      <div className='page-header-v2'>
        Cluster Capacity
        <AutoReloadPanel
          isLoading={dnPendingDeletes.loading}
          lastRefreshed={state.lastUpdated}
          togglePolling={autoReload.handleAutoReloadToggle}
          onReload={loadData} />
      </div>
      <div className='data-container'>
        <Typography.Title level={4} className='section-title'>Cluster</Typography.Title>
        <CapacityBreakdown
          title='Ozone Capacity'
          loading={storageDistribution.loading}
          items={[{
            title: (
              <span>
                TOTAL
                <WrappedInfoIcon title={totalCapacityDesc} />
              </span>
            ),
            value: storageDistribution.data.globalStorage.totalCapacity,
          }, {
            title: 'OZONE USED SPACE',
            value: storageDistribution.data.globalStorage.totalUsedSpace,
            color: '#f4a233'
          }, {
            title: (
              <span>
                OTHER USED SPACE
                <WrappedInfoIcon title={otherUsedSpaceDesc} />
              </span>
            ),
            value: (
              storageDistribution.data.globalStorage.totalCapacity
              - storageDistribution.data.globalStorage.totalFreeSpace
              - storageDistribution.data.globalStorage.totalUsedSpace
            ),
            color: '#11073a'
          }, {
            title: 'CONTAINER PRE-ALLOCATED',
            value: storageDistribution.data.usedSpaceBreakdown.preAllocatedContainerBytes,
            color: '#f47b2d'
          }, {
            title: 'REMAINING SPACE',
            value: storageDistribution.data.globalStorage.totalFreeSpace,
            color: '#4553ee'
          }]}
        />
        <Typography.Title level={4} className='section-title'>Service</Typography.Title>
        <CapacityBreakdown
          title={(
            <span>
              Ozone Used Space
              <WrappedInfoIcon title={ozoneUsedSpaceDesc} />
            </span>
          )}
          loading={storageDistribution.loading}
          items={[{
            title: 'TOTAL',
            value: storageDistribution.data.globalStorage.totalUsedSpace
          }, {
            title: openKeyUsageBreakdown,
            value: storageDistribution.data.usedSpaceBreakdown.openKeyBytes?.totalOpenKeyBytes ?? 0,
            color: '#f47c2d'
          }, {
            title: 'COMMITTED KEYS',
            value: storageDistribution.data.usedSpaceBreakdown.committedKeyBytes,
            color: '#f4a233'
          }, {
            title: (
              dnPendingDeletes.data.status !== "FINISHED" || dnPendingDeletes.loading
              ? (
                <span>
                  PENDING DELETION
                  <WrappedInfoIcon title="DN pending deletion data is not yet available. It will be fetched once the pending deletion scan is finished on all datanodes." />
                </span>
              )
              : 'PENDING DELETION' 
            ),
            value: (
              omPendingDeletes.data.totalSize
              + scmPendingDeletes.data.totalBlocksize
              + (dnPendingDeletes.data.totalPendingDeletionSize ?? 0)
            ),
            color: "#10073b"
          }]}
        />
        <div className='data-breakdown-section'>
          <CapacityDetail
            title='Pending Deletion'
            loading={omPendingDeletes.loading || scmPendingDeletes.loading}
            showDropdown={false}
            dataDetails={[{
              title: 'OZONE MANAGER',
              size: omPendingDeletes.data.totalSize ?? 0,
              breakdown: [{
                label: 'KEYS',
                value: omPendingDeletes.data.pendingKeySize,
                color: '#f4a233'
              }, {
                label: 'DIRECTORIES',
                value: omPendingDeletes.data.pendingDirectorySize,
                color: '#10073b'
              }]
            }, {
              title: 'STORAGE CONTAINER MANAGER',
              size: scmPendingDeletes.data.totalBlocksize,
              breakdown: [{
                label: 'BLOCKS',
                value: scmPendingDeletes.data.totalBlocksize,
                color: '#f4a233'
              }]
            }, {
              title: (
                <span>
                  DATANODES
                  <WrappedInfoIcon title={datanodesPendingDeletionDesc} />
                </span>
              ),
              loading: dnPendingDeletes.loading || dnPendingDeletes.data.status !== "FINISHED",
              size: dnPendingDeletes.data.totalPendingDeletionSize ?? 0,
              breakdown: [{
                label: 'BLOCKS',
                value: dnPendingDeletes.data.totalPendingDeletionSize ?? 0,
                color: '#f4a233'
              }]
            }]} />
          <CapacityDetail
            title={dnReportStatus}
            loading={dnPendingDeletes.loading || dnPendingDeletes.data.status !== "FINISHED"}
            showDropdown={true}
            selectorTitle={dnSelectorTitle}
            downloadUrl={DN_CSV_DOWNLOAD_URL}
            onDownloadClick={() => downloadCsv(DN_CSV_DOWNLOAD_URL)}
            handleSelect={setSelectedDatanode}
            dropdownItems={storageDistribution.data.dataNodeUsage.map(datanode => ({
              label: (
                <>
                  <span>{datanode.hostName}</span>
                  <span className="dn-select-option-uuid">{datanode.datanodeUuid}</span>
                </>
              ),
              value: datanode.hostName,
              key: datanode.datanodeUuid
            }))}
            disabledOpts={
              (dnPendingDeletes.data.pendingDeletionPerDataNode ?? [])
                .filter(dn => dn.pendingBlockSize === -1)
                .map(dn => dn.hostName)
            }
            optsClass={'dn-select-option'}
            dataDetails={[{
              title: 'USED SPACE',
              size: (selectedDNDetails.used ?? 0) + (selectedDNDetails.pendingBlockSize ?? 0),
              breakdown: [{
                label: 'PENDING DELETION',
                value: selectedDNDetails.pendingBlockSize ?? 0,
                color: '#f4a233'
              }, {
                label: 'OZONE USED',
                value: selectedDNDetails.used ?? 0,
                color: '#10073b'
              }]
            }, {
              title: 'FREE SPACE',
              size: (selectedDNDetails.remaining ?? 0) + (selectedDNDetails.committed ?? 0),
              breakdown: [{
                label: unusedSpaceBreakdown,
                value: selectedDNDetails.remaining ?? 0,
                color: '#f4a233'
              }, {
                label: 'OZONE PRE-ALLOCATED',
                value: selectedDNDetails.committed ?? 0,
                color: '#10073b'
              }]
            }]} />
        </div>
      </div>
    </>

  )

};

export default Capacity;
