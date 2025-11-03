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
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';
import { showDataFetchError } from '@/utils/common';
import { AxiosError } from 'axios';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import moment from 'moment';
import CapacityBreakdown from '@/v2/pages/capacity/components/CapacityBreakdown';
import CapacityDetail from '@/v2/pages/capacity/components/CapacityDetail';
import {
  datanodesPendingDeletionDesc,
  otherUsedSpaceDesc,
  ozoneUsedSpaceDesc,
  totalCapacityDesc
} from '@/v2/pages/capacity/constants/descriptions.constants';
import WrappedInfoIcon from '@/v2/pages/capacity/components/WrappedInfoIcon';
import filesize from 'filesize';
import { InfoCircleOutlined } from '@ant-design/icons';

type CapacityState = {
  loading: boolean;
  lastUpdated: number;
};

const Capacity: React.FC<object> = () => {

  const [state, setState] = React.useState<CapacityState>({
    loading: false,
    lastUpdated: 0
  });
  const [globalStorage, setGlobalStorage] = React.useState<GlobalStorage>({
    totalUsedSpace: 0,
    totalFreeSpace: 0,
    totalCapacity: 0,
  });

  // Not being used for now
  // const [globalNamespace, setGlobalNamespace] = React.useState<GlobalNamespace>({
  //   totalUsedSpace: 0,
  //   totalKeys: 0
  // });

  const [usageBreakdown, setUsageBreakdown] = React.useState<UsedSpaceBreakdown>({
    openKeysBytes: 0,
    committedBytes: 0,
    containerPreAllocated: 0,
    deletionPendingBytes: {
      total: 0,
      byStage: {
        DN: {
            pendingBytes: 0
        },
        SCM: {
            pendingBytes: 0
        },
        OM: {
            pendingKeyBytes: 0,
            pendingBytes: 0,
            pendingDirectoryBytes: 0
        }
      }
    }
  });
  const [datanodeUsage, setDatanodeUsage] = React.useState<DataNodeUsage[]>([]);
  const [selectedDatanode, setSelectedDatanode] = React.useState<string | null>(null);

  const cancelSignal = React.useRef<AbortController>();

  const loadData = () => {
    setState(prev => ({
      ...prev,
      loading: true
    }));

    cancelRequests([cancelSignal.current!]);
    const { request, controller } = AxiosGetHelper(
      '/api/v1/storageDistribution',
      cancelSignal.current
    );
    cancelSignal.current = controller;
    
    request.then(response => {
      const utilizationResponse: UtilizationResponse = response.data;
      setGlobalStorage(utilizationResponse.globalStorage);
      // setGlobalNamespace(utilizationResponse.globalNamespace);
      setUsageBreakdown(utilizationResponse.usedSpaceBreakdown);
      setDatanodeUsage(utilizationResponse.dataNodeUsage);
      setSelectedDatanode(utilizationResponse.dataNodeUsage[0].hostName);
      setState({
        loading: false,
        lastUpdated: Number(moment())
      });
    }).catch(error => {
      setState(prev => ({
        ...prev,
        loading: true
      }));
      showDataFetchError((error as AxiosError).toString());
    });
  };

  const  autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  React.useEffect(() => {
    autoReloadHelper.startPolling();
    loadData();

    return (() => {
      autoReloadHelper.stopPolling();
      cancelRequests([cancelSignal.current!]);
    });
  }, []);

  const selectedDNDetails: DataNodeUsage = React.useMemo(() => {
    return datanodeUsage.find(datanode => datanode.hostName === selectedDatanode) ?? {
      datanodeUuid: "unknown-uuid",
      hostName: "unknown-host",
      capacity: 0,
      used: 0,
      remaining: 0,
      committed: 0,
      pendingDeletion: 0
    } as DataNodeUsage;
  }, [selectedDatanode]);

  const unusedSpaceBreakdown = (
    <span>
      UNUSED
      <Popover
        title="Unused Space Breakdown"
        placement='topLeft'
        content={
          <div className='unused-space-breakdown'>
            Minimum Free Space
            <Tag color='red'>To Be Added</Tag>
            Remaining
            <Tag color='green'>{filesize(selectedDNDetails.remaining, { round: 1})}</Tag>
          </div>
        }
      >
        <InfoCircleOutlined style={{ color: '#2f84d8', fontSize: 12, marginLeft: 4 }} />
      </Popover>
    </span>
  )

  return (
    <>
      <div className='page-header-v2'>
        Cluster Capacity
        <AutoReloadPanel
          isLoading={state.loading}
          lastRefreshed={state.lastUpdated}
          togglePolling={autoReloadHelper.handleAutoReloadToggle}
          onReload={loadData} />
      </div>
      <div className='data-container'>
        <Typography.Title level={4} className='section-title'>Cluster</Typography.Title>
        <CapacityBreakdown
          title='Ozone Capacity'
          loading={state.loading}
          items={[{
            title: (
              <span>
                TOTAL
                <WrappedInfoIcon title={totalCapacityDesc} />
              </span>
            ),
            value: globalStorage.totalCapacity,
          }, {
            title: 'OZONE USED SPACE',
            value: globalStorage.totalUsedSpace,
            color: '#f4a233'
          }, {
            title: (
              <span>
                OTHER USED SPACE
                <WrappedInfoIcon title={otherUsedSpaceDesc} />
              </span>
            ),
            value: globalStorage.totalCapacity - globalStorage.totalFreeSpace - globalStorage.totalUsedSpace,
            color: '#11073a'
          }, {
            title: 'CONTAINER PRE-ALLOCATED',
            value: usageBreakdown.containerPreAllocated,
            color: '#f47b2d'
          }, {
            title: 'REMAINING SPACE',
            value: globalStorage.totalFreeSpace,
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
          loading={state.loading}
          items={[{
            title: 'TOTAL',
            value: globalStorage.totalUsedSpace
          }, {
            title: 'OPEN KEYS',
            value: usageBreakdown.openKeysBytes,
            color: '#f47c2d'
          }, {
            title: 'COMMITTED KEYS',
            value: usageBreakdown.committedBytes,
            color: '#f4a233'
          }, {
            title: 'PENDING DELETION',
            value: usageBreakdown.deletionPendingBytes.total,
            color: '#10073b'
          }]}
        />
        <div className='data-breakdown-section'>
          <CapacityDetail
            title='Pending Deletion'
            loading={state.loading}
            showDropdown={false}
            dataDetails={[{
              title: 'OZONE MANAGER',
              size: usageBreakdown.deletionPendingBytes.byStage.OM.totalBytes ?? 0,
              breakdown: [{
                label: 'KEYS',
                value: usageBreakdown.deletionPendingBytes.byStage.OM.pendingKeyBytes,
                color: '#f4a233'
              }, {
                label: 'DIRECTORIES',
                value: usageBreakdown.deletionPendingBytes.byStage.OM.pendingDirectoryBytes,
                color: '#10073b'
              }]
            }, {
              title: 'STORAGE CONTAINER MANAGER',
              size: usageBreakdown.deletionPendingBytes.byStage.SCM.pendingBytes,
              breakdown: [{
                label: 'BLOCKS',
                value: usageBreakdown.deletionPendingBytes.byStage.SCM.pendingBytes,
                color: '#f4a233'
              }]
            }, {
              title: (
                <span>
                  DATANODES
                  <WrappedInfoIcon title={datanodesPendingDeletionDesc} />
                </span>
              ),
              size: usageBreakdown.deletionPendingBytes.byStage.DN.pendingBytes,
              breakdown: [{
                label: 'BLOCKS',
                value: usageBreakdown.deletionPendingBytes.byStage.DN.pendingBytes,
                color: '#f4a233'
              }]
            }]} />
          <CapacityDetail
            title='Datanode'
            loading={state.loading}
            showDropdown={true}
            handleSelect={setSelectedDatanode}
            dropdownItems={datanodeUsage.map(datanode => datanode.hostName)}
            dataDetails={[{
              title: 'USED SPACE',
              size: (selectedDNDetails.used ?? 0) + (selectedDNDetails.pendingDeletion ?? 0),
              breakdown: [{
                label: 'PENDING DELETION',
                value: selectedDNDetails.pendingDeletion ?? 0,
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
