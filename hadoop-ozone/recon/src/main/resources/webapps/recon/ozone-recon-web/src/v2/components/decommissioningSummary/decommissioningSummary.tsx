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

import React, { useEffect } from 'react';
import { AxiosError } from 'axios';
import { Descriptions, Popover, Result } from 'antd';
import { SummaryData } from '@/v2/types/datanode.types';
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';
import { showDataFetchError } from '@/utils/common';
import Spin from 'antd/es/spin';

type DecommisioningSummaryProps = {
  uuid: string;
}

type DecommisioningSummaryState = {
  loading: boolean;
  summaryData: SummaryData | Record<string, unknown>;
};

function getDescriptions(summaryData: SummaryData): React.ReactElement {
  const {
    datanodeDetails: {
      uuid,
      networkLocation,
      ipAddress,
      hostName
    },
    containers: { UnderReplicated, UnClosed },
    metrics: {
      decommissionStartTime,
      numOfUnclosedPipelines,
      numOfUnclosedContainers,
      numOfUnderReplicatedContainers
    }
  } = summaryData;
  return (
    <Descriptions size="small" bordered column={1} title={`Decommission Status: DECOMMISSIONING`}>
      <Descriptions.Item label="Datanode"> <b>{uuid}</b></Descriptions.Item>
      <Descriptions.Item label="Location">({networkLocation}/{ipAddress}/{hostName})</Descriptions.Item>
      <Descriptions.Item label="Decommissioning Started at">{decommissionStartTime}</Descriptions.Item>
      <Descriptions.Item label="No. of Unclosed Pipelines">{numOfUnclosedPipelines}</Descriptions.Item>
      <Descriptions.Item label="No. of Unclosed Containers">{numOfUnclosedContainers}</Descriptions.Item>
      <Descriptions.Item label="No. of Under-Replicated Containers">{numOfUnderReplicatedContainers}</Descriptions.Item>
      <Descriptions.Item label="Under-Replicated">{UnderReplicated}</Descriptions.Item>
      <Descriptions.Item label="Unclosed">{UnClosed}</Descriptions.Item>
    </Descriptions>
  );
}


const DecommissionSummary: React.FC<DecommisioningSummaryProps> = ({
  uuid = ''
}) => {
  const [state, setState] = React.useState<DecommisioningSummaryState>({
    summaryData: {},
    loading: false
  });
  const cancelSignal = React.useRef<AbortController>();
  let content = (
    <Spin
      size='large'
      style={{ margin: '15px 15px 10px 15px' }} />
  );

  async function fetchDecommissionSummary(selectedUuid: string) {
    setState({
      ...state,
      loading: true
    });
    try {
      const { request, controller } = AxiosGetHelper(
        `/api/v1/datanodes/decommission/info/datanode?uuid=${selectedUuid}`,
        cancelSignal.current
      );
      cancelSignal.current = controller;
      const datanodesInfoResponse = await request;
      setState({
        ...state,
        loading: false,
        summaryData: datanodesInfoResponse?.data?.DatanodesDecommissionInfo[0] ?? {}
      });
    } catch (error) {
      setState({
        ...state,
        loading: false,
        summaryData: {}
      });
      showDataFetchError((error as AxiosError).toString());
      content = (
        <Result
          status='error'
          title='Unable to fetch Decommission Summary data'
          className='decommission-summary-result' />
      )
    }
  }

  useEffect(() => {
    fetchDecommissionSummary(uuid);
    return (() => {
      cancelRequests([cancelSignal.current!]);
    })
  }, []);

  const { summaryData } = state;
  if (summaryData?.datanodeDetails
      && summaryData?.metrics
      && summaryData?.containers
  ) {
    content = getDescriptions(summaryData as SummaryData);
  }

  return (
    <Popover
      content={content}
      placement="rightTop" trigger="hover">
      &nbsp;{uuid}
    </Popover>
  );

}

export default DecommissionSummary;
