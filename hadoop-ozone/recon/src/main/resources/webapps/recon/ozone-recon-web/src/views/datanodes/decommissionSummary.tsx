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
import { Descriptions, Popover, Tooltip } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import { withRouter } from 'react-router-dom';
import { RouteComponentProps } from 'react-router';
import axios from 'axios';
import { showDataFetchError } from '@/utils/common';

interface IDecommissionSummaryProps extends RouteComponentProps<object> {
  uuid: string;
  isLoading: boolean;
  summaryData: string[];
  record: string[];
}

class DecommissionSummary extends React.Component<IDecommissionSummaryProps> {
  constructor(props = {}) {
    super(props);
    this.state = {
      uuid: this.props.uuid,
      record: this.props.record,
      isLoading: false,
      summaryData: []
    };
  }

  componentDidMount(): void {
    this.setState({
      isLoading: true
    });
    if (this.state?.record && this.state?.summaryData) {
      this.fetchDecommissionSummary(this.state.uuid);
    }
  }

  fetchDecommissionSummary = async (selectedUuid: any) => {
    try {
      const datanodeEndpoint = `/api/v1/datanodes/decommission/info/datanode?uuid=${selectedUuid}`;
      let infoDatanodeResponse = await axios.get(datanodeEndpoint);
      let DatanodesDecommissionInfo = [];
      DatanodesDecommissionInfo = infoDatanodeResponse?.data?.DatanodesDecommissionInfo[0];
      this.setState({
        loading: false,
        summaryData: DatanodesDecommissionInfo
      });
    }
    catch (error) {
      this.setState({
        loading: false,
        summaryData: []
      });
      showDataFetchError(error);
    }
  };

  render() {
    const { summaryData, uuid } = this.state;
    let content;
    
    if ( summaryData && summaryData.length !== 0 && summaryData !== null && summaryData !== undefined && summaryData.datanodeDetails) {   
      const { datanodeDetails, containers, metrics } = summaryData;
      content = (
        <Descriptions size="small" bordered column={1} title={`Decommission Status: DECOMMISSIONING`}>
          <Descriptions.Item label="Datanode"> <b>{datanodeDetails.uuid}</b></Descriptions.Item>
          <Descriptions.Item label="Location">({datanodeDetails.networkLocation}/{datanodeDetails.ipAddress}/{datanodeDetails.hostname})</Descriptions.Item>
          {metrics !== null && metrics !== undefined && Object.keys(metrics).length !== 0 &&
            <>
              {<Descriptions.Item label="Decommissioning Started at">{metrics.decommissionStartTime}</Descriptions.Item>}
              {<Descriptions.Item label="No. of Unclosed Pipelines">{metrics.numOfUnclosedPipelines}</Descriptions.Item>}
              {<Descriptions.Item label="No. of Unclosed Containers">{metrics.numOfUnclosedContainers}</Descriptions.Item>}
              {<Descriptions.Item label="No. of Under-Replicated Containers">{metrics.numOfUnderReplicatedContainers}</Descriptions.Item>}
            </>
          }
          {
            containers && Object.keys(containers).length !== 0 &&
            <>
              {containers.UnderReplicated && containers.UnderReplicated.length > 0 && <Descriptions.Item label="Under-Replicated">{containers.UnderReplicated}</Descriptions.Item>}
              {containers.UnClosed && containers.UnClosed.length > 0 && <Descriptions.Item label="Unclosed">{containers.UnClosed}</Descriptions.Item>}
            </>
          }
        </Descriptions>
      );
    }
    // Need to check summarydata is not empty 
    return (
      <>
        { (summaryData && summaryData.length !== 0) ?
          <>
            <Tooltip title="Detailed Summary of Decomssioned Records.">
              <InfoCircleOutlined />
            </Tooltip>
            <Popover content={content}  placement="top" trigger="hover">
              &nbsp;{uuid}
            </Popover>
          </> : uuid
        }
      </>

    );
  }
}

export default withRouter(DecommissionSummary);
