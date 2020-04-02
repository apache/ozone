/**
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
import {Row, Col, Icon, Tooltip} from 'antd';
import OverviewCard from 'components/OverviewCard/OverviewCard';
import axios from 'axios';
import prettyBytes from 'pretty-bytes';
import './Overview.less';
import {StorageReport} from "types/datanode.types";
import {MissingContainersResponse} from "../MissingContainers/MissingContainers";

interface ClusterStateResponse {
  totalDatanodes: number;
  healthyDatanodes: number;
  pipelines: number;
  storageReport: StorageReport;
  containers: number;
  volumes: number;
  buckets: number;
  keys: number;
}

interface OverviewState {
  loading: boolean;
  datanodes: string;
  pipelines: number;
  storageReport: StorageReport;
  containers: number;
  volumes: number;
  buckets: number;
  keys: number;
  missingContainersCount: number;
}

export class Overview extends React.Component<any, OverviewState> {

  constructor(props: any) {
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
      missingContainersCount: 0
    }
  }

  componentDidMount(): void {
    this.setState({
      loading: true
    });
    axios.all([
        axios.get('/api/v1/clusterState'),
        axios.get('/api/v1/containers/missing')
    ]).then(axios.spread((clusterStateResponse, missingContainersResponse) => {
      const clusterState: ClusterStateResponse = clusterStateResponse.data;
      const missingContainers: MissingContainersResponse = missingContainersResponse.data;
      const missingContainersCount = missingContainers.totalCount;
      this.setState({
        loading: false,
        datanodes: `${clusterState.healthyDatanodes}/${clusterState.totalDatanodes}`,
        storageReport: clusterState.storageReport,
        pipelines: clusterState.pipelines,
        containers: clusterState.containers,
        volumes: clusterState.volumes,
        buckets: clusterState.buckets,
        keys: clusterState.keys,
        missingContainersCount
      });
    }));
  }

  render() {
    const {loading, datanodes, pipelines, storageReport, containers, volumes, buckets,
      keys, missingContainersCount} = this.state;
    const datanodesElement = <span>
      <Icon type="check-circle" theme="filled" className="icon-success icon-small"/> {datanodes} <span className="ant-card-meta-description meta">HEALTHY</span>
    </span>;
    const containersTooltip = missingContainersCount === 1 ? "container is missing" : "containers are missing";
    const containersLink = missingContainersCount > 0 ? '/MissingContainers' : '';
    const containersElement = missingContainersCount > 0 ?
        <span>
          <Tooltip placement="bottom" title={`${missingContainersCount} ${containersTooltip}`}>
            <Icon type="exclamation-circle" theme="filled" className="icon-failure icon-small"/>
          </Tooltip>
          <span className="padded-text">{containers-missingContainersCount}/{containers}</span>
        </span>
        : containers.toString();
    const clusterCapacity = `${prettyBytes(storageReport.capacity - storageReport.remaining)}/${prettyBytes(storageReport.capacity)}`;
    return (
        <div className="overview-content">
          <Row gutter={[25, 25]}>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Datanodes"} data={datanodesElement} icon={"cluster"} hoverable={true}
                            linkToUrl="/Datanodes"/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Pipelines"} data={pipelines.toString()} icon={"deployment-unit"} hoverable={true}
                            linkToUrl="/Pipelines"/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Cluster Capacity"} data={clusterCapacity} icon={"database"}
                            storageReport={storageReport}/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Containers"} data={containersElement} icon={"container"}
                            error={missingContainersCount > 0} linkToUrl={containersLink}/>
            </Col>
          </Row>
          <Row gutter={[25, 25]}>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Volumes"} data={volumes.toString()} icon={"inbox"}/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Buckets"} data={buckets.toString()} icon={"folder-open"}/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Keys"} data={keys.toString()} icon={"file-text"}/>
            </Col>
          </Row>
        </div>
    );
  }
}
