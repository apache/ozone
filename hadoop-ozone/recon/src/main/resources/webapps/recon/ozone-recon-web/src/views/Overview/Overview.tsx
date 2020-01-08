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
import {Row, Col, Icon} from 'antd';
import OverviewCard from 'components/OverviewCard/OverviewCard';
import axios from 'axios';
import prettyBytes from 'pretty-bytes';
import './Overview.less';
import {getCapacityPercent} from "../../utils/common";

interface OverviewState {
  loading: boolean;
  datanodes: string;
  pipelines: string;
  clusterCapacity: string;
  clusterUsedPercent: number;
  containers: string;
  volumes: string;
  buckets: string;
  keys: string;
}

export class Overview extends React.Component<any, OverviewState> {

  constructor(props: any) {
    super(props);
    this.state = {
      loading: false,
      datanodes: '',
      pipelines: '',
      clusterCapacity: '',
      clusterUsedPercent: 0,
      containers: '',
      volumes: '',
      buckets: '',
      keys: ''
    }
  }

  componentDidMount(): void {
    this.setState({
      loading: true
    });
    axios.get('/api/v1/stats').then(response => {
      const stats = response.data;
      const clusterUsedPercent = getCapacityPercent(stats.capacity.used, stats.capacity.total);
      this.setState({
        loading: false,
        datanodes: `${stats.datanodes.healthy}/${stats.datanodes.total}`,
        clusterCapacity: `${prettyBytes(stats.capacity.used)}/${prettyBytes(stats.capacity.total)}`,
        clusterUsedPercent,
        pipelines: stats.pipelines,
        containers: stats.containers,
        volumes: stats.volumes,
        buckets: stats.buckets,
        keys: stats.keys
      });
    });
  }

  render() {
    const {loading, datanodes, pipelines, clusterCapacity, clusterUsedPercent, containers, volumes, buckets, keys} = this.state;
    const datanodesElement = <span>
      <Icon type="check-circle" theme="filled" className="icon-success icon-small"/> {datanodes} <span className="ant-card-meta-description meta">HEALTHY</span>
    </span>;
    return (
        <div className="overview-content">
          <Row gutter={[25, 25]}>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Datanodes"} data={datanodesElement} icon={"cluster"} hoverable={true}
                            linkToUrl="/Datanodes"/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Pipelines"} data={pipelines} icon={"deployment-unit"} hoverable={true}
                            linkToUrl="/Pipelines"/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Cluster Capacity (Used/Total)"} data={clusterCapacity} icon={"database"}
                            capacityPercent={clusterUsedPercent}/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Containers"} data={containers} icon={"container"}/>
            </Col>
          </Row>
          <Row gutter={[25, 25]}>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Volumes"} data={volumes} icon={"inbox"}/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Buckets"} data={buckets} icon={"folder-open"}/>
            </Col>
            <Col xs={24} sm={18} md={12} lg={12} xl={6}>
              <OverviewCard loading={loading} title={"Keys"} data={keys} icon={"file-text"}/>
            </Col>
          </Row>
        </div>
    );
  }
}
