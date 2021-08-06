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
import axios from 'axios';
import Plot from 'react-plotly.js';
import {Row, Col} from 'antd';
import {DetailPanel} from 'components/rightDrawer/rightDrawer';
import {PathForm} from 'components/pathForm/pathForm';
import * as Plotly from 'plotly.js';
import {showDataFetchError} from 'utils/common';
import './diskUsage.less';
import {AutoReloadHelper} from 'utils/autoReloadHelper';
import AutoReloadPanel from 'components/autoReloadPanel/autoReloadPanel';

interface IDUSubpath {
  path: string;
  size: number;
  sizeWithReplica: number;
}

interface IDUResponse {
  status: string;
  path: string;
  subPathCount: number;
  size: number;
  sizeWithReplica: number;
  subPaths: IDUSubpath[];
  sizeDirectKey: number;
}

interface IDUState {
  isLoading: boolean;
  duResponse: IDUResponse[];
  plotData: Plotly.Data[];
  path: string;
}

export class DiskUsage extends React.Component<Record<string, object>, IDUState> {
  autoReload: AutoReloadHelper;

    constructor(props = {}) {
        super(props);
        this.state = {
            isLoading: false,
            duResponse: [],
            plotData: [],
            path: "/"
        };
        this.autoReload = new AutoReloadHelper(this._loadData);
    }

    byteToSize = (bytes) => {
       var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
       if (bytes == 0) return '0 Byte';
       var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
       return Math.round(bytes / Math.pow(1024, i), 2) + ' ' + sizes[i];
    };

    updateHeatmap = () => {
        const {duResponse, path} = this.state;
        const status = duResponse.status;
        if (status !== "OK") {
            showDataFetchError("Invalid Path: " + path);
            return;
        }

        const dataSize = duResponse.size;
        const totalDU = duResponse.sizeWithReplica;
        const subpaths = duResponse.subPaths;
        const pathLabels = subpaths.map(subpath => {
            return subpath.path;
        });

        const percentage = subpaths.map(subpath => {
            return subpath.sizeWithReplica / totalDU;
        });

        const sizeStr = subpaths.map(subpath => {
            return this.byteToSize(subpath.sizeWithReplica);
        });

        this.setState({
            plotData: [{
              type: 'pie',
              hole: .2,
              values: percentage,
              labels: pathLabels,
              text: sizeStr,
              textinfo: 'label+percent',
              hovertemplate: 'Disk Space Consumed: %{text}<extra></extra>'
            }]
        });
    };

    componentDidMount(): void {
        this.setState({
          isLoading: true
        });
        const rootPath = '/api/v1/namespace/du?path=/';
        axios.get(rootPath).then(response => {
            const duResponse: IDUResponse[] = response.data;
            console.log(duResponse);

            this.setState({
                isLoading: false,
                path: duResponse.path,
                duResponse
            }, () => {
                this.updateHeatmap();
            });

        }).catch(error => {
          this.setState({
            isLoading: false
          });
          showDataFetchError(error.toString());
        });
    }

    showDetailPanel(e): void {
        const path = e.points[0].label;
        const summaryEndpoint = "/api/v1/namespace/summary?path=" + path;
        axios.get(summaryEndpoint).then(response => {
            const summaryResponse = response.data;

            if (summaryResponse.status !== "OK") {
              showDataFetchError("Invalid path: " + path);
              return;
            }

            const text = summaryResponse.type + " " + summaryResponse.numBucket + " " + summaryResponse.numDir + " " + summaryResponse.numKey;
            alert(text);

        }).catch(error => {
          this.setState({
            isLoading: false
          });
          showDataFetchError(error.toString());
        });
    }

    render() {
    const {plotData, isLoading, duResponse, path} = this.state;
      return (
        <div className='du-container'>
            <div className='page-header'>
              Disk Usage
            </div>
            <div className='content-div'>
            <Row>
                <div className='filter-block'>
                <h3>Path</h3>
                    <PathForm />
                </div>
            </Row>

            <Row>
                <Plot onClick={(e) => this.showDetailPanel(e)}
                  data={plotData}
                  layout={
                    {
                      width: 750,
                      height: 750,
                      title: 'Disk Usage for ' + path
                    }
                  }/>
                  <DetailPanel/ >
              </Row>
            </div>
          </div>
        );
    }
}