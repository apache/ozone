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
import {Row, Col, Icon, Button, Input} from 'antd';
import {DetailPanel} from 'components/rightDrawer/rightDrawer';
import {PathForm} from 'components/pathForm/pathForm';
import * as Plotly from 'plotly.js';
import {showDataFetchError} from 'utils/common';
import './diskUsage.less';
import {AutoReloadHelper} from 'utils/autoReloadHelper';
import AutoReloadPanel from 'components/autoReloadPanel/autoReloadPanel';

const DISPLAY_LIMIT = 20;
const OTHER_PATH_NAME = "Other Objects";
const DIRECT_KEYS = "Direct Keys";

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
  showPanel: boolean;
  panelKeys: string[];
  panelValues: string[];
  returnPath: string;
  inputPath: "";
}

export class DiskUsage extends React.Component<Record<string, object>, IDUState> {
  autoReload: AutoReloadHelper;

    constructor(props = {}) {
        super(props);
        this.state = {
            isLoading: false,
            duResponse: [],
            plotData: [],
            showPanel: false,
            panelKeys: [],
            panelValues: [],
            returnPath: "/",
            inputPath: "/"
        };
        this.autoReload = new AutoReloadHelper(this._loadData);
    }

    byteToSize = (bytes) => {
       var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
       if (bytes === 0) return '0 Byte';
       var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
       return Math.round(bytes / Math.pow(1024, i), 2) + ' ' + sizes[i];
    };

    handleChange = (e) => {
        this.setState({inputPath: e.target.value, showPanel: false});
    }

    handleSubmit = (e) => {
        this.updatePieChart(this.state.inputPath);
    }

    // Take the request path, make a DU request, inject response
    // into the pie chart
    updatePieChart = (path) => {
        this.setState({
          isLoading: true
        });
        const duEndpoint = "/api/v1/namespace/du?path=" + path;
        axios.get(duEndpoint).then(response => {
            const duResponse: IDUResponse[] = response.data;
            const status = duResponse.status;
            if (status !== "OK") {
                showDataFetchError("Invalid Path: " + path);
                return;
            }

            const dataSize = duResponse.size;
            var subpaths: IDUSubpath[] = duResponse.subPaths;

            subpaths.sort((a, b) => (a.size < b.size) ? 1 : -1);

            // show all direct keys as a single block
            // Do not enable "&files=true" on UI
            if (duResponse.sizeDirectKey > 0) {
                const directKey = {"path": DIRECT_KEYS, "size": duResponse.sizeDirectKey};
                subpaths.push(directKey);
            }
            // Only show 20 blocks with the most DU,
            // other blocks are merged as a single block
            if (subpaths.length > DISPLAY_LIMIT) {
                subpaths = subpaths.slice(0, DISPLAY_LIMIT);
                var topSize = 0;
                for (var i = 0; i < DISPLAY_LIMIT; ++i) {
                    topSize += subpaths[i].size;
                }
                const otherSize = dataSize - topSize;
                const other: IDUSubpath = {"path": OTHER_PATH_NAME, "size": otherSize};
                subpaths.push(other);
            }

            var pathLabels = subpaths.map(subpath => {
                return subpath.path;
            });

            var percentage = subpaths.map(subpath => {
                return subpath.size / dataSize;
            });

            var sizeStr = subpaths.map(subpath => {
                return this.byteToSize(subpath.size);
            });
            this.setState({
                // normalized path
                isLoading: false,
                showPanel: false,
                inputPath: duResponse.path,
                returnPath: duResponse.path,
                duResponse: duResponse,
                plotData: [{
                  type: 'pie',
                  hole: .2,
                  values: percentage,
                  labels: pathLabels,
                  text: sizeStr,
                  textinfo: 'label+percent',
                  hovertemplate: 'Total Data Size: %{text}<extra></extra>'
                }]
            });

        }).catch(error => {
          this.setState({
            isLoading: false
          });
          showDataFetchError(error.toString());
        });
    };

    componentDidMount(): void {
        this.setState({
          isLoading: true
        });
        // By default render the DU for root path
        this.updatePieChart("/");
    }

    clickPieSection(e): void {
        const path = e.points[0].label;
        if (path === OTHER_PATH_NAME || path === DIRECT_KEYS) {
            return;
        }
        this.updatePieChart(path);
    }

    // show the right side panel that display metadata details of path
    showMetadataDetails(e, path): void {
        const summaryEndpoint = "/api/v1/namespace/summary?path=" + path;
        var keys = ["Type"];
        var values = [summaryResponse.type];
        axios.get(summaryEndpoint).then(response => {
            const summaryResponse = response.data;

            if (summaryResponse.status !== "OK") {
              showDataFetchError("Invalid path: " + path);
              return;
            }
            if (summaryResponse.numVolume !== -1) {
                keys.push("Volumes");
                values.push(summaryResponse.numVolume);
            } else if (summaryResponse.numBucket !== -1) {
                keys.push("Buckets");
                values.push(summaryResponse.numBucket);
            } else if (summaryResponse.numDir !== -1) {
                keys.push("Directories");
                values.push(summaryResponse.numDir);
            } else if (summaryResponse.numKey !== -1) {
                keys.push("Keys");
                values.push(summaryResponse.numKey);
            }
            // show the right drawer
            this.setState({
              showPanel: true,
              panelKeys: keys,
              panelValues: values
            })

        }).catch(error => {
          this.setState({
            isLoading: false,
            showPanel: false
          });
          showDataFetchError(error.toString());
        });

        const quotaEndpoint = "/api/v1/namespace/quota?path=" + path;
        axios.get(quotaEndpoint).then(response => {
            const quotaResponse = response.data;

            if (quotaResponse.status === "PATH_NOT_FOUND") {
              showDataFetchError("Invalid path: " + path);
              return;
            }

            // if quota request not applicable for this path, silently return
            if (quotaResponse.status === "TYPE_NOT_APPLICABLE") {
              return;
            }
            // append quota information
            // In case the object's quota isn't set
            if (quotaResponse.allowed !== -1) {
                keys.push("Quota Allowed");
                values.push(this.byteToSize(quotaResponse.allowed);
            }
            keys.push("Quota Used");
            values.push(this.byteToSize(quotaResponse.used);
            this.setState({
              showPanel: true,
              panelKeys: keys,
              panelValues: values
            })

        }).catch(error => {
          this.setState({
            isLoading: false,
            showPanel: false
          });
          showDataFetchError(error.toString());
        });
    }

    render() {
    const {plotData, duResponse, returnPath, panelKeys, panelValues, showPanel, isLoading, inputPath} = this.state;
      return (
        <div className='du-container'>
            <div className='page-header'>
              Disk Usage
            </div>
            {isLoading ? <span><Icon type='loading'/> Loading...</span> :
            <div className='content-div'>
            <Row>
            <Col>
                <div className='input-bar'>
                <h3>Path</h3>
                    <form className='input' onSubmit={this.handleSubmit} id="input-form">
                      <Input placeholder="/" value={inputPath} onChange={this.handleChange} />
                    </form>
                </div>
                <div className='metadata-button'>
                    <Button onClick={(e) => this.showMetadataDetails(e, returnPath)} type='primary'>
                    <b>Show Metadata Summary</b>
                    </Button>
                </div>
            </Col>
            </Row>
            {(duResponse.size > 0) ?
            <Row>
            <Plot onClick={(e) => this.clickPieSection(e)}
                  data={plotData}
                  layout={
                    {
                      width: 800,
                      height: 750,
                      showlegend: false,
                      title: 'Disk Usage for ' + returnPath + ' (Total Size: ' + this.byteToSize(duResponse.size) + ')'
                    }
                  }/>
                  <DetailPanel path={returnPath} keys={panelKeys} values={panelValues} visible={showPanel}/>
              </Row>
              : <div><br></br><h3>This object is empty. Add files to it to see a visualization on disk usage.</h3></div>}
            </div>}
          </div>
        );
    }
}