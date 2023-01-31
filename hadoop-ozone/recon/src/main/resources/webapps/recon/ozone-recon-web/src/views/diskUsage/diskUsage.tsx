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
import {Row, Col, Icon, Button, Input, Menu, Dropdown} from 'antd';
import {DetailPanel} from 'components/rightDrawer/rightDrawer';
import * as Plotly from 'plotly.js';
import {showDataFetchError} from 'utils/common';
import './diskUsage.less';
import moment from 'moment';

const DEFAULT_DISPLAY_LIMIT = 10;
const OTHER_PATH_NAME = 'Other Objects';

interface IDUSubpath {
  path: string;
  size: number;
  sizeWithReplica: number;
  isKey: boolean;
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
  inputPath: string;
  displayLimit: number;
}

export class DiskUsage extends React.Component<Record<string, object>, IDUState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      isLoading: false,
      duResponse: [],
      plotData: [],
      showPanel: false,
      panelKeys: [],
      panelValues: [],
      returnPath: '/',
      inputPath: '/',
      displayLimit: DEFAULT_DISPLAY_LIMIT
    };
  }

  byteToSize = (bytes, decimals) => {
    if (bytes === 0) {
      return '0 Bytes';
    }

    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];

    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return `${Number.parseFloat((bytes / (k ** i)).toFixed(dm))} ${sizes[i]}`;
  };

  handleChange = e => {
    this.setState({inputPath: e.target.value, showPanel: false});
  };

  handleSubmit = _e => {
    // Avoid empty request trigger 400 response
    if (!this.state.inputPath) {
      this.updatePieChart('/', DEFAULT_DISPLAY_LIMIT);
      return;
    }

    this.updatePieChart(this.state.inputPath, DEFAULT_DISPLAY_LIMIT);
  };

  // The returned path is passed in, which should have been
  // normalized by the backend
  goBack = (e, path) => {
    if (!path || path === '/') {
      return;
    }

    const arr = path.split('/');
    let parentPath = arr.slice(0, -1).join('/');
    if (parentPath.length === 0) {
      parentPath = '/';
    }

    this.updatePieChart(parentPath, DEFAULT_DISPLAY_LIMIT);
  };

  // Take the request path, make a DU request, inject response
  // into the pie chart
  updatePieChart = (path: string, limit: number) => {
    this.setState({
      isLoading: true
    });
    const duEndpoint = `/api/v1/namespace/du?path=${path}&files=true`;
    axios.get(duEndpoint).then(response => {
      const duResponse: IDUResponse[] = response.data;
      const status = duResponse.status;
      if (status === 'PATH_NOT_FOUND') {
        this.setState({isLoading: false});
        showDataFetchError(`Invalid Path: ${path}`);
        return;
      }

      const dataSize = duResponse.size;
      let subpaths: IDUSubpath[] = duResponse.subPaths;

      subpaths.sort((a, b) => (a.size < b.size) ? 1 : -1);

      // Only show top n blocks with the most DU,
      // other blocks are merged as a single block
      if (subpaths.length > limit) {
        subpaths = subpaths.slice(0, limit);
        let topSize = 0;
        for (let i = 0; i < limit; ++i) {
          topSize += subpaths[i].size;
        }

        const otherSize = dataSize - topSize;
        const other: IDUSubpath = {path: OTHER_PATH_NAME, size: otherSize};
        subpaths.push(other);
      }

      let pathLabels, values, percentage, sizeStr, pieces, subpathName;

      if (duResponse.subPathCount === 0 || subpaths === 0) {
        pieces = duResponse && duResponse.path != null && duResponse.path.split('/');
        subpathName = pieces[pieces.length - 1];
        pathLabels = [subpathName];
        values = [0.1];
        percentage = [100.00];
        sizeStr = [this.byteToSize(duResponse.size, 1)];
      }
      else {
        pathLabels = subpaths.map(subpath => {
          // The return subPath must be normalized in a format with
          // a leading slash and without trailing slash
          pieces = subpath.path.split('/');
          subpathName = pieces[pieces.length - 1];
          // Differentiate key without trailing slash
          return (subpath.isKey || subpathName === OTHER_PATH_NAME) ? subpathName : subpathName + '/';
        });

        values = subpaths.map(subpath => {
          return subpath.size / dataSize;
        });

        percentage = values.map(value => {
          return (value * 100).toFixed(2);
        });

        sizeStr = subpaths.map(subpath => {
          return this.byteToSize(subpath.size, 1);
        });
      }
    
      this.setState({
        // Normalized path
        isLoading: false,
        showPanel: false,
        inputPath: duResponse.path,
        returnPath: duResponse.path,
        displayLimit: limit,
        duResponse,
        plotData: [{
          type: 'pie',
          hole: 0.2,
          values: values,
          customdata: percentage,
          labels: pathLabels,
          text: sizeStr,
          textinfo: 'label',
          hovertemplate: 'Percentage: %{customdata}%<br>Total Data Size: %{text}<extra></extra>'
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
    this.updatePieChart('/', DEFAULT_DISPLAY_LIMIT);
  }

  clickPieSection(e, curPath: string): void {
    const subPath: string = e.points[0].label;
    if (subPath === OTHER_PATH_NAME) {
      return;
    }

    const path = (curPath === '/') ? `${curPath}${subPath}` : `${curPath}/${subPath}`;

    // Reset to default everytime
    this.updatePieChart(path, DEFAULT_DISPLAY_LIMIT);
  }

  refreshCurPath(e, path: string): void {
    if (!path) {
      return;
    }

    this.updatePieChart(path, this.state.displayLimit);
  }

  updateDisplayLimit(e): void {
    let res = -1;
    if (e.key === 'all') {
      res = Number.MAX_VALUE;
    } else {
      res = Number.parseInt(e.key, 10);
    }

    this.updatePieChart(this.state.inputPath, res);
  }

  // Show the right side panel that display metadata details of path
  showMetadataDetails(e, path: string): void {
    const summaryEndpoint = `/api/v1/namespace/summary?path=${path}`;
    const keys = [];
    const values = [];
    axios.get(summaryEndpoint).then(response => {
      const summaryResponse = response.data;
      keys.push('Entity Type');
      values.push(summaryResponse.type);

      if (summaryResponse.countStats.type === 'KEY') {
        const keyEndpoint = `/api/v1/namespace/du?path=${path}&replica=true`;
        axios.get(keyEndpoint).then(response => {
          keys.push('File Size');
          values.push(this.byteToSize(response.data.size, 3));
          keys.push('File Size With Replication');
          values.push(this.byteToSize(response.data.sizeWithReplica, 3));
          console.log(values);

          this.setState({
            showPanel: true,
            panelKeys: keys,
            panelValues: values
          });
        }).catch(error => {
          this.setState({
            isLoading: false,
            showPanel: false
          });
          showDataFetchError(error.toString());
        });
        return;
      }

      if (summaryResponse.countStats.status === 'PATH_NOT_FOUND') {
        showDataFetchError(`Invalid Path: ${path}`);
        return;
      }

      if (summaryResponse.countStats.numVolume !== -1) {
        keys.push('Volumes');
        values.push(summaryResponse.countStats.numVolume);
      }

      if (summaryResponse.countStats.numBucket !== -1) {
        keys.push('Buckets');
        values.push(summaryResponse.countStats.numBucket);
      }

      if (summaryResponse.countStats.numDir !== -1) {
        keys.push('Total Directories');
        values.push(summaryResponse.countStats.numDir);
      }

      if (summaryResponse.countStats.numKey !== -1) {
        keys.push('Total Keys');
        values.push(summaryResponse.countStats.numKey);
      }

      if (summaryResponse.objectInfo.bucketName && summaryResponse.objectInfo.bucketName !== -1) {
        keys.push('Bucket Name');
        values.push(summaryResponse.objectInfo.bucketName);
      }

      if (summaryResponse.objectInfo.bucketLayout && summaryResponse.objectInfo.bucketLayout !== -1) {
        keys.push('Bucket Layout');
        values.push(summaryResponse.objectInfo.bucketLayout);
      }

      if (summaryResponse.objectInfo.creationTime && summaryResponse.objectInfo.creationTime !== -1) {
        keys.push('Creation Time');
        values.push(moment(summaryResponse.objectInfo.creationTime).format('ll LTS'));
      }

      if (summaryResponse.objectInfo.dataSize && summaryResponse.objectInfo.dataSize !== -1) {
        keys.push('Data Size');
        values.push(this.byteToSize(summaryResponse.objectInfo.dataSize, 3));
      }

      if (summaryResponse.objectInfo.encInfo && summaryResponse.objectInfo.encInfo !== -1) {
        keys.push('ENC Info');
        values.push(summaryResponse.objectInfo.encInfo);
      }

      if (summaryResponse.objectInfo.fileName && summaryResponse.objectInfo.fileName !== -1) {
        keys.push('File Name');
        values.push(summaryResponse.objectInfo.fileName);
      }

      if (summaryResponse.objectInfo.keyName && summaryResponse.objectInfo.keyName !== -1) {
        keys.push('Key Name');
        values.push(summaryResponse.objectInfo.keyName);
      }

      if (summaryResponse.objectInfo.modificationTime && summaryResponse.objectInfo.modificationTime !== -1) {
        keys.push('Modification Time');
        values.push(moment(summaryResponse.objectInfo.modificationTime).format('ll LTS'));
      }

      if (summaryResponse.objectInfo.name && summaryResponse.objectInfo.name !== -1) {
        keys.push('Name');
        values.push(summaryResponse.objectInfo.name);
      }

      if (summaryResponse.objectInfo.owner && summaryResponse.objectInfo.owner !== -1) {
        keys.push('Owner');
        values.push(summaryResponse.objectInfo.owner);
      }

      if (summaryResponse.objectInfo.quotaInBytes && summaryResponse.objectInfo.quotaInBytes !== -1) {
        keys.push('Quota In Bytes');
        values.push(this.byteToSize(summaryResponse.objectInfo.quotaInBytes, 3));
      }

      if (summaryResponse.objectInfo.quotaInNamespace && summaryResponse.objectInfo.quotaInNamespace !== -1) {
        keys.push('Quota In Namespace');
        values.push(this.byteToSize(summaryResponse.objectInfo.quotaInNamespace, 3));
      }

      if (summaryResponse.objectInfo.replicationConfig && summaryResponse.objectInfo.replicationConfig.replicationFactor && summaryResponse.objectInfo.replicationConfig.replicationFactor !== -1) {
        keys.push('Replication Factor');
        values.push(summaryResponse.objectInfo.replicationConfig.replicationFactor);
      }

      if (summaryResponse.objectInfo.replicationConfig && summaryResponse.objectInfo.replicationConfig.replicationType && summaryResponse.objectInfo.replicationConfig.replicationType !== -1) {
        keys.push('Replication Type');
        values.push(summaryResponse.objectInfo.replicationConfig.replicationType);
      }

      if (summaryResponse.objectInfo.replicationConfig && summaryResponse.objectInfo.replicationConfig.requiredNodes && summaryResponse.objectInfo.replicationConfig.requiredNodes !== -1) {
        keys.push('Replication Required Nodes');
        values.push(summaryResponse.objectInfo.replicationConfig.requiredNodes);
      }
      
      if (summaryResponse.objectInfo.sourceBucket && summaryResponse.objectInfo.sourceBucket !== -1) {
        keys.push('Source Bucket');
        values.push(summaryResponse.objectInfo.sourceBucket);
      }

      if (summaryResponse.objectInfo.sourceVolume && summaryResponse.objectInfo.sourceVolume !== -1) {
        keys.push('Source Volume');
        values.push(summaryResponse.objectInfo.sourceVolume);
      }

      if (summaryResponse.objectInfo.storageType && summaryResponse.objectInfo.storageType !== -1) {
        keys.push('Storage Type');
        values.push(summaryResponse.objectInfo.storageType);
      }

      if (summaryResponse.objectInfo.usedBytes && summaryResponse.objectInfo.usedBytes !== -1) {
        keys.push('Used Bytes');
        values.push(summaryResponse.objectInfo.usedBytes);
      }

      if (summaryResponse.objectInfo.usedNamespace && summaryResponse.objectInfo.usedNamespace !== -1) {
        keys.push('Used NameSpaces');
        values.push(summaryResponse.objectInfo.usedNamespace);
      }

      if (summaryResponse.objectInfo.volumeName && summaryResponse.objectInfo.volumeName !== -1) {
        keys.push('Volume Name');
        values.push(summaryResponse.objectInfo.volumeName);
      }

      if (summaryResponse.objectInfo.volume && summaryResponse.objectInfo.volume !== -1) {
        keys.push('Volume');
        values.push(summaryResponse.objectInfo.volume);
      }

      // Show the right drawer
      this.setState({
        showPanel: true,
        panelKeys: keys,
        panelValues: values
      });
    }).catch(error => {
      this.setState({
        isLoading: false,
        showPanel: false
      });
      showDataFetchError(error.toString());
    });

    const quotaEndpoint = `/api/v1/namespace/quota?path=${path}`;
    axios.get(quotaEndpoint).then(response => {
      const quotaResponse = response.data;

      if (quotaResponse.status === 'PATH_NOT_FOUND') {
        showDataFetchError(`Invalid Path: ${path}`);
        return;
      }

      // If quota request not applicable for this path, silently return
      if (quotaResponse.status === 'TYPE_NOT_APPLICABLE') {
        return;
      }

      // Append quota information
      // In case the object's quota isn't set
      if (quotaResponse.allowed !== -1) {
        keys.push('Quota Allowed');
        values.push(this.byteToSize(quotaResponse.allowed, 3));
      }

      keys.push('Quota Used');
      values.push(this.byteToSize(quotaResponse.used, 3));
      this.setState({
        showPanel: true,
        panelKeys: keys,
        panelValues: values
      });
    }).catch(error => {
      this.setState({
        isLoading: false,
        showPanel: false
      });
      showDataFetchError(error.toString());
    });
  }

  render() {
    const {plotData, duResponse, returnPath, panelKeys, panelValues, showPanel, isLoading, inputPath, displayLimit} = this.state;
    const menu = (
      <Menu onClick={e => this.updateDisplayLimit(e)}>
        <Menu.Item key='5'>
          5
        </Menu.Item>
        <Menu.Item key='10'>
          10
        </Menu.Item>
        <Menu.Item key='15'>
          15
        </Menu.Item>
        <Menu.Item key='20'>
          20
        </Menu.Item>
        <Menu.Item key='all'>
          All
        </Menu.Item>
      </Menu>
    );
    return (
      <div className='du-container'>
        <div className='page-header'>
          Disk Usage
        </div>
        <div className='content-div'>
          {isLoading ? <span><Icon type='loading'/> Loading...</span> : (
            <div>
              <Row>
                <Col>
                  <div className='go-back-button'>
                    <Button type='primary' onClick={e => this.goBack(e, returnPath)}><Icon type='left'/></Button>
                  </div>
                  <div className='input-bar'>
                    <h3>Path</h3>
                    <form className='input' id='input-form' onSubmit={this.handleSubmit}>
                      <Input placeholder='/' value={inputPath} onChange={this.handleChange}/>
                    </form>
                  </div>
                  <div className='go-back-button'>
                    <Button type='primary' onClick={e => this.refreshCurPath(e, returnPath)}><Icon type='redo'/></Button>
                  </div>
                  <div className='dropdown-button'>
                    <Dropdown overlay={menu} placement='bottomCenter'>
                      <Button>Display Limit: {(displayLimit === Number.MAX_VALUE) ? 'All' : displayLimit}</Button>
                    </Dropdown>
                  </div>
                  <div className='metadata-button'>
                    <Button type='primary' onClick={e => this.showMetadataDetails(e, returnPath)}>
                      <b>
                        Show Metadata for Current Path
                      </b>
                    </Button>
                  </div>
                </Col>
              </Row>
              <Row>
                {(duResponse.size > 0) ?
                  <div style={{height: 800}}>
                    <Plot
                      data={plotData}
                      layout={
                        {
                          width: 800,
                          height: 750,
                          font: {
                            family: 'Roboto, sans-serif',
                            size: 15
                          },
                          showlegend: true,
                          title: 'Disk Usage for ' + returnPath + ' (Total Size: ' + this.byteToSize(duResponse.size, 1) + ')'
                        }
                      }
                      onClick={(duResponse.subPathCount === 0) ? undefined : e => this.clickPieSection(e, returnPath)}/>
                  </div>
                    :
                  <div style={{height: 800}} className='metadatainformation'><br/>
                    This object is empty. Add files to it to see a visualization on disk usage.{' '}<br/>
                      You can also view its metadata details by clicking the top right button.
                  </div>}
                <DetailPanel path={returnPath} keys={panelKeys} values={panelValues} visible={showPanel}/>
              </Row>
            </div>)}
        </div>
      </div>
    );
  }
}
