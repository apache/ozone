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
import moment from 'moment';
import { Row, Button, Input, Menu, Dropdown, Tooltip } from 'antd';
import { MenuProps } from 'antd/es/menu';
import { InfoCircleOutlined, LeftOutlined, LoadingOutlined, RedoOutlined } from '@ant-design/icons';

import { DetailPanel } from '@/components/rightDrawer/rightDrawer';
import { byteToSize, showDataFetchError } from '@/utils/common';
import { EChart } from '@/components/eChart/eChart';
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';

import './diskUsage.less';

const DEFAULT_DISPLAY_LIMIT = 10;
const OTHER_PATH_NAME = 'Other Objects';
const MAX_DISPLAY_LIMIT = 30;
const MIN_BLOCK_SIZE = 0.05;

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

interface IPlotData {
  value: number;
  name: string;
  size: string;
  percentage: string;
}

interface IDUState {
  isLoading: boolean;
  duResponse: IDUResponse[];
  plotData: IPlotData[];
  showPanel: boolean;
  panelKeys: string[];
  panelValues: string[];
  returnPath: string;
  inputPath: string;
  displayLimit: number;
}

let cancelPieSignal: AbortController;
let cancelSummarySignal: AbortController;
let cancelQuotaSignal: AbortController;
let cancelKeyMetadataSignal: AbortController;
let valuesWithMinBlockSize: number[] = [];

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
      displayLimit: DEFAULT_DISPLAY_LIMIT,
    };
  }

  handleChange = (e) => {
    this.setState({ inputPath: e.target.value, showPanel: false });
  };

  handleSubmit = (_e) => {
    // Avoid empty request trigger 400 response
    cancelRequests([
      cancelKeyMetadataSignal,
      cancelQuotaSignal,
      cancelSummarySignal,
      cancelPieSignal,
    ]);

    if (!this.state.inputPath) {
      this.updatePieChart('/', DEFAULT_DISPLAY_LIMIT);
      return;
    }

    this.updatePieChart(this.state.inputPath, DEFAULT_DISPLAY_LIMIT);
  };

  // The returned path is passed in, which should have been
  // normalized by the backend
  goBack = (e, path) => {
    cancelRequests([
      cancelKeyMetadataSignal,
      cancelQuotaSignal,
      cancelSummarySignal,
      cancelPieSignal,
    ]);

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
      isLoading: true,
    });
    const duEndpoint = `/api/v1/namespace/du?path=${path}&files=true&sortSubPaths=true`;
    const { request, controller } = AxiosGetHelper(duEndpoint, cancelPieSignal);
    cancelPieSignal = controller;
    request
      .then((response) => {
        const duResponse: IDUResponse[] = response.data;
        const status = duResponse.status;
        if (status === 'PATH_NOT_FOUND') {
          this.setState({ isLoading: false });
          showDataFetchError(`Invalid Path: ${path}`);
          return;
        }

        const dataSize = duResponse.size;
        let subpaths: IDUSubpath[] = duResponse.subPaths;

        // We need to calculate the size of "Other objects" in two cases:
        // 1) If we have more subpaths listed, than the limit.
        // 2) If the limit is set to the maximum limit (30) and we have any number of subpaths. In this case we won't
        // necessarily have "Other objects", but later we check if the other objects's size is more than zero (we will have
        // other objects if there are more than 30 subpaths, but we can't check on that, as the response will always have
        // 30 subpaths, but from the total size and the subpaths size we can calculate it).

        if (subpaths.length > limit || (subpaths.length > 0 && limit === MAX_DISPLAY_LIMIT)) {
          subpaths = subpaths.slice(0, limit);
          let topSize = 0;
          for (let i = 0; i < subpaths.length; ++i) {
            topSize += subpaths[i].size;
          }
          const otherSize = dataSize - topSize;
          if (otherSize > 0) {
            const other: IDUSubpath = { path: OTHER_PATH_NAME, size: otherSize };
            subpaths.push(other);
          }
        }

        let pathLabels,
          values: number[] = [],
          percentage,
          sizeStr,
          pieces,
          subpathName;

        if (duResponse.subPathCount === 0 || subpaths.length === 0) {
          pieces = duResponse && duResponse.path != null && duResponse.path.split('/');
          subpathName = pieces[pieces.length - 1];
          pathLabels = [subpathName];
          values = [0.1];
          valuesWithMinBlockSize = structuredClone(values);
          percentage = [100.0];
          sizeStr = [byteToSize(duResponse.size, 1)];
        } else {
          pathLabels = subpaths.map((subpath) => {
            // The return subPath must be normalized in a format with
            // a leading slash and without trailing slash
            pieces = subpath.path.split('/');
            subpathName = pieces[pieces.length - 1];
            // Differentiate key without trailing slash
            return subpath.isKey || subpathName === OTHER_PATH_NAME
              ? subpathName
              : subpathName + '/';
          });

          // To avoid NaN Condition NaN will get divide by Zero to avoid map iterations
          if (dataSize > 0) {
            values = subpaths.map((subpath) => {
              return subpath.size / dataSize;
            });
          }

          // Adding a MIN_BLOCK_SIZE to non-zero size entities to ensure that even the smallest entities are visible on the pie chart.
          // Note: The percentage and size string calculations remain unchanged.
          const clonedValues = structuredClone(values);
          valuesWithMinBlockSize =
            clonedValues &&
            clonedValues.map((item: number) => (item > 0 ? item + MIN_BLOCK_SIZE : item));

          percentage = values.map((value) => {
            return (value * 100).toFixed(2);
          });

          sizeStr = subpaths.map((subpath) => {
            return byteToSize(subpath.size, 1);
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
          plotData: valuesWithMinBlockSize.map((key, idx) => {
            return {
              value: key,
              name: pathLabels[idx],
              size: sizeStr[idx],
              percentage: percentage[idx],
            } as IPlotData;
          }),
        });
      })
      .catch((error) => {
        this.setState({
          isLoading: false,
        });
        showDataFetchError(error.toString());
      });
  };

  componentDidMount(): void {
    this.setState({
      isLoading: true,
    });
    // By default render the DU for root path
    this.updatePieChart('/', DEFAULT_DISPLAY_LIMIT);
  }

  componentWillUnmount(): void {
    cancelRequests([
      cancelPieSignal,
      cancelSummarySignal,
      cancelQuotaSignal,
      cancelKeyMetadataSignal,
    ]);
  }

  clickPieSection(e, curPath: string): void {
    const subPath: string = e.name;
    if (subPath === OTHER_PATH_NAME) {
      return;
    }

    const path = curPath === '/' ? `${curPath}${subPath}` : `${curPath}/${subPath}`;

    // Reset to default everytime
    this.updatePieChart(path, DEFAULT_DISPLAY_LIMIT);
  }

  refreshCurPath(e, path: string): void {
    cancelRequests([cancelKeyMetadataSignal, cancelQuotaSignal, cancelSummarySignal]);

    if (!path) {
      return;
    }

    this.updatePieChart(path, this.state.displayLimit);
  }

  // Show the right side panel that display metadata details of path
  showMetadataDetails(e, path: string): void {
    const summaryEndpoint = `/api/v1/namespace/summary?path=${path}`;
    const keys = [];
    const values = [];

    const { request: summaryRequest, controller: summaryNewController } = AxiosGetHelper(
      summaryEndpoint,
      cancelSummarySignal
    );
    cancelSummarySignal = summaryNewController;
    summaryRequest
      .then((response) => {
        const summaryResponse = response.data;
        keys.push('Entity Type');
        values.push(summaryResponse.type);

        // If status is INITIALIZING, we cannot add entities for metadata as it will cause null failures and showing Warning message
        // Hence we only add Entities if the status is not INITIALIZING
        if (summaryResponse.status === 'INITIALIZING') {
          showDataFetchError(
            `The metadata is currently initializing. Please wait a moment and try again later`
          );
          return;
        }

        if (
          summaryResponse.status !== 'INITIALIZING' &&
          summaryResponse.status !== 'PATH_NOT_FOUND'
        ) {
          if (summaryResponse.type === 'KEY') {
            const keyEndpoint = `/api/v1/namespace/du?path=${path}&replica=true`;
            const { request: metadataRequest, controller: metadataNewController } = AxiosGetHelper(
              keyEndpoint,
              cancelKeyMetadataSignal
            );
            cancelKeyMetadataSignal = metadataNewController;
            metadataRequest
              .then((response) => {
                keys.push('File Size');
                values.push(byteToSize(response.data.size, 3));
                keys.push('File Size With Replication');
                values.push(byteToSize(response.data.sizeWithReplica, 3));
                this.setState({
                  showPanel: true,
                  panelKeys: keys,
                  panelValues: values,
                });
              })
              .catch((error) => {
                this.setState({
                  isLoading: false,
                  showPanel: false,
                });
                showDataFetchError(error.toString());
              });
            return;
          }

          if (summaryResponse.countStats?.status === 'PATH_NOT_FOUND') {
            showDataFetchError(`Invalid Path: ${path}`);
            return;
          }

          if (
            summaryResponse.countStats?.numVolume !== undefined &&
            summaryResponse.countStats?.numVolume !== -1
          ) {
            keys.push('Volumes');
            values.push(summaryResponse.countStats.numVolume);
          }

          if (
            summaryResponse.countStats?.numBucket !== undefined &&
            summaryResponse.countStats?.numBucket !== -1
          ) {
            keys.push('Buckets');
            values.push(summaryResponse.countStats.numBucket);
          }

          if (
            summaryResponse.countStats?.numDir !== undefined &&
            summaryResponse.countStats?.numDir !== -1
          ) {
            keys.push('Total Directories');
            values.push(summaryResponse.countStats.numDir);
          }

          if (
            summaryResponse.countStats?.numKey !== undefined &&
            summaryResponse.countStats?.numKey !== -1
          ) {
            keys.push('Total Keys');
            values.push(summaryResponse.countStats.numKey);
          }

          if (
            summaryResponse.objectInfo?.bucketName !== undefined &&
            summaryResponse.objectInfo?.bucketName !== -1
          ) {
            keys.push('Bucket Name');
            values.push(summaryResponse.objectInfo.bucketName);
          }

          if (
            summaryResponse.objectInfo?.bucketLayout !== undefined &&
            summaryResponse.objectInfo?.bucketLayout !== -1
          ) {
            keys.push('Bucket Layout');
            values.push(summaryResponse.objectInfo.bucketLayout);
          }

          if (
            summaryResponse.objectInfo?.creationTime !== undefined &&
            summaryResponse.objectInfo?.creationTime !== -1
          ) {
            keys.push('Creation Time');
            values.push(moment(summaryResponse.objectInfo.creationTime).format('ll LTS'));
          }

          if (
            summaryResponse.objectInfo?.dataSize !== undefined &&
            summaryResponse.objectInfo?.dataSize !== -1
          ) {
            keys.push('Data Size');
            values.push(byteToSize(summaryResponse.objectInfo.dataSize, 3));
          }

          if (
            summaryResponse.objectInfo?.encInfo !== undefined &&
            summaryResponse.objectInfo?.encInfo !== -1
          ) {
            keys.push('ENC Info');
            values.push(summaryResponse.objectInfo.encInfo);
          }

          if (
            summaryResponse.objectInfo?.fileName !== undefined &&
            summaryResponse.objectInfo?.fileName !== -1
          ) {
            keys.push('File Name');
            values.push(summaryResponse.objectInfo.fileName);
          }

          if (
            summaryResponse.objectInfo?.keyName !== undefined &&
            summaryResponse.objectInfo?.keyName !== -1
          ) {
            keys.push('Key Name');
            values.push(summaryResponse.objectInfo.keyName);
          }

          if (
            summaryResponse.objectInfo?.modificationTime !== undefined &&
            summaryResponse.objectInfo?.modificationTime !== -1
          ) {
            keys.push('Modification Time');
            values.push(moment(summaryResponse.objectInfo.modificationTime).format('ll LTS'));
          }

          if (
            summaryResponse.objectInfo?.name !== undefined &&
            summaryResponse.objectInfo?.name !== -1
          ) {
            keys.push('Name');
            values.push(summaryResponse.objectInfo.name);
          }

          if (
            summaryResponse.objectInfo?.owner !== undefined &&
            summaryResponse.objectInfo?.owner !== -1
          ) {
            keys.push('Owner');
            values.push(summaryResponse.objectInfo.owner);
          }

          if (
            summaryResponse.objectInfo?.quotaInBytes !== undefined &&
            summaryResponse.objectInfo?.quotaInBytes !== -1
          ) {
            keys.push('Quota In Bytes');
            values.push(byteToSize(summaryResponse.objectInfo.quotaInBytes, 3));
          }

          if (
            summaryResponse.objectInfo?.quotaInNamespace !== undefined &&
            summaryResponse.objectInfo?.quotaInNamespace !== -1
          ) {
            keys.push('Quota In Namespace');
            values.push(byteToSize(summaryResponse.objectInfo.quotaInNamespace, 3));
          }

          if (
            summaryResponse.objectInfo?.replicationConfig?.replicationFactor !== undefined &&
            summaryResponse.objectInfo?.replicationConfig?.replicationFactor !== -1
          ) {
            keys.push('Replication Factor');
            values.push(summaryResponse.objectInfo.replicationConfig.replicationFactor);
          }

          if (
            summaryResponse.objectInfo?.replicationConfig?.replicationType !== undefined &&
            summaryResponse.objectInfo?.replicationConfig?.replicationType !== -1
          ) {
            keys.push('Replication Type');
            values.push(summaryResponse.objectInfo.replicationConfig.replicationType);
          }

          if (
            summaryResponse.objectInfo?.replicationConfig?.requiredNodes !== undefined &&
            summaryResponse.objectInfo?.replicationConfig?.requiredNodes !== -1
          ) {
            keys.push('Replication Required Nodes');
            values.push(summaryResponse.objectInfo.replicationConfig.requiredNodes);
          }

          if (
            summaryResponse.objectInfo?.sourceBucket !== undefined &&
            summaryResponse.objectInfo?.sourceBucket !== -1
          ) {
            keys.push('Source Bucket');
            values.push(summaryResponse.objectInfo.sourceBucket);
          }

          if (
            summaryResponse.objectInfo?.sourceVolume !== undefined &&
            summaryResponse.objectInfo?.sourceVolume !== -1
          ) {
            keys.push('Source Volume');
            values.push(summaryResponse.objectInfo.sourceVolume);
          }

          if (
            summaryResponse.objectInfo?.storageType !== undefined &&
            summaryResponse.objectInfo?.storageType !== -1
          ) {
            keys.push('Storage Type');
            values.push(summaryResponse.objectInfo.storageType);
          }

          if (
            summaryResponse.objectInfo?.usedBytes !== undefined &&
            summaryResponse.objectInfo?.usedBytes !== -1
          ) {
            keys.push('Used Bytes');
            values.push(summaryResponse.objectInfo.usedBytes);
          }

          if (
            summaryResponse.objectInfo?.usedNamespace !== undefined &&
            summaryResponse.objectInfo?.usedNamespace !== -1
          ) {
            keys.push('Used NameSpaces');
            values.push(summaryResponse.objectInfo.usedNamespace);
          }

          if (
            summaryResponse.objectInfo?.volumeName !== undefined &&
            summaryResponse.objectInfo?.volumeName !== -1
          ) {
            keys.push('Volume Name');
            values.push(summaryResponse.objectInfo.volumeName);
          }

          if (
            summaryResponse.objectInfo?.volume !== undefined &&
            summaryResponse.objectInfo?.volume !== -1
          ) {
            keys.push('Volume');
            values.push(summaryResponse.objectInfo.volume);
          }
        }

        // Show the right drawer
        this.setState({
          showPanel: true,
          panelKeys: keys,
          panelValues: values,
        });
      })
      .catch((error) => {
        this.setState({
          isLoading: false,
          showPanel: false,
        });
        showDataFetchError(error.toString());
      });

    const quotaEndpoint = `/api/v1/namespace/quota?path=${path}`;
    const { request: quotaRequest, controller: quotaNewController } = AxiosGetHelper(
      quotaEndpoint,
      cancelQuotaSignal
    );
    cancelQuotaSignal = quotaNewController;
    quotaRequest
      .then((response) => {
        const quotaResponse = response.data;

        if (quotaResponse.status === 'PATH_NOT_FOUND') {
          showDataFetchError(`Invalid Path: ${path}`);
          return;
        }

        // If quota request not applicable for this path, silently return
        if (quotaResponse.status === 'TYPE_NOT_APPLICABLE') {
          return;
        }

        // If Quota status is INITIALIZING then do not append entities to metadata to avoid null
        if (quotaResponse.status === 'INITIALIZING') {
          return;
        }

        // Append quota information
        // In case the object's quota isn't set
        if (quotaResponse.allowed !== undefined && quotaResponse.allowed !== -1) {
          keys.push('Quota Allowed');
          values.push(byteToSize(quotaResponse.allowed, 3));
        }

        if (quotaResponse.used !== undefined && quotaResponse.used !== -1) {
          keys.push('Quota Used');
          values.push(byteToSize(quotaResponse.used, 3));
        }
        this.setState({
          showPanel: true,
          panelKeys: keys,
          panelValues: values,
        });
      })
      .catch((error) => {
        this.setState({
          isLoading: false,
          showPanel: false,
        });
        showDataFetchError(error.toString());
      });
  }

  render() {
    const {
      plotData,
      duResponse,
      returnPath,
      panelKeys,
      panelValues,
      showPanel,
      isLoading,
      inputPath,
      displayLimit,
    } = this.state;

    const updateDisplayLimit: MenuProps['onClick'] = (e): void => {
      const res = Number.parseInt(e.key, 10);
      this.updatePieChart(this.state.inputPath, res);
    };

    const menuItems = (
      <Menu onClick={updateDisplayLimit}>
        <Menu.Item key='5'>5</Menu.Item>
        <Menu.Item key='10'>10</Menu.Item>
        <Menu.Item key='15'>15</Menu.Item>
        <Menu.Item key='20'>20</Menu.Item>
        <Menu.Item key='30'>30</Menu.Item>
      </Menu>
    );

    const eChartsOptions = {
      title: {
        text: `Disk Usage for ${returnPath} (Total Size: ${byteToSize(duResponse.size, 1)})`,
        left: 'center',
      },
      tooltip: {
        trigger: 'item',
        formatter: ({ dataIndex, name, color }) => {
          const nameEl = `<strong style='color: ${color}'>${name}</strong><br>`;
          const dataEl = `Total Data Size: ${plotData[dataIndex]['size']}<br>`;
          const percentageEl = `Percentage: ${plotData[dataIndex]['percentage']} %`;
          return `${nameEl}${dataEl}${percentageEl}`;
        },
      },
      legend: {
        top: '10%',
        orient: 'vertical',
        left: 'left',
      },
      series: [
        {
          type: 'pie',
          radius: '50%',
          data: plotData.map((value) => {
            return {
              value: value.value,
              name: value.name,
            };
          }),
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
              shadowColor: 'rgba(0, 0, 0, 0.5)',
            },
          },
        },
      ],
    };

    return (
      <div className='du-container'>
        <div className='page-header'>Disk Usage</div>
        <div className='content-div'>
          {isLoading ? (
            <span>
              <LoadingOutlined /> Loading...
            </span>
          ) : (
            <div>
              <Row
                style={{
                  alignItems: 'end',
                  margin: '0px 10px',
                  justifyContent: 'space-between',
                }}
              >
                <div className='path-nav-container'>
                  <div className='go-back-button'>
                    <Button type='primary' onClick={(e) => this.goBack(e, returnPath)}>
                      <LeftOutlined />
                    </Button>
                  </div>
                  <div className='input-bar'>
                    <h3>Path</h3>
                    <form className='input' id='input-form' onSubmit={this.handleSubmit}>
                      <Input placeholder='/' value={inputPath} onChange={this.handleChange} />
                    </form>
                  </div>
                  <div className='go-back-button'>
                    <Button type='primary' onClick={(e) => this.refreshCurPath(e, returnPath)}>
                      <RedoOutlined />
                    </Button>
                  </div>
                  <div style={{ paddingLeft: '15px' }}>
                    <Tooltip
                      placement='rightTop'
                      color='rgba(26, 165, 122, 0.9)'
                      title='Additional block size is added to small entities, for better visibility. Please refer to pie-chart tooltip for exact size information.'
                    >
                      <InfoCircleOutlined
                        style={{
                          fontSize: '1.3em',
                        }}
                      />
                    </Tooltip>
                  </div>
                </div>
                <div className='du-button-container'>
                  <div className='dropdown-button'>
                    <Dropdown overlay={menuItems} placement='bottomCenter'>
                      <Button>Display Limit: {displayLimit}</Button>
                    </Dropdown>
                  </div>
                  <div className='metadata-button'>
                    <Button type='primary' onClick={(e) => this.showMetadataDetails(e, returnPath)}>
                      <b>Show Metadata for Current Path</b>
                    </Button>
                  </div>
                </div>
              </Row>
              <Row>
                {duResponse.size > 0 ? (
                  <div
                    style={{
                      height: 700,
                      margin: 'auto',
                      marginTop: '5%',
                    }}
                  >
                    <EChart
                      option={eChartsOptions}
                      onClick={
                        duResponse.subPathCount === 0
                          ? undefined
                          : (e) => this.clickPieSection(e, returnPath)
                      }
                    />
                  </div>
                ) : (
                  <div style={{ height: 800 }} className='metadatainformation'>
                    <br />
                    This object is empty. Add files to it to see a visualization on disk usage.{' '}
                    <br />
                    You can also view its metadata details by clicking the top right button.
                  </div>
                )}
                <DetailPanel
                  path={returnPath}
                  keys={panelKeys}
                  values={panelValues}
                  visible={showPanel}
                />
              </Row>
            </div>
          )}
        </div>
      </div>
    );
  }
}
