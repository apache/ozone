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
import { Row, Icon, Button, Input, Dropdown, Menu, DatePicker } from 'antd';
import { DownOutlined } from '@ant-design/icons';
import moment from 'moment';
import { showDataFetchError } from 'utils/common';
import './heatmap.less';
import HeatMapConfiguration from './heatMapConfiguration';

interface ITreeResponse {
  label: string;
  children: IChildren[];
  size: number;
  path: string;
  minAccessCount: number;
  maxAccessCount: number;
}

interface IChildren {
  label: string;
  size: number;
  accessCount: number;
  color: number;
}

interface ITreeState {
  isLoading: boolean;
  treeResponse: ITreeResponse[];
  showPanel: boolean;
  inputPath: string;
  entityType: string;
  date: string;
  inputRadio?: number;
}

let minSize = Infinity;
let maxSize = 0;

export class Heatmap extends React.Component<Record<string, object>, ITreeState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      isLoading: false,
      treeResponse: [],
      showPanel: false,
      entityType: 'key',
      inputPath: '/',
      date: '24H'
    };
  }

  handleCalendarChange = (e: any) => {
    if (e.key === '24H' || e.key === '7D' || e.key === '90D') {
      this.setState((prevState, _newState) => ({
        date: e.key,
        inputPath: prevState.inputPath,
        entityType: prevState.entityType
      }), () => {
        this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date)
      });
    }
  };

  handleMenuChange = (e: any) => {
    if (e.key === 'volume' || e.key === 'bucket' || e.key === 'key') {
      minSize = Infinity;
      maxSize = 0;
      this.setState((prevState, _newState) => ({
        entityType: e.key,
        date: prevState.date,
        inputPath: prevState.inputPath
      }), () => {
        this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date)
      });
    }
  };

  handleChange = (e: any) => {
    const value = e.target.value;
    let validExpression;
    // Only allow letters, numbers,underscores and forward slashes
    const regex = /^[a-zA-Z0-9_/]*$/;
    if (regex.test(value)) {
      validExpression = value;
    }
    else {
      alert("Please Enter Valid Input Path.");
      validExpression = '/';
    }
    this.setState({
      inputPath: validExpression
    });
  };

  handleSubmit = (_e: any) => {
    // Avoid empty request trigger 400 response
    this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date);
  };

  updateTreeMap = (path: string, entityType: string, date: string) => {
    this.setState({
      isLoading: true
    });

    const treeEndpoint = `/api/v1/heatmap/readaccess?startDate=${date}&path=${path}&entityType=${entityType}`;
    axios.get(treeEndpoint).then(response => {
      minSize = this.minmax(response.data)[0];
      maxSize = this.minmax(response.data)[1];
      const treeResponse: ITreeResponse = this.updateSize(response.data);
      this.setState({
        isLoading: false,
        showPanel: false,
        treeResponse
      });
    }).catch(error => {
      this.setState({
        isLoading: false,
        inputPath: '',
        entityType: '',
        date: ''
      });
      showDataFetchError(error.toString());
    });
  };

  updateTreemapParent = (path: any) => {
    this.setState({
      isLoading: true,
      inputPath: path
    }, () => {
      this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date)
    });
  };

  componentDidMount(): void {
    this.setState({
      isLoading: true
    });
    // By default render treemap for default path entity type and date
    this.updateTreeMap('/', this.state.entityType, this.state.date);
  }

  onChange = (date: any[]) => {
    this.setState(prevState => ({
      date: moment(date).unix(),
      entityType: prevState.entityType,
      inputPath: prevState.inputPath
    }), () => {
      this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date);
    });
  };

  disabledDate(current: any) {
    return current > moment() || current < moment().subtract(90, 'day');
  }

  resetInputpath = (_e: any, path: string) => {
    if (!path || path === '/') {
      return;
    }
    else {
      this.setState({
        inputPath: '/'
      })
    }
  };

  minmax = (obj: any) => {
    if (Object.prototype.hasOwnProperty.call(obj, 'children')) {
      obj.children.forEach((child: any) => this.minmax(child));
    }

    if (Object.prototype.hasOwnProperty.call(obj, 'size') && Object.prototype.hasOwnProperty.call(obj, 'color')) {
      minSize = Math.min(minSize, obj.size);
      maxSize = Math.max(maxSize, obj.size);
    }
    return [minSize, maxSize];
  };

  updateSize = (obj: any) => {
    //Normalize Size so other blocks also get visualized if size is large in bytes minimize and if size is too small make it big
    if (Object.prototype.hasOwnProperty.call(obj, 'size') && Object.prototype.hasOwnProperty.call(obj, 'color')) {
      const newSize = this.normalize(minSize, maxSize, obj.size);
      obj['normalizedSize'] = newSize;
    }
    if (Object.prototype.hasOwnProperty.call(obj, 'children')) {
      obj.children.forEach((child: any) => this.updateSize(child));
    }
    return obj;
  };

  normalize = (min: number, max: number, size: number) => {
    //Normaized Size using Deviation and mid Point
    const mean = (max + min) / 2;
    const highMean = (max + mean) / 2;
    const lowMean1 = (min + mean) / 2;
    const lowMean2 = (lowMean1 + min) / 2;

    if (size > highMean) {
      const newsize = highMean + (size * 0.1);
      return (newsize);
    }
    // lowmean2= 100 value=10, diff=
    // min= 10 ,max=100, mean=55, lowmean1=32.5,lowmean2=22, value= 15, diff=7, diff/2=3.5, newsize= 22-3.5=18.5
    if (size < lowMean2) {
      const diff = (lowMean2 - size) / 2;
      const newSize = lowMean2 - diff;
      return (newSize);
    }
    return size;
  };

  render() {
    const { treeResponse, isLoading, inputPath, date } = this.state;
    const menuCalendar = (
      <Menu
        defaultSelectedKeys={[date]}
        onClick={e => this.handleCalendarChange(e)}>
        <Menu.Item key='24H'>
          24 Hour
        </Menu.Item>
        <Menu.Item key='7D'>
          7 Days
        </Menu.Item>
        <Menu.Item key='90D'>
          90 Days
        </Menu.Item>
        <Menu.SubMenu title="Custom Select Last 90 Days">
          <Menu.Item>
            <DatePicker
              format="YYYY-MM-DD"
              onChange={this.onChange}
              disabledDate={this.disabledDate}
            />
          </Menu.Item>
        </Menu.SubMenu>
      </Menu>
    );

    const entityTypeMenu = (
      <Menu
        defaultSelectedKeys={[this.state.entityType]}
        onClick={e => this.handleMenuChange(e)}>
        <Menu.Item key='volume'>
          Volume
        </Menu.Item>
        <Menu.Item key='bucket'>
          Bucket
        </Menu.Item>
        <Menu.Item key='key'>
          Key
        </Menu.Item>
      </Menu>
    );

    return (
      <div className='heatmap-container'>
        <div className='page-header'>
          Tree Map for Entities
        </div>
        <div className='content-div'>
          {isLoading ? <span><Icon type='loading' /> Loading...</span> : (
            <div>
              {(Object.keys(treeResponse).length > 0 && (treeResponse.minAccessCount > 0 || treeResponse.maxAccessCount > 0)) ?
                (treeResponse.size === 0) ? <div className='heatmapinformation'><br />Failed to Load Heatmap.{' '}<br /></div>
                  :
                  <div>
                    <div className='heatmap-header-container'>
                      <Row>
                        <div className='go-back-button'>
                          <Button type='primary' onClick={e => this.resetInputpath(e, inputPath)}><Icon type='undo' /></Button>
                        </div>
                        <div className='path-input-container'>
                          <h4 style={{ margin: "auto" }}>Path</h4>
                          <form className='input' autoComplete="off" id='input-form' onSubmit={this.handleSubmit}>
                            <Input placeholder='/' name="inputPath" value={inputPath} onChange={this.handleChange} />
                          </form>
                        </div>
                        <div className='entity-dropdown-button'>
                          <Dropdown overlay={entityTypeMenu} placement='bottomCenter'>
                            <Button>Entity Type:&nbsp;{this.state.entityType}<DownOutlined /></Button>
                          </Dropdown>
                        </div>
                        <div className='date-dropdown-button'>
                          <Dropdown overlay={menuCalendar} placement='bottomLeft'>
                            <Button>Last &nbsp;{date > 100 ? new Date(date * 1000).toLocaleString() : date}<DownOutlined /></Button>
                          </Dropdown>
                        </div>
                      </Row>
                      <div className='heatmap-legend-container'>
                        <div className='heatmap-legend-item'>
                          <div style={{ width: "13px", height: "13px", backgroundColor: "#CCFFD9", marginRight: "5px" }}> </div>
                          <span>Less Accessed</span>
                        </div>
                        <div className='heatmap-legend-item'>
                          <div style={{ width: "13px", height: "13px", backgroundColor: "#FFD28F", marginRight: "5px" }}> </div>
                          <span>Moderate Accessed</span>
                        </div>
                        <div className='heatmap-legend-item'>
                          <div style={{ width: "13px", height: "13px", backgroundColor: "#FD6C64", marginRight: "5px" }}> </div>
                          <span>Most Accessed</span>
                        </div>
                      </div>
                    </div>
                    <div id="heatmap-chart-container">
                      <HeatMapConfiguration data={treeResponse} onClick={this.updateTreemapParent}></HeatMapConfiguration>
                    </div>
                  </div>
                :
                <div className='heatmap-no-data-text'><br />
                  No Data Available.{' '}<br />
                </div>
              }
            </div>
          )}
        </div>
      </div>
    );
  }
}