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
import { Row, Icon, Button, Input, Dropdown, Menu, DatePicker, Form } from 'antd';
import { DownOutlined } from '@ant-design/icons';
import moment from 'moment';
import { showDataFetchError } from 'utils/common';
import './heatmap.less';
import HeatMapConfiguration from './heatMapConfiguration';

type inputPathValidity = "" | "error" | "success" | "warning" | "validating" | undefined

interface ITreeResponse {
  label: string;
  path: string;
  maxAccessCount: number;
  minAccessCount: number;
  size: number;
  children: IChildren[];
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
  inputRadio: number;
  inputPath: string;
  entityType: string;
  date: string;
  treeEndpointFailed: boolean;
  inputPathValid: inputPathValidity;
  helpMessage: string;
}

let minSize = Infinity;
let maxSize = 0;

const colourScheme = {
  pastel_greens: [
    '#CCFFD9', //light green start (least accessed)
    '#B9FBD5',
    '#A7F7D1',
    '#94F2CD',
    '#82EEC9',
    '#6FEAC5',
    '#5DE6C2',
    '#4AE2BE',
    '#38DEBA',
    '#25D9B6',
    '#13D5B2',
    '#00D1AE', //dark green ends (light to moderate accces)
    '#FFD28F', //light orange (moderate access)
    '#FFC58A',
    '#FFB984',
    '#FEAC7F',
    '#FE9F7A',
    '#FE9274',
    '#FE866F',
    '#FD7969',
    '#FD6C64' //red (most accessed)
  ],
  amber_alert: [
    '#FFCF88',
    '#FFCA87',
    '#FFC586',
    '#FFC085',
    '#FFBB83',
    '#FFB682',
    '#FFB181',
    '#FFA676',
    '#FF9F6F',
    '#FF9869',
    '#FF9262',
    '#FF8B5B',
    '#FF8455',
    '#FF7D4E',
    '#FF8282',
    '#FF7776',
    '#FF6D6A',
    '#FF625F',
    '#FF5753',
    '#FF4D47',
    '#FF423B'
  ]
};

export class Heatmap extends React.Component<Record<string, object>, ITreeState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      isLoading: false,
      treeResponse: [],
      showPanel: false,
      entityType: 'key',
      inputPath: '/',
      date: '24H',
      treeEndpointFailed: false,
      inputPathValid: undefined,
      helpMessage: ""
    };
  }

  handleCalendarChange = (e: any) => {
    if (e.key === '24H' || e.key === '7D' || e.key === '90D') {
      this.setState((prevState, _newState) => ({
        date: e.key,
        inputPath: prevState.inputPath,
        entityType: prevState.entityType
      }), () => {
        this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date);
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
        this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date);
      });
    }
  };

  handleChange = (e: any) => {
    const value = e.target.value;
    let inputValid = "";
    let helpMessage = ""
    // Only allow letters, numbers,underscores and forward slashes
    const regex = /^[a-zA-Z0-9_/]*$/;
    if (!regex.test(value)) {
      helpMessage = "Please enter valid path"
      inputValid = "error"
    }
    this.setState({
      inputPath: value,
      inputPathValid: inputValid as inputPathValidity,
      helpMessage: helpMessage
    });
  };

  handleSubmit = (_e: any) => {
    // Avoid empty request trigger 400 response
    this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date);
  };

  updateTreeMap = (path: string, entityType: string, date: string) => {
    this.setState({
      isLoading: true,
      treeEndpointFailed: false
    });

    if (date && path && entityType) {
      const treeEndpoint = `/api/v1/heatmap/readaccess?startDate=${date}&path=${path}&entityType=${entityType}`;
      axios.get(treeEndpoint).then(response => {
        minSize = this.minmax(response.data)[0];
        maxSize = this.minmax(response.data)[1];
        let treeResponse: ITreeResponse = this.updateSize(response.data);
        console.log("Final treeResponse--", treeResponse);
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
          date: '',
          treeEndpointFailed: true
        });
        showDataFetchError(error.toString());
      });
    }
    else {
      this.setState({
        isLoading: false
      });
    }
  };

  updateTreemapParent = (path: any) => {
    this.setState({
      isLoading: true,
      inputPath: path
    }, () => {
      this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date);
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
      });
    }
  };

  minmax = (obj :any) => {
    //min max property will get applied to only for leaf level which we are showing on UI.
    if (obj.hasOwnProperty('children')) {
      obj.children.forEach((child: any) => this.minmax(child))
    };
    if (obj.hasOwnProperty('size') && obj.hasOwnProperty('color')) {
      minSize = Math.min(minSize, obj.size);
      maxSize = Math.max(maxSize, obj.size);
    }
    return [minSize, maxSize];
  };

  updateSize = (obj: any) => {
    //Normalize Size so other blocks also get visualized if size is large in bytes minimize and if size is too small make it big
    // it will only apply on leaf level as checking color property
    if (obj.hasOwnProperty('size') && obj.hasOwnProperty('color')) {

      // hide block at key,volume,bucket level if size accessCount and maxAccessCount are zero apply normalized size only for leaf level
      if (obj && obj.size === 0 && obj.accessCount === 0) {
        obj['normalizedSize'] = 0;
      } else if (obj && obj.size === 0 && obj.maxAccessCount === 0) {
        obj['normalizedSize'] = 0;
      }
      else if (obj && obj.size === 0 && (obj.accessCount >= 0 || obj.maxAccessCount >= 0))
      {
        obj['normalizedSize'] = 1;
        obj.size = 0;
      }
      else {
        let newSize = this.normalize(minSize, maxSize, obj.size);
        obj['normalizedSize'] = newSize;
      }
    }

    if (obj.hasOwnProperty('children')) {
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
    const { treeResponse, isLoading, inputPath, date, treeEndpointFailed, inputPathValid, helpMessage } = this.state;
    
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
              {treeEndpointFailed ? <div className='heatmapinformation'><br />Failed to Load Heatmap.{' '}<br /></div> :
                (Object.keys(treeResponse).length > 0 && (treeResponse.label !== null || treeResponse.path !== null)) ?
                  <div>
                    <div className='heatmap-header-container'>
                      <Row>
                        <div className='go-back-button'>
                          <Button type='primary' onClick={e => this.resetInputpath(e, inputPath)}><Icon type='undo' /></Button>
                        </div>
                        <div className='path-input-container'>
                          <h4 style={{ marginTop: "10px" }}>Path</h4>
                          <form className='input' autoComplete="off" id='input-form' onSubmit={this.handleSubmit}>
                            <Form.Item  className='path-input-element' validateStatus={inputPathValid} help={helpMessage}>
                              <Input placeholder='/' name="inputPath" value={inputPath} onChange={this.handleChange}/>
                            </Form.Item>
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
                          <div style={{ width: "13px", height: "13px", backgroundColor: `${colourScheme["amber_alert"][0]}`, marginRight: "5px" }}> </div>
                          <span>Less Accessed</span>
                        </div>
                        <div className='heatmap-legend-item'>
                          <div style={{ width: "13px", height: "13px", backgroundColor: `${colourScheme["amber_alert"][8]}`, marginRight: "5px" }}> </div>
                          <span>Moderate Accessed</span>
                        </div>
                        <div className='heatmap-legend-item'>
                          <div style={{ width: "13px", height: "13px", backgroundColor: `${colourScheme["amber_alert"][20]}`, marginRight: "5px" }}> </div>
                          <span>Most Accessed</span>
                        </div>
                      </div>
                    </div>
                    <div id="heatmap-chart-container">
                      <HeatMapConfiguration data={treeResponse} colorScheme={colourScheme["amber_alert"]} onClick={this.updateTreemapParent}></HeatMapConfiguration>
                    </div>
                  </div>
                  :
                  <div className='heatmapinformation'><br />
                    No Data Available. {' '}<br />
                  </div>
              }

            </div>
          )
          }
        </div>
      </div>
    );
  }
}