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
import {DownOutlined} from '@ant-design/icons';
import moment from 'moment';
import {showDataFetchError} from 'utils/common';
import './heatmap.less';
import HeatMap1 from './heatMap1';




interface IDUSubpath {
  path: string;
  size: number;
  sizeWithReplica: number;
  isKey: boolean;
}

interface ITreeResponse {
  status: string;
  path: string;
  subPathCount: number;
  size: number;
  sizeWithReplica: number;
  subPaths: IDUSubpath[];
  sizeDirectKey: number;
}

interface ITreeState {
  isLoading: boolean;
  treeResponse: ITreeResponse[];
  showPanel: boolean;
  inputRadio: number;
  inputPath: string;
  entityType: string;
  date: string;
 
}

export class HeatMap extends React.Component<Record<string, object>, ITreeState> {
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

  handleCalenderChange = e => {
    console.log("handleCalenderChange--", e.key);
    if (e.key === '24H' || e.key === '7D' || e.key === '90D') {
      this.setState((prevState, newState) => ({
        date: e.key,
        inputPath: prevState.inputPath,
        entityType: prevState.entityType
      }), () => {
        this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date)
      });
    }
  };

  handleMenuChange = e => {
    console.log("handleMenuChange--", e.key);
    //e.item == 'MenuItem
    if (e.key === 'volume' || e.key === 'bucket' || e.key === 'key')
    {
      this.setState((prevState, newState) => ({
        entityType: e.key,
        date: prevState.date,
        inputPath : prevState.inputPath
      }), () => {
        this.updateTreeMap(this.state.inputPath, this.state.entityType,this.state.date)
      });
    }
  };

  handleChange = e => {
    console.log("handleChange---", e.target.value);
    const value = e.target.value;
    let validExpression;
    // Only allow letters, numbers,underscores and forward slashes
    const regex = /^[a-zA-Z0-9_/]*$/;
    if (regex.test(value)) {
      validExpression = value;
     }
    else {
      alert("Please enter valid Input Path.");
      validExpression = '/';
    }
    this.setState({ 
      inputPath: validExpression
    })
  };

  handleSubmit = _e => {
    // Avoid empty request trigger 400 response
    console.log("handleSubmit --- ",this.state.inputPath, this.state.entityType, this.state.date);
    this.updateTreeMap(this.state.inputPath, this.state.entityType, this.state.date);
  };

  updateTreeMap = (path: string, entityType:string, date:string) => {
    this.setState({
      isLoading: true
    });
    console.log("Under Update Tree Map--", path, entityType,date);
    const treeEndpoint = `/api/v1/heatmap/readaccess?startDate=${date}&path=${path}&entityType=${entityType}`;
    axios.get(treeEndpoint).then(response => {
      const treeResponse: ITreeResponse[] = response.data;
      console.log("API Response:", treeResponse);
      this.setState({
        isLoading: false,
        showPanel: false,
        treeResponse,
        
      });
    }).catch(error => {
      this.setState({
        isLoading: false,
        inputPath: '',
        entityType: '',
        date:''
      });
      showDataFetchError(error.toString());
    });
  };

  componentDidMount(): void {
    this.setState({
      isLoading: true
    });
    // By default render the DU for root path
    this.updateTreeMap('/',this.state.entityType,this.state.date);
  }

  onChange  = (date: any[], dateString: any[]) => {
    
    console.log("onChange--",moment(date).unix().toString());
    this.setState((prevState, newState) => ({
      date: moment(date).unix().toString(),
      entityType: prevState.entityType,
      inputPath : prevState.inputPath
    }), () => {
      this.updateTreeMap(this.state.inputPath, this.state.entityType,this.state.date)
    });
    
  }

  disabledDate(current: any) {
    return current > moment() || current < moment().subtract(90, 'day');
  }


  render() {
    //console.log("Render--", this.state);
    const { treeResponse, isLoading, inputPath} = this.state;
    const menuCalender = (
      <Menu onClick={e => this.handleCalenderChange(e)}>
        <Menu.Item key='24H'>
        Last 24 Hour
        </Menu.Item>
        <Menu.Item key='7D'>
        Last 7 Days
        </Menu.Item>
        <Menu.Item key='90D'>
        Last 90 Days
        </Menu.Item>
        <Menu.SubMenu  title="Custom Select Last 90 Days">
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
      <Menu onClick={e => this.handleMenuChange(e)}>
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
          { isLoading ? <span><Icon type='loading'/> Loading...</span> : (
            <div>
                {(Object.keys(treeResponse).length > 0) ?
                  <div>
                    <Row>
                          <div className='input-bar'>
                            <h4>Path</h4>
                            <form className='input' autoComplete="off" id='input-form' onSubmit={this.handleSubmit}>
                              <Input placeholder='/' name="inputPath" value={inputPath} onChange={this.handleChange} />
                            </form>
                          </div>
                          <div className='dropdown-button'>
                            <Dropdown  overlay={entityTypeMenu} placement='bottomCenter'>
                                <Button>Entity Type <DownOutlined/></Button>
                            </Dropdown>
                          </div>
                      
                          <div className='dropdown-button1'>
                            <Dropdown overlay={menuCalender} placement='bottomLeft'>
                              <Button>Calender <DownOutlined/></Button>
                            </Dropdown>
                          </div>
                    </Row>
                    <br/><br/><br/><br/><br/>
                     <div style={{display:"flex",alignItems: "right"}}>
                          <div style={{ display: "flex", alignItems: "center",marginLeft:"30px"}}>
                              <div style={{ width: "13px", height: "13px", backgroundColor: "yellow", marginRight: "5px" }}> </div>
                              <span>Less Accessed</span>
                          </div>
                          <div style={{ display: "flex", alignItems: "center",marginLeft:"50px" }}>
                              <div style={{ width: "13px", height: "13px", backgroundColor: "orange", marginRight: "5px" }}> </div>
                              <span>Moderate Accessed</span>
                          </div>
                          <div style={{ display: "flex", alignItems: "center",marginLeft:"50px" }}>
                              <div style={{ width: "13px", height: "13px", backgroundColor: "maroon", marginRight: "5px" }}> </div>
                              <span>Most Accessed</span>
                          </div>
                     </div>
                     <div style={{ height:750}}>
                        <HeatMap1 data={treeResponse}></HeatMap1>
                     </div>
                  </div>
                  :
                  <div style={{ height: 800 }} className='metadatainformation'><br />
                    This object is empty. Add volumes and Buckets to it to see a visualization on Tree Map.{' '}<br />
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