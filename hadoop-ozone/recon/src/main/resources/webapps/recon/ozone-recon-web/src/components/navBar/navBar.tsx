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
import logo from '../../logo.png';
import {Layout, Menu, Icon} from 'antd';
import './navBar.less';
import {withRouter, Link} from 'react-router-dom';
import {RouteComponentProps} from 'react-router';

const {Sider} = Layout;

interface INavBarProps extends RouteComponentProps<object> {
  collapsed: boolean;
  onCollapse: (arg: boolean) => void;
}

class NavBar extends React.Component<INavBarProps> {
  render() {
    const {location} = this.props;
    return (
      <Sider
        collapsible
        collapsed={this.props.collapsed}
        collapsedWidth={50}
        style={{
          overflow: 'auto', height: '100vh', position: 'fixed', left: 0
        }}
        onCollapse={this.props.onCollapse}
      >
        <div className='logo'>
          <img src={logo} alt='Ozone Recon Logo' width={32} height={32}/>
          <span className='logo-text'>Ozone Recon</span>
        </div>
        <Menu
          theme='dark' defaultSelectedKeys={['/Dashboard']}
          mode='inline' selectedKeys={[location.pathname]}
        >
          <Menu.Item key='/Overview'>
            <Icon type='dashboard'/>
            <span>Overview</span>
            <Link to='/Overview'/>
          </Menu.Item>
          <Menu.Item key='/Datanodes'>
            <Icon type='cluster'/>
            <span>Datanodes</span>
            <Link to='/Datanodes'/>
          </Menu.Item>
          <Menu.Item key='/Pipelines'>
            <Icon type='deployment-unit'/>
            <span>Pipelines</span>
            <Link to='/Pipelines'/>
          </Menu.Item>
          <Menu.Item key='/Insights'>
            <Icon type='bar-chart'/>
            <span>Insights</span>
            <Link to='/Insights'/>
          </Menu.Item>
        </Menu>
      </Sider>
    );
  }
}

export default withRouter(NavBar);
