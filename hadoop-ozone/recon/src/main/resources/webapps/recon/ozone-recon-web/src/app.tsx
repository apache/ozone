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

import { Switch as AntDSwitch, Layout } from 'antd';
import NavBar from './components/navBar/navBar';
import Breadcrumbs from './components/breadcrumbs/breadcrumbs';
import { HashRouter as Router, Switch, Route, Redirect } from 'react-router-dom';
import { routes } from '@/routes';
import { routesV2 } from '@/v2/routes-v2';
import { MakeRouteWithSubRoutes } from '@/makeRouteWithSubRoutes';
import classNames from 'classnames';

import './app.less';

const {
  Header, Content, Footer
} = Layout;

interface IAppState {
  collapsed: boolean;
  enableNewUI: boolean;
}

class App extends React.Component<Record<string, object>, IAppState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      collapsed: false,
      enableNewUI: false
    };
  }

  onCollapse = (collapsed: boolean) => {
    this.setState({ collapsed });
  };

  render() {
    const { collapsed, enableNewUI } = this.state;
    const layoutClass = classNames('content-layout', { 'sidebar-collapsed': collapsed });


    return (
      <Router>
        <Layout style={{ minHeight: '100vh' }}>
          <NavBar collapsed={collapsed} onCollapse={this.onCollapse} />
          <Layout className={layoutClass}>
            <Header>
              <div style={{ margin: '16px 0', display: 'flex', justifyContent: 'space-between' }}>
                <Breadcrumbs />
                <AntDSwitch
                  disabled={true}
                  checkedChildren={<div style={{ paddingLeft: '2px' }}>New UI</div>}
                  onChange={(checked: boolean) => {
                    this.setState({
                      enableNewUI: checked
                    });
                  }} />
              </div>
            </Header>
            <Content style={(enableNewUI) ? {} : { margin: '0 16px 0', overflow: 'initial' }}>
              <Switch>
                <Route exact path='/'>
                  <Redirect to='/Overview' />
                </Route>
                {(enableNewUI)
                  ? routesV2.map(
                    (route, index) => <MakeRouteWithSubRoutes key={index} {...route} />
                  )
                  : routes.map(
                    (route, index) => <MakeRouteWithSubRoutes key={index} {...route} />
                  )
                }
              </Switch>
            </Content>
            <Footer style={{ textAlign: 'center' }} />
          </Layout>
        </Layout>
      </Router>
    );
  }
}

export default App;