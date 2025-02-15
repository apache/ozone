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

import React, { Suspense } from 'react';

import { Switch as AntDSwitch, Layout } from 'antd';
import NavBar from './components/navBar/navBar';
import NavBarV2 from '@/v2/components/navBar/navBar';
import Breadcrumbs from './components/breadcrumbs/breadcrumbs';
import BreadcrumbsV2 from '@/v2/components/breadcrumbs/breadcrumbs';
import { HashRouter as Router, Switch, Route, Redirect } from 'react-router-dom';
import { routes } from '@/routes';
import { routesV2 } from '@/v2/routes-v2';
import { MakeRouteWithSubRoutes } from '@/makeRouteWithSubRoutes';
import classNames from 'classnames';

import Loader from '@/v2/components/loader/loader';

import './app.less';
import NotFound from '@/v2/pages/notFound/notFound';

const {
  Header, Content, Footer
} = Layout;

interface IAppState {
  collapsed: boolean;
  enableOldUI: boolean;
}

class App extends React.Component<Record<string, object>, IAppState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      collapsed: false,
      // Set the state from data persisted in current session storage, else default to new UI
      enableOldUI: JSON.parse(sessionStorage.getItem('enableOldUI') ?? 'false')
    };
  }

  onCollapse = (collapsed: boolean) => {
    this.setState({ collapsed });
  };

  render() {
    const { collapsed, enableOldUI } = this.state;
    const layoutClass = classNames('content-layout', { 'sidebar-collapsed': collapsed });


    return (
      <Router>
        <Layout style={{ minHeight: '100vh' }}>
          {
            (enableOldUI)
              ? <NavBar collapsed={collapsed} onCollapse={this.onCollapse} />
              : <NavBarV2 collapsed={collapsed} onCollapse={this.onCollapse} />
          }
          <Layout className={layoutClass}>
            <Header>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                {(enableOldUI) ? <Breadcrumbs /> : <BreadcrumbsV2 />}
                <span>
                  <span style={{ marginRight: '8px', color: '#515151'}}>Switch to</span>
                  <AntDSwitch
                    unCheckedChildren={<div style={{ paddingRight: '2px' }}>Old UI</div>}
                    checkedChildren={<div style={{ paddingLeft: '2px' }}>New UI</div>}
                    checked={this.state.enableOldUI}
                    onChange={(checked: boolean) => {
                      this.setState({
                        enableOldUI: checked
                      }, () => {
                        // This is to persist the state of the UI between refreshes.
                        // While using session storage to store state is an anti-pattern, provided the size of the data stored in this case
                        // and the plan to deprecate UI v1 (old UI) in the future - this is the simplest approach/fix for persisting state.
                        sessionStorage.setItem('enableOldUI', JSON.stringify(checked));
                      });
                    }} />
                </span>
              </div>
            </Header>
            <Content style={(enableOldUI) ? { margin: '0 16px 0', overflow: 'initial' } : {}}>
              <Suspense fallback={<Loader />}>
                <Switch>
                  <Route exact path='/'>
                    <Redirect to='/Overview' />
                  </Route>
                  {(enableOldUI)
                    ? routes.map(
                      (route, index) => <MakeRouteWithSubRoutes key={index} {...route} />
                    )
                    : routesV2.map(
                      (route, index) => {
                        return <MakeRouteWithSubRoutes key={index} {...route} />
                      }
                    )
                  }
                  <Route component={NotFound} />
                </Switch>
              </Suspense>
            </Content>
            <Footer style={{ textAlign: 'center' }} />
          </Layout>
        </Layout>
      </Router>
    );
  }
}

export default App;