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

import { Switch as AntDSwitch, Layout, message } from 'antd';
import NavBar from './components/navBar/navBar';
import NavBarV2 from '@/v2/components/navBar/navBar';
import Breadcrumbs from './components/breadcrumbs/breadcrumbs';
import BreadcrumbsV2 from '@/v2/components/breadcrumbs/breadcrumbs';
import { HashRouter as Router, Switch, Route, Redirect, useLocation } from 'react-router-dom';
import { routes } from '@/routes';
import { routesV2 } from '@/v2/routes-v2';
import { breadcrumbNameMap as breadcrumbNameMapV1 } from '@/constants/breadcrumbs.constants';
import { breadcrumbNameMap as breadcrumbNameMapV2 } from '@/v2/constants/breadcrumbs.constants';
import { MakeRouteWithSubRoutes } from '@/makeRouteWithSubRoutes';
import classNames from 'classnames';

import Loader from '@/v2/components/loader/loader';

import './app.less';
import NotFound from '@/v2/pages/notFound/notFound';

const {
  Header, Content, Footer
} = Layout;

const FALLBACK_PATH = '/Overview';
const TOAST_DURATION_SECONDS = 4;
type BreadcrumbNameMap = typeof breadcrumbNameMapV1;

// Strict membership check that ignores parameterized/catch-all entries
// (the v1 routes table ends with `/:NotFound`, which would otherwise match anything).
const pathExistsIn = (path: string, table: ReadonlyArray<{ path: string }>): boolean =>
  table.some((r) => !r.path.includes(':') && r.path === path);

const getViewName = (path: string, preferredMap: BreadcrumbNameMap): string =>
  preferredMap[path] ?? path;

interface IAppState {
  collapsed: boolean;
  enableOldUI: boolean;
}

const AppLayout = ({ enableOldUI, collapsed, onCollapse, onToggleUI }: any) => {
  const location = useLocation();
  const isAssistantRoute = location.pathname === '/Assistant';
  const layoutClass = classNames('content-layout', { 'sidebar-collapsed': collapsed });

  return (
    <Layout style={{ minHeight: '100vh' }}>
      {
        (enableOldUI)
          ? <NavBar collapsed={collapsed} onCollapse={onCollapse} />
          : <NavBarV2 collapsed={collapsed} onCollapse={onCollapse} />
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
                checked={enableOldUI}
                onChange={onToggleUI} />
            </span>
          </div>
        </Header>
        <Content style={(enableOldUI) ? { margin: '0 16px 0', overflow: 'initial' } : (isAssistantRoute ? { display: 'flex', flexDirection: 'column', height: 'calc(100vh - 50px)', overflow: 'hidden' } : {})}>
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
        {!isAssistantRoute && <Footer style={{ textAlign: 'center' }} />}
      </Layout>
    </Layout>
  );
};

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

  handleUIToggle = (enableOldUI: boolean) => {
    const currentPath = window.location.hash.slice(1).split('?')[0] || FALLBACK_PATH;
    const targetTable = enableOldUI ? routes : routesV2;
    const sourceTable = enableOldUI ? routesV2 : routes;
    const shouldRedirect = !pathExistsIn(currentPath, targetTable);
    let redirectMessage: string | undefined;

    if (shouldRedirect) {
      window.location.hash = FALLBACK_PATH;
      // Only explain the redirect when the user came from a real page in the source UI;
      // a typo'd path otherwise produces a misleading "only available in..." message.
      if (pathExistsIn(currentPath, sourceTable)) {
        const sourceMap = enableOldUI ? breadcrumbNameMapV2 : breadcrumbNameMapV1;
        const targetMap = enableOldUI ? breadcrumbNameMapV1 : breadcrumbNameMapV2;
        const friendly = getViewName(currentPath, sourceMap);
        const fallbackViewName = getViewName(FALLBACK_PATH, targetMap);
        const sourceUiName = enableOldUI ? 'New UI' : 'Old UI';
        redirectMessage =
          `The '${friendly}' view is only available in the ${sourceUiName}. We've returned you to the ${fallbackViewName} dashboard.`;
      }
    }

    this.setState({ enableOldUI }, () => {
      // This is to persist the state of the UI between refreshes.
      // While using session storage to store state is an anti-pattern, provided the size of the data stored in this case
      // and the plan to deprecate UI v1 (old UI) in the future - this is the simplest approach/fix for persisting state.
      sessionStorage.setItem('enableOldUI', JSON.stringify(enableOldUI));
      if (redirectMessage) {
        message.info(redirectMessage, TOAST_DURATION_SECONDS);
      }
    });
  };

  render() {
    const { collapsed, enableOldUI } = this.state;

    return (
      <Router>
        <AppLayout
          enableOldUI={enableOldUI}
          collapsed={collapsed}
          onCollapse={this.onCollapse}
          onToggleUI={this.handleUIToggle}
        />
      </Router>
    );
  }
}

export default App;
