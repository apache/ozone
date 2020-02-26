/**
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

import * as React from 'react';

export class FilledIcon extends React.Component {
  render() {
    const path =
        'M864 64H160C107 64 64 107 64 160v' +
        '704c0 53 43 96 96 96h704c53 0 96-43 96-96V16' +
        '0c0-53-43-96-96-96z';
    return (
        <svg {...(this.props as any)} viewBox="0 0 1024 1024">
          <path d={path} />
        </svg>
    );
  }
}
