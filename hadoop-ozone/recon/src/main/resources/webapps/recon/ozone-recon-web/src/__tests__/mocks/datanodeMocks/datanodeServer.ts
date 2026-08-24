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

import { setupServer } from "msw/node";
import { rest } from "msw";

import * as mockResponses from "./datanodeResponseMocks";

const handlers = [
  rest.get("api/v1/datanodes", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.DatanodeResponse)
    );
  }),
  rest.get("api/v1/datanodes/decommission/info", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.DecommissionInfo)
    );
  })
];

const nullDatanodeResponseHandler = [
  rest.get("api/v1/datanodes", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.NullDatanodeResponse)
    );
  }),
  rest.get("api/v1/datanodes/decommission/info", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.DecommissionInfo)
    );
  })
]

const nullDatanodeHandler = [
  rest.get("api/v1/datanodes", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.NullDatanodes)
    );
  }),
  rest.get("api/v1/datanodes/decommission/info", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.DecommissionInfo)
    );
  })
]

//This will configure a request mocking server using MSW
export const datanodeServer = setupServer(...handlers);
export const nullDatanodeResponseServer = setupServer(...nullDatanodeResponseHandler);
export const nullDatanodeServer = setupServer(...nullDatanodeHandler);
