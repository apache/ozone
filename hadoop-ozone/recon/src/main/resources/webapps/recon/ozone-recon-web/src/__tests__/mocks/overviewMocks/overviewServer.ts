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

import * as mockResponses from "./overviewResponseMocks";

const handlers = [
  rest.get("api/v1/clusterState", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.ClusterState)
    );
  }),
  rest.get("api/v1/task/status", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.TaskStatus)
    );
  }),
  rest.get("api/v1/keys/open/summary", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.OpenKeys)
    );
  }),
  rest.get("api/v1/keys/deletePending/summary", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(mockResponses.DeletePendingSummary)
    );
  })
]

const faultyHandlers = [
  rest.get("api/v1/clusterState", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(null)
    );
  }),
  rest.get("api/v1/task/status", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(null)
    );
  }),
  rest.get("api/v1/keys/open/summary", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(null)
    );
  }),
  rest.get("api/v1/keys/deletePending/summary", (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(null)
    );
  })
]
//This will configure a request mocking server using MSW
export const overviewServer = setupServer(...handlers);
export const faultyOverviewServer = setupServer(...faultyHandlers);
