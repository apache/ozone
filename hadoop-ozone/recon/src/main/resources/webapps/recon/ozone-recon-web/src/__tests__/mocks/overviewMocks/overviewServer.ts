import { setupServer } from "msw/node";
import { rest } from "msw";

import * as mockResponses from "./responseMocks";

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
//This will configure a request mocking server using MSW
export const overviewServer = setupServer(...handlers);