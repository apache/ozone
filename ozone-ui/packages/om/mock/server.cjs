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

/**
 * json-server based mock for the Ozone Manager JMX endpoint.
 *
 * Serves `GET /jmx?qry=<mbean query>` from the captured responses in
 * jmxData.cjs so the OM UI can run without a live cluster. Start with
 * `pnpm mock:om` (or `pnpm dev:om:mock` to run it alongside the OM dev
 * server, which proxies /jmx to it).
 */

/* eslint-disable @typescript-eslint/no-require-imports */
const jsonServer = require('json-server');
const jmxData = require('./jmxData.cjs');

const PORT = process.env.MOCK_PORT ? Number(process.env.MOCK_PORT) : 9878;

const server = jsonServer.create();
server.use(jsonServer.defaults());

server.get('/jmx', (req, res) => {
  const qry = String(req.query.qry || '');
  const match = jmxData.find((entry) => entry.test.test(qry));
  // Mirror the real JMX servlet, which always returns a { beans: [...] } shape.
  res.json({ beans: match ? match.beans : [] });
});

server.listen(PORT, () => {
  console.log(`OM JMX mock listening on http://localhost:${PORT}/jmx?qry=...`);
});
