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
 *
 * json-server middleware that simulates real backend pagination for the
 * unhealthy-containers endpoints.
 *
 * json-server v0.15 inserts custom --middlewares AFTER its own URL rewriter,
 * so by the time this middleware fires the path has already been rewritten:
 *
 *   /api/v1/containers/unhealthy/MISSING  →  /unhealthyMissing
 *
 * We therefore match against the rewritten resource paths and filter the
 * in-memory db data based on the original query-string parameters.
 *
 * Supports:
 *   limit          – max number of items to return (default 10)
 *   minContainerId – return only containers with containerID > this value
 */

'use strict';

const fs   = require('fs');
const path = require('path');

const DB_PATH = path.join(__dirname, 'db.json');

// Map rewritten resource path  →  db.json top-level key
const PATH_TO_KEY = {
  '/unhealthyMissing':          'unhealthyMissing',
  '/unhealthyUnderReplicated':  'unhealthyUnderReplicated',
  '/unhealthyOverReplicated':   'unhealthyOverReplicated',
  '/unhealthyMisReplicated':    'unhealthyMisReplicated',
  '/unhealthyReplicaMismatch':  'unhealthyReplicaMismatch',
};

module.exports = function paginationMiddleware(req, res, next) {
  const dbKey = PATH_TO_KEY[req.path];
  if (!dbKey) return next();

  const limit          = Math.max(1, parseInt(req.query.limit, 10)          || 10);
  const minContainerId = Math.max(0, parseInt(req.query.minContainerId, 10) || 0);

  let db;
  try {
    db = JSON.parse(fs.readFileSync(DB_PATH, 'utf-8'));
  } catch (e) {
    console.error('[pagination] Failed to read db.json:', e.message);
    return next();
  }

  const resource = db[dbKey];
  if (!resource) return next();

  const allContainers = (resource.containers || [])
    .filter(c => c.containerID > minContainerId)
    .sort((a, b) => a.containerID - b.containerID);

  const page     = allContainers.slice(0, limit);
  const firstKey = page.length > 0 ? page[0].containerID               : 0;
  const lastKey  = page.length > 0 ? page[page.length - 1].containerID : 0;

  res.json({
    missingCount:         resource.missingCount         || 0,
    underReplicatedCount: resource.underReplicatedCount || 0,
    overReplicatedCount:  resource.overReplicatedCount  || 0,
    misReplicatedCount:   resource.misReplicatedCount   || 0,
    replicaMismatchCount: resource.replicaMismatchCount || 0,
    firstKey,
    lastKey,
    containers: page,
  });
};
