<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Ozone Local Compose Example

This Compose definition runs the packaged `ozone local run` command in a single
container. The quickstart surface is intentionally small: the Compose file pins
the published ports, enables Recon, and widens the bind host to `0.0.0.0` (the
command binds loopback by default, which the published ports cannot reach),
then lets `ozone local run` use its defaults for the single-node cluster.

## Usage

Start the container:

```bash
docker-compose up -d
docker-compose logs -f local
```

The startup summary prints the suggested local AWS settings. The default S3
endpoint from this example is `http://127.0.0.1:9878`. Recon is off unless
asked for, and the Compose command passes `--recon`, so it is available here at
`http://127.0.0.1:9888`.

Example AWS CLI invocation from the host:

```bash
AWS_ACCESS_KEY_ID=admin \
AWS_SECRET_ACCESS_KEY=admin123 \
AWS_REGION=us-east-1 \
aws --endpoint-url http://127.0.0.1:9878 s3 ls
```

Stop and remove the container and its named volume:

```bash
docker-compose down -v
```

## Troubleshooting

`ozone local run` is quiet by default: the launcher turns service logging off
for CLI commands, so the container prints the startup summary and nothing else.
When startup fails, the message names the cause and the two ways to get more:

```bash
ozone --loglevel INFO local run   # service logs from SCM, OM, datanodes, S3 Gateway and Recon
ozone local run --verbose         # full stack trace instead of the single-line message
```

A startup that times out reports the condition it was still waiting on, such as
a datanode that has not registered with SCM or a safe-mode rule that has not
passed. Set `OZONE_LOCAL_STARTUP_TIMEOUT` to allow a slower start; the value
needs a time unit (`PT3M`, `180s`), because a bare number is rejected rather
than guessed at.

## Advanced configuration

The Compose file enables Recon and pins both HTTP ports with
`ozone local run --s3g-port 9878 --recon --recon-port 9888` so the host can
publish stable local endpoints. Additional local runtime settings can be
provided with `OZONE_LOCAL_*` environment variables, for example:

```yaml
environment:
  OZONE_LOCAL_DATANODES: 2
  OZONE_LOCAL_FORMAT: always
  OZONE_LOCAL_STARTUP_TIMEOUT: PT3M
```
