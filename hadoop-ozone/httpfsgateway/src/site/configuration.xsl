<?xml version="1.0"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.


-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="html"/>
  <xsl:template match="configuration">
    <html>
      <body>
        <h2>Configuration Properties</h2>
        <table border="1">
          <tr>
            <th>name</th>
            <th>value</th>
            <th>description</th>
          </tr>
          <xsl:for-each select="property">
            <tr>
              <td>
                <a name="{name}">
                  <xsl:value-of select="name"/>
                </a>
              </td>
              <td>
                <xsl:value-of select="value"/>
              </td>
              <td>
                <xsl:value-of select="description"/>
              </td>
            </tr>
          </xsl:for-each>
        </table>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
