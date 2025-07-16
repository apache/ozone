---
title: Security Token Service (STS) for Ozone
summary: Allows clients to generate temporary S3 credentials using a REST API.
date: 2025-07-16
jira: HDDS-13323
status: implementing
author: Ren Koike
---
<!--
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
# Introduction

S3 credentials used to communicate with Ozone S3 APIs are based on the kerberos credential used to run the ozone CLI to generate the S3 credential. There is a configuration to specify a S3 Administrator (or default to OM administrators) who can generate S3 credentials for other kerberos principals.

Historically the Ozone community has had an interest in having a REST API to be able to programmatically be able to generate S3 credentials. 

With Amazon AWS, there is a central service which has the ability to generate [Security Tokens that span resources across services](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html). 

This document covers a basic proposal that describes how Ozone can offer a stand alone STS service that can be used by users to use REST APIs to retrieve. This can be later extended to integrate with a centralized STS service.

## Requirements

### Functional requirements

1. Allow privileged users to generate temporary S3 credentials with:  
   * Limited duration  
   * Restricted to specific bucket/prefix paths  
   * Restricted to specific S3 operations  
   * Issuing credentials either to self or another identity  
2. Authenticate the AssumeRoleKerberos call using Kerberos  
3. Authorize the credential issuance via Ranger  
4. Store temporary credentials securely in Ozone Manager  
5. Validate S3 API calls using the temporary credentials against stored permissions  
6. Verify all operations against Ranger policies  
7. Expire the credentials depending on the configured duration   
8. Should work with external stores such as vault (currently Ozone supports this for S3 credentials)

### Non functional requirements 

1. Support in the order of 20k credentials  
   

### Non Goals

1. Support for native ACLs 

## API Spec

Ozone will serve Rest endpoints over the webui ports currently in place.

Clients will need to authenticate with Kerberose before calling the AssumeRoleKerberos endpoint. The AssumeRoleKerberos endpoint will allow a client to Assume a Role specified in Ranger if the user principal used is part of the Role list of users. Each invocation will include a list of bucket:prefix:action list. This list has to be a subset of what the Role in Ranger has access to. 

Ozone will call Ranger to authorize the AssumeRoleKerberos request. Once authorized, Ozone will generate S3 credentials and store the S3 credentials, role and resources requested.

When the client invokes S3 APIs using the S3 credentials passed in, Ozone will call Ranger with the requested bucketr:path:action along with the original bucket:prefix:action requested for AssumeRoleKerberos. Ranger will authorize the S3 request if it is compliantcomplaint with the original AssumeRoleKerberos request.
```json
{  
  "api": "Apache Ozone S3 Gateway",  
  "endpoint": "/sts/AssumeRoleKerberos",  
  "method": "POST",  
  "authentication": {  
    "type": "Kerberos",  
    "description": "Client must be authenticated via Kerberos SPNEGO before making this request"  
  },  
  "request": {  
    "content-type": "application/json",  
    "body": {  
      "roleName": {  
        "type": "string",  
        "description": "The name of the role in Ranger that the client wishes to assume",  
        "required": true  
      },  
      "resourcePermissions": {  
        "type": "array",  
        "description": "List of bucket:prefix:action permissions being requested",  
        "required": true,  
        "items": {  
          "type": "object",  
          "properties": {  
            "bucket": {  
              "type": "string",  
              "description": "The bucket name",  
              "required": true  
            },  
            "prefix": {  
              "type": "string",  
              "description": "The object key prefix",  
              "required": true  
            },  
            "action": {  
              "type": "string",  
              "description": "The S3 action (READ, WRITE, DELETE, etc.)",  
              "required": true,  
              "enum": ["READ", "WRITE", "DELETE", "LIST", "ALL"]  
            }  
          }  
        }  
      },  
      "durationSeconds": {  
        "type": "integer",  
        "description": "Duration in seconds for which the credentials should be valid",  
        "required": false,  
        "default": 3600,  
        "minimum": 900,  
        "maximum": 43200  
      }  
    }  
  },  
  "process": {  
    "description": "Ozone will validate that the Kerberos-authenticated principal is authorized to assume the specified role in Ranger. It will also validate that the requested bucket:prefix:action permissions are a subset of what the role is allowed to access in Ranger.",  
    "steps": [  
      "1. Verify Kerberos authentication",  
      "2. Call Ranger to check if user principal is part of the specified role",  
      "3. Validate requested resourcePermissions against role permissions in Ranger",  
      "4. Generate temporary S3 credentials if authorized",  
      "5. Store credentials, role, and requested resources for future authorization"  
    ]  
  },  
  "response": {  
    "success": {  
      "status": 200,  
      "content-type": "application/json",  
      "body": {  
        "credentials": {  
          "accessKeyId": {  
            "type": "string",  
            "description": "Temporary S3 access key ID"  
          },  
          "secretAccessKey": {  
            "type": "string",  
            "description": "Temporary S3 secret access key"  
          },  
          "sessionToken": {  
            "type": "string",  
            "description": "Temporary S3 session token"  
          },  
          "expiration": {  
            "type": "string",  
            "format": "date-time",  
            "description": "Expiration time of the temporary credentials"  
          }  
        },  
        "roleInfo": {  
          "roleName": {  
            "type": "string",  
            "description": "The name of the assumed role"  
          },  
          "roleId": {  
            "type": "string",  
            "description": "Unique identifier for the assumed role session"  
          }  
        },  
        "resourcePermissions": {  
          "type": "array",  
          "description": "List of bucket:prefix:action permissions granted",  
          "items": {  
            "type": "object",  
            "properties": {  
              "bucket": {  
                "type": "string",  
                "description": "The bucket name"  
              },  
              "prefix": {  
                "type": "string",  
                "description": "The object key prefix"  
              },  
              "action": {  
                "type": "string",  
                "description": "The S3 action granted"  
              }  
            }  
          }  
        }  
      }  
    },  
    "error": {  
      "unauthorized": {  
        "status": 403,  
        "content-type": "application/json",  
        "body": {  
          "code": "AccessDenied",  
          "message": "User is not authorized to assume the specified role or access the requested resources"  
        }  
      },  
      "badRequest": {  
        "status": 400,  
        "content-type": "application/json",  
        "body": {  
          "code": "InvalidRequest",  
          "message": "Description of the validation error"  
        }  
      },  
      "serverError": {  
        "status": 500,  
        "content-type": "application/json",  
        "body": {  
          "code": "InternalServerError",  
          "message": "An internal server error occurred"  
        }  
      }  
    }  
  },  
  "s3AuthorizationFlow": {  
    "description": "When S3 API requests are made using the temporary credentials, Ozone will validate them against the original requested permissions",  
    "steps": [  
      "1. Client makes S3 API request with temporary credentials",  
      "2. Ozone validates credentials and extracts original role and resourcePermissions",  
      "3. Ozone calls Ranger with the requested bucket:path:action and original resourcePermissions",  
      "4. Ranger authorizes the request if it complies with the original permissions",  
      "5. Ozone processes the S3 request if authorized"  
    ]  
  }  
}
```
Update:

1. Knox / S3 Admin will use ozone cli to get secret (already there)  
2. **They will use AWS tokens returned in 1 to call AssumeRole (AWS style header validation authentication)**   
   1. **Optionally** also implement AssumeRoleKerberos (where there is no step 1\) (SNPEGO based authentication)  
   2. Ranger should validate if the user can generate secret and delegate access to bucket:path:action.  
3. Some user uses secret to access bucket:path:action  
   1. We know the secret and user and which role they are assuming and which resource.   
   2. Ranger STS Token Resources : Requested Resources. Sub set check
