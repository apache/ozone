/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OZONE_FSAL_INTERNAL_H
#define OZONE_FSAL_INTERNAL_H

#include "fsal.h"
#include "FSAL/fsal_commonlib.h"
#include "ozone_grpc.h"
#include "ozone_handle.h"

#include <stdatomic.h>

#define OZONE_SUPPORTED_ATTRIBUTES (ATTR_TYPE | ATTR_SIZE | ATTR_FSID | ATTR_FILEID | \
		ATTR_MODE | ATTR_NUMLINKS | ATTR_OWNER | ATTR_GROUP | ATTR_ATIME | \
		ATTR_CTIME | ATTR_MTIME | ATTR_CHANGE | ATTR_SPACEUSED)

struct ozone_fsal_export {
	struct fsal_export export;
	struct ozone_grpc_client *client;
	uint8_t export_uuid[16];
	uint64_t volume_id;
	uint64_t bucket_id;
	char *export_path;
	struct ozone_fsal_handle *root;
};

struct ozone_fsal_handle {
	struct fsal_obj_handle obj_handle;
	struct ozone_fsal_export *export;
	struct ozone_nfs_handle identity;
	uint8_t wire_handle[OZONE_NFS_HANDLE_LENGTH];
	atomic_uint references;
	struct ozone_nfs_attributes attributes;
};

struct ozone_fsal_module {
	struct fsal_module fsal;
	struct fsal_obj_ops handle_ops;
};

extern struct ozone_fsal_module OZONE;

void ozone_fsal_handle_ops_init(struct fsal_obj_ops *ops);
fsal_status_t ozone_fsal_create_root(struct ozone_fsal_export *export);
fsal_status_t ozone_fsal_lookup_path(struct fsal_export *export_hdl,
				     const char *path,
				     struct fsal_obj_handle **handle,
				     struct fsal_attrlist *attrs_out);
fsal_status_t ozone_fsal_create_handle(struct fsal_export *export_hdl,
				       struct gsh_buffdesc *fh_desc,
				       struct fsal_obj_handle **handle,
				       struct fsal_attrlist *attrs_out);
fsal_status_t ozone_fsal_create_export(struct fsal_module *fsal_hdl,
				       void *parse_node,
				       struct config_error_type *err_type,
				       const struct fsal_up_vector *up_ops);
fsal_status_t ozone_fsal_update_export(struct fsal_module *fsal_hdl,
				       void *parse_node,
				       struct config_error_type *err_type,
				       struct fsal_export *original,
				       struct fsal_module *updated_super);
fsal_status_t ozone_fsal_init_config(struct fsal_module *fsal_hdl,
				     config_file_t config,
				     struct config_error_type *err_type);
void ozone_fsal_export_ops_init(struct export_ops *ops);

#endif
