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

#include "config.h"

#include "ozone_internal.h"

#include "FSAL/fsal_init.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>

static const char ozone_name[] = "OZONE";
static bool ozone_registered;

struct ozone_fsal_module OZONE = {
	.fsal = {
		.fs_info = {
			.maxfilesize = INT64_MAX,
			.maxlink = 0,
			.maxnamelen = OZONE_NFS_MAX_NAME_BYTES,
			.maxpathlen = 4096,
			.no_trunc = true,
			.chown_restricted = true,
			.case_insensitive = false,
			.case_preserving = true,
			.link_support = false,
			.symlink_support = false,
			.lock_support = false,
			.lock_support_async_block = false,
			.named_attr = false,
			.unique_handles = true,
			.acl_support = 0,
			.cansettime = false,
			.homogenous = true,
			.supported_attrs = OZONE_SUPPORTED_ATTRIBUTES,
			.maxread = OZONE_NFS_MAX_READ_BYTES,
			.maxwrite = FSAL_MAXIOSIZE,
			.umask = 0,
			.auth_exportpath_xdev = false,
			.link_supports_permission_checks = false,
			.readdir_plus = true,
			.expire_time_parent = -1,
		}
	}
};

static struct config_item ozone_module_params[] = {
	CONFIG_EOL
};

static struct config_block ozone_module_block = {
	.dbus_interface_name = "org.ganesha.nfsd.config.fsal.ozone",
	.blk_desc.name = "OZONE",
	.blk_desc.type = CONFIG_BLOCK,
	.blk_desc.flags = CONFIG_UNIQUE,
	.blk_desc.u.blk.init = noop_conf_init,
	.blk_desc.u.blk.params = ozone_module_params,
	.blk_desc.u.blk.commit = noop_conf_commit
};

fsal_status_t ozone_fsal_init_config(struct fsal_module *fsal_hdl,
				     config_file_t config,
				     struct config_error_type *err_type)
{
	int result;

	result = load_config_from_parse(config, &ozone_module_block, fsal_hdl,
					true, err_type);
	if (result != 0 || !config_error_is_harmless(err_type))
		return fsalstat(ERR_FSAL_INVAL, result == 0 ? EINVAL : result);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

MODULE_INIT void ozone_fsal_init(void)
{
	int result;

	result = register_fsal(&OZONE.fsal, ozone_name, FSAL_MAJOR_VERSION,
			       FSAL_MINOR_VERSION, FSAL_ID_NO_PNFS);
	if (result != 0) {
		LogCrit(COMPONENT_FSAL, "OZONE FSAL module failed to register: %d", result);
		return;
	}
	ozone_registered = true;
	OZONE.fsal.m_ops.create_export = ozone_fsal_create_export;
	OZONE.fsal.m_ops.update_export = ozone_fsal_update_export;
	OZONE.fsal.m_ops.init_config = ozone_fsal_init_config;
	ozone_fsal_handle_ops_init(&OZONE.handle_ops);
}

MODULE_FINI void ozone_fsal_finish(void)
{
	int result;

	if (!ozone_registered)
		return;
	result = unregister_fsal(&OZONE.fsal);

	if (result != 0) {
		LogCrit(COMPONENT_FSAL, "Unable to unload OZONE FSAL: %d", result);
		abort();
	}
}
