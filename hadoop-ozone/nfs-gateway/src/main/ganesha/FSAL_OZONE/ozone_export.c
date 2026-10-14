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

#include "FSAL/fsal_config.h"
#include "FSAL/fsal_commonlib.h"
#include "export_mgr.h"
#include "fsal_convert.h"
#include "nfs_exports.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define OZONE_PEM_MAX_BYTES (1024 * 1024)
#define OZONE_REQUIRED_OPERATIONS ((UINT64_C(1) << 1) | (UINT64_C(1) << 2) | \
				       (UINT64_C(1) << 3) | (UINT64_C(1) << 4) | \
				       (UINT64_C(1) << 5) | (UINT64_C(1) << 6) | \
				       (UINT64_C(1) << 7) | (UINT64_C(1) << 8))

struct ozone_export_config {
	char *address;
	char *export_uuid;
	uint64_t volume_id;
	uint64_t bucket_id;
	char *trusted_ca_file;
	char *client_certificate_file;
	char *client_key_file;
	uint32_t timeout_millis;
};

static struct config_item ozone_export_params[] = {
	CONF_MAND_STR("Endpoint", 1, 255, NULL, ozone_export_config, address),
	CONF_MAND_STR("Export_UUID", 36, 36, NULL, ozone_export_config, export_uuid),
	CONF_ITEM_UI64_ZERO("Volume_ID", 0, UINT64_MAX, 0,
			    ozone_export_config, volume_id),
	CONF_ITEM_UI64_ZERO("Bucket_ID", 0, UINT64_MAX, 0,
			    ozone_export_config, bucket_id),
	CONF_MAND_STR("Trusted_CA_File", 1, 4096, NULL,
		       ozone_export_config, trusted_ca_file),
	CONF_MAND_STR("Client_Certificate_File", 1, 4096, NULL,
		       ozone_export_config, client_certificate_file),
	CONF_MAND_STR("Client_Key_File", 1, 4096, NULL,
		       ozone_export_config, client_key_file),
	CONF_ITEM_UI32("Timeout_Millis", 1, 60000, 5000,
		       ozone_export_config, timeout_millis),
	CONFIG_EOL
};

static struct config_block ozone_export_param_block = {
	.dbus_interface_name = "org.ganesha.nfsd.config.fsal.ozone-export%d",
	.blk_desc.name = "FSAL",
	.blk_desc.type = CONFIG_BLOCK,
	.blk_desc.u.blk.init = noop_conf_init,
	.blk_desc.u.blk.params = ozone_export_params,
	.blk_desc.u.blk.commit = noop_conf_commit
};

static bool ozone_parse_export_uuid(const char *text, uint8_t uuid[16])
{
	static const uint8_t hyphen_positions[] = {8, 13, 18, 23};
	size_t source = 0;
	unsigned int destination = 0;

	if (text == NULL || strlen(text) != 36)
		return false;
	while (source < 36) {
		unsigned int high, low;
		char a, b;

		if (source == hyphen_positions[0] || source == hyphen_positions[1] ||
		    source == hyphen_positions[2] || source == hyphen_positions[3]) {
			if (text[source++] != '-')
				return false;
			continue;
		}
		a = text[source++];
		b = text[source++];
		if (a >= '0' && a <= '9')
			high = (unsigned int)(a - '0');
		else if (a >= 'a' && a <= 'f')
			high = (unsigned int)(a - 'a' + 10);
		else if (a >= 'A' && a <= 'F')
			high = (unsigned int)(a - 'A' + 10);
		else
			return false;
		if (b >= '0' && b <= '9')
			low = (unsigned int)(b - '0');
		else if (b >= 'a' && b <= 'f')
			low = (unsigned int)(b - 'a' + 10);
		else if (b >= 'A' && b <= 'F')
			low = (unsigned int)(b - 'A' + 10);
		else
			return false;
		uuid[destination++] = (uint8_t)((high << 4) | low);
	}
	return destination == 16;
}

static char *ozone_read_pem(const char *path)
{
	struct stat file_stat;
	size_t remaining, offset = 0;
	char *contents;
	int file;

	file = open(path, O_RDONLY | O_CLOEXEC);
	if (file < 0)
		return NULL;
	if (fstat(file, &file_stat) != 0 || !S_ISREG(file_stat.st_mode) ||
	    file_stat.st_size <= 0 || file_stat.st_size > OZONE_PEM_MAX_BYTES) {
		close(file);
		return NULL;
	}
	remaining = (size_t)file_stat.st_size;
	contents = gsh_malloc(remaining + 1);
	if (contents == NULL) {
		close(file);
		return NULL;
	}
	while (offset < remaining) {
		ssize_t count = read(file, contents + offset, remaining - offset);

		if (count <= 0) {
			gsh_free(contents);
			close(file);
			return NULL;
		}
		offset += (size_t)count;
	}
	close(file);
	if (memchr(contents, '\0', remaining) != NULL) {
		gsh_free(contents);
		return NULL;
	}
	contents[remaining] = '\0';
	return contents;
}

static fsal_status_t ozone_status(uint32_t status)
{
	if (status == 0)
		return fsalstat(ERR_FSAL_NO_ERROR, 0);
	if (status == 10004)
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	if (status == 13)
		return fsalstat(ERR_FSAL_ACCESS, 0);
	return fsalstat(ERR_FSAL_SERVERFAULT, (int)status);
}

static void ozone_release_export(struct fsal_export *export_hdl)
{
	struct ozone_fsal_export *export = container_of(export_hdl,
								struct ozone_fsal_export,
								export);

	if (export->root != NULL) {
		export->root->obj_handle.obj_ops->put_ref(&export->root->obj_handle);
		export->root = NULL;
	}
	if (export->client != NULL) {
		ozone_grpc_client_destroy(export->client);
		export->client = NULL;
	}
	fsal_detach_export(export_hdl->fsal, &export_hdl->exports);
	free_export_ops(export_hdl);
	gsh_free(export->export_path);
	gsh_free(export);
}

static fsal_status_t ozone_wire_to_host(struct fsal_export *export_hdl,
					fsal_digesttype_t input_type,
					struct gsh_buffdesc *fh_desc,
					int flags)
{
	struct ozone_fsal_export *export = container_of(export_hdl,
								struct ozone_fsal_export,
								export);
	struct ozone_nfs_handle identity;

	(void)flags;
	if ((input_type != FSAL_DIGEST_NFSV3 && input_type != FSAL_DIGEST_NFSV4) ||
	    fh_desc->len != OZONE_NFS_HANDLE_LENGTH ||
	    ozone_nfs_handle_decode(fh_desc->addr, fh_desc->len, &identity) != 0 ||
	    memcmp(identity.export_id, export->export_uuid, sizeof(export->export_uuid)) != 0 ||
	    identity.volume_id != export->volume_id || identity.bucket_id != export->bucket_id)
		return fsalstat(ERR_FSAL_BADHANDLE, 0);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static fsal_status_t ozone_get_dynamic_info(struct fsal_export *export_hdl,
					    struct fsal_obj_handle *obj_hdl,
					    fsal_dynamicfsinfo_t *info)
{
	(void)export_hdl;
	(void)obj_hdl;
	info->total_bytes = 0;
	info->free_bytes = 0;
	info->avail_bytes = 0;
	info->total_files = 0;
	info->free_files = 0;
	info->avail_files = 0;
	info->time_delta.tv_sec = 0;
	info->time_delta.tv_nsec = FSAL_DEFAULT_TIME_DELTA_NSEC;
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static struct state_t *ozone_alloc_state(struct fsal_export *export_hdl,
					 enum state_type state_type,
					 struct state_t *related_state)
{
	(void)export_hdl;
	return init_state(gsh_calloc(1, sizeof(struct state_t)), NULL, state_type, related_state);
}

void ozone_fsal_export_ops_init(struct export_ops *ops)
{
	ops->release = ozone_release_export;
	ops->lookup_path = ozone_fsal_lookup_path;
	ops->wire_to_host = ozone_wire_to_host;
	ops->create_handle = ozone_fsal_create_handle;
	ops->get_fs_dynamic_info = ozone_get_dynamic_info;
	ops->alloc_state = ozone_alloc_state;
}

fsal_status_t ozone_fsal_create_export(struct fsal_module *fsal_hdl,
				       void *parse_node,
				       struct config_error_type *err_type,
				       const struct fsal_up_vector *up_ops)
{
	struct ozone_export_config config = {0};
	struct ozone_fsal_export *export;
	char *trusted_ca = NULL, *client_certificate = NULL, *client_key = NULL;
	uint64_t operations = 0;
	uint32_t nfs_status = 0;
	int result;
	fsal_status_t status;

	export = gsh_calloc(1, sizeof(*export));
	if (export == NULL)
		return fsalstat(ERR_FSAL_NOMEM, 0);
	fsal_export_init(&export->export);
	export->export.fsal = fsal_hdl;
	export->export.up_ops = up_ops;
	ozone_fsal_export_ops_init(&export->export.exp_ops);
	result = load_config_from_node(parse_node, &ozone_export_param_block,
				       &config, true, err_type);
	if (result != 0 || config.volume_id == 0 || config.bucket_id == 0 ||
	    !ozone_parse_export_uuid(config.export_uuid, export->export_uuid)) {
		status = fsalstat(ERR_FSAL_INVAL, result == 0 ? EINVAL : result);
		goto fail;
	}
	export->volume_id = config.volume_id;
	export->bucket_id = config.bucket_id;
	export->export_path = gsh_strdup(CTX_FULLPATH(op_ctx));
	if (export->export_path == NULL) {
		status = fsalstat(ERR_FSAL_NOMEM, 0);
		goto fail;
	}
	trusted_ca = ozone_read_pem(config.trusted_ca_file);
	client_certificate = ozone_read_pem(config.client_certificate_file);
	client_key = ozone_read_pem(config.client_key_file);
	if (trusted_ca == NULL || client_certificate == NULL || client_key == NULL) {
		status = fsalstat(ERR_FSAL_INVAL, errno);
		goto fail;
	}
	export->client = ozone_grpc_client_create(config.address, trusted_ca,
		client_certificate, client_key, config.timeout_millis);
	if (export->client == NULL) {
		status = fsalstat(ERR_FSAL_INVAL, EINVAL);
		goto fail;
	}
	result = ozone_grpc_capabilities(export->client, export->export_uuid, 1,
					&nfs_status, &operations);
	if (result != 0) {
		LogCrit(COMPONENT_FSAL,
			"OZONE capability request failed with gRPC status %d", result);
		status = fsalstat(ERR_FSAL_DELAY, result);
		goto fail;
	}
	status = ozone_status(nfs_status);
	if (FSAL_IS_ERROR(status)) {
		LogCrit(COMPONENT_FSAL, "OZONE capability request returned NFS status %u", nfs_status);
		goto fail;
	}
	if ((operations & OZONE_REQUIRED_OPERATIONS) != OZONE_REQUIRED_OPERATIONS) {
		status = fsalstat(ERR_FSAL_NOTSUPP, 0);
		goto fail;
	}
	status = ozone_fsal_create_root(export);
	if (FSAL_IS_ERROR(status))
		goto fail;
	result = fsal_attach_export(fsal_hdl, &export->export.exports);
	if (result != 0) {
		status = posix2fsal_status(result);
		goto fail;
	}
	export->export.fsal = fsal_hdl;
	export->export.up_ops = up_ops;
	op_ctx->fsal_export = &export->export;
	gsh_free(config.address);
	gsh_free(config.export_uuid);
	gsh_free(config.trusted_ca_file);
	gsh_free(config.client_certificate_file);
	gsh_free(config.client_key_file);
	gsh_free(trusted_ca);
	gsh_free(client_certificate);
	gsh_free(client_key);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);

fail:
	if (export->root != NULL)
		export->root->obj_handle.obj_ops->put_ref(&export->root->obj_handle);
	if (export->client != NULL)
		ozone_grpc_client_destroy(export->client);
	free_export_ops(&export->export);
	gsh_free(export->export_path);
	gsh_free(export);
	gsh_free(config.address);
	gsh_free(config.export_uuid);
	gsh_free(config.trusted_ca_file);
	gsh_free(config.client_certificate_file);
	gsh_free(config.client_key_file);
	gsh_free(trusted_ca);
	gsh_free(client_certificate);
	gsh_free(client_key);
	return status;
}

fsal_status_t ozone_fsal_update_export(struct fsal_module *fsal_hdl,
				       void *parse_node,
				       struct config_error_type *err_type,
				       struct fsal_export *original,
				       struct fsal_module *updated_super)
{
	(void)fsal_hdl;
	(void)parse_node;
	(void)err_type;
	(void)original;
	(void)updated_super;
	return fsalstat(ERR_FSAL_NOTSUPP, 0);
}
