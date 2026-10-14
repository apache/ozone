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

#include "ozone_internal.h"
#include "fsal_up.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static struct fsal_obj_ops ozone_handle_ops;
static const uint8_t ozone_krb5_mechanism[] = {
	0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02
};

static fsal_status_t ozone_status(uint32_t status)
{
	switch (status) {
	case 0:
		return fsalstat(ERR_FSAL_NO_ERROR, 0);
	case 13: /* NFS4ERR_ACCESS */
		return fsalstat(ERR_FSAL_ACCESS, 0);
	case 5: /* NFS4ERR_IO */
		return fsalstat(ERR_FSAL_IO, 0);
	case 17: /* NFS4ERR_EXIST */
		return fsalstat(ERR_FSAL_EXIST, 0);
	case 2: /* NFS4ERR_NOENT */
		return fsalstat(ERR_FSAL_NOENT, 0);
	case 70: /* NFS4ERR_STALE */
		return fsalstat(ERR_FSAL_STALE, 0);
	case 10001: /* NFS4ERR_BADHANDLE */
		return fsalstat(ERR_FSAL_BADHANDLE, 0);
	case 10003: /* NFS4ERR_BAD_COOKIE */
		return fsalstat(ERR_FSAL_BADCOOKIE, 0);
	case 10004: /* NFS4ERR_NOTSUPP */
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	case 20: /* NFS4ERR_NOTDIR */
		return fsalstat(ERR_FSAL_NOTDIR, 0);
	case 66: /* NFS4ERR_NOTEMPTY */
		return fsalstat(ERR_FSAL_NOTEMPTY, 0);
	default:
		return fsalstat(ERR_FSAL_SERVERFAULT, 0);
	}
}

static bool ozone_request_identity(const char **principal, size_t *length,
				   uint32_t *protection)
{
	if (op_ctx == NULL || op_ctx->client_rpcsec_gss_principal_len == 0 ||
	    op_ctx->client_rpcsec_gss_principal_len >
		    sizeof(op_ctx->client_rpcsec_gss_principal) ||
	    op_ctx->client_rpcsec_gss_mech_len == 0 ||
	    op_ctx->client_rpcsec_gss_mech_len != sizeof(ozone_krb5_mechanism) ||
	    memcmp(op_ctx->client_rpcsec_gss_mech, ozone_krb5_mechanism,
		   sizeof(ozone_krb5_mechanism)) != 0 ||
	    memchr(op_ctx->client_rpcsec_gss_principal, '\0',
		   op_ctx->client_rpcsec_gss_principal_len) != NULL ||
	    op_ctx->client_rpcsec_gss_service < FSAL_RPCSEC_GSS_PROTECTION_INTEGRITY ||
	    op_ctx->client_rpcsec_gss_service > FSAL_RPCSEC_GSS_PROTECTION_PRIVACY)
		return false;

	*principal = op_ctx->client_rpcsec_gss_principal;
	*length = op_ctx->client_rpcsec_gss_principal_len;
	*protection = op_ctx->client_rpcsec_gss_service;
	return true;
}

static void ozone_copy_attributes(struct ozone_fsal_handle *handle,
				  const struct ozone_nfs_attributes *source)
{
	handle->attributes = *source;
	handle->obj_handle.fileid = source->object_id;
	handle->obj_handle.type = source->directory ? DIRECTORY : REGULAR_FILE;
	handle->obj_handle.fsid.major = handle->identity.volume_id;
	handle->obj_handle.fsid.minor = handle->identity.bucket_id;
}

static void ozone_fill_attrs(const struct ozone_fsal_handle *handle,
			     struct fsal_attrlist *attrs)
{
	uint64_t requested = attrs->request_mask;
	uint64_t supported = OZONE_SUPPORTED_ATTRIBUTES;

	attrs->valid_mask = 0;
	attrs->supported = supported;
	attrs->type = handle->attributes.directory ? DIRECTORY : REGULAR_FILE;
	attrs->filesize = handle->attributes.size;
	attrs->fsid = handle->obj_handle.fsid;
	attrs->fsid3 = squash_fsid(&handle->obj_handle.fsid);
	attrs->fileid = handle->attributes.object_id;
	/* OM performs the authoritative ACL check for every gateway operation. */
	attrs->mode = handle->attributes.directory ? 0777 : 0666;
	attrs->numlinks = handle->attributes.directory ? 2 : 1;
	attrs->owner = 0;
	attrs->group = 0;
	attrs->atime.tv_sec = (time_t)(handle->attributes.modification_time_millis / 1000);
	attrs->atime.tv_nsec = (long)(handle->attributes.modification_time_millis % 1000) * 1000000;
	attrs->mtime = attrs->atime;
	attrs->ctime.tv_sec = (time_t)(handle->attributes.creation_time_millis / 1000);
	attrs->ctime.tv_nsec = (long)(handle->attributes.creation_time_millis % 1000) * 1000000;
	attrs->creation = attrs->ctime;
	attrs->change = handle->attributes.change_id;
	attrs->spaceused = handle->attributes.size;
	attrs->valid_mask = requested & supported;
}

static struct ozone_fsal_handle *ozone_new_handle(struct ozone_fsal_export *export,
						 const struct ozone_nfs_attributes *attributes)
{
	struct ozone_fsal_handle *handle = gsh_calloc(1, sizeof(*handle));

	if (handle == NULL)
		return NULL;
	if (ozone_nfs_handle_decode(attributes->handle, sizeof(attributes->handle),
				     &handle->identity) != 0 ||
	    memcmp(handle->identity.export_id, export->export_uuid,
		   sizeof(export->export_uuid)) != 0 ||
	    handle->identity.volume_id != export->volume_id ||
	    handle->identity.bucket_id != export->bucket_id) {
		gsh_free(handle);
		return NULL;
	}
	handle->export = export;
	memcpy(handle->wire_handle, attributes->handle, sizeof(handle->wire_handle));
	atomic_init(&handle->references, 1);
	fsal_obj_handle_init(&handle->obj_handle, &export->export,
			     attributes->directory ? DIRECTORY : REGULAR_FILE);
	handle->obj_handle.obj_ops = &ozone_handle_ops;
	ozone_copy_attributes(handle, attributes);
	return handle;
}

static fsal_status_t ozone_get_attributes(struct ozone_fsal_handle *handle,
					  struct ozone_nfs_attributes *attributes)
{
	const char *principal;
	size_t principal_size;
	uint32_t protection, status = 0;
	int transport;

	if (!ozone_request_identity(&principal, &principal_size, &protection))
		return fsalstat(ERR_FSAL_ACCESS, 0);
	transport = ozone_grpc_getattr(handle->export->client, handle->export->export_uuid,
		principal, principal_size, protection, handle->wire_handle,
		sizeof(handle->wire_handle), attributes, &status);
	if (transport != 0)
		return fsalstat(ERR_FSAL_DELAY, 0);
	return ozone_status(status);
}

static void ozone_get_ref(struct fsal_obj_handle *obj_hdl)
{
	struct ozone_fsal_handle *handle = container_of(obj_hdl,
							       struct ozone_fsal_handle,
							       obj_handle);

	atomic_fetch_add_explicit(&handle->references, 1, memory_order_relaxed);
}

static void ozone_put_ref(struct fsal_obj_handle *obj_hdl)
{
	struct ozone_fsal_handle *handle = container_of(obj_hdl,
							       struct ozone_fsal_handle,
							       obj_handle);

	if (atomic_fetch_sub_explicit(&handle->references, 1, memory_order_acq_rel) == 1) {
		fsal_obj_handle_fini(obj_hdl);
		gsh_free(handle);
	}
}

static void ozone_release(struct fsal_obj_handle *obj_hdl)
{
	ozone_put_ref(obj_hdl);
}

static fsal_status_t ozone_lookup(struct fsal_obj_handle *dir_hdl, const char *name,
				  struct fsal_obj_handle **obj_hdl,
				  struct fsal_attrlist *attrs_out)
{
	struct ozone_fsal_handle *directory = container_of(dir_hdl,
								    struct ozone_fsal_handle,
								    obj_handle);
	struct ozone_nfs_attributes attributes = {0};
	const char *principal;
	size_t principal_size;
	uint32_t protection, status = 0;
	int transport;
	struct ozone_fsal_handle *object;

	if (dir_hdl->type != DIRECTORY)
		return fsalstat(ERR_FSAL_NOTDIR, 0);
	if (name == NULL || strlen(name) > OZONE_NFS_MAX_NAME_BYTES)
		return fsalstat(ERR_FSAL_INVAL, 0);
	if (!ozone_request_identity(&principal, &principal_size, &protection))
		return fsalstat(ERR_FSAL_ACCESS, 0);
	transport = ozone_grpc_lookup(directory->export->client,
		directory->export->export_uuid, principal, principal_size, protection,
		directory->wire_handle, sizeof(directory->wire_handle),
		(const uint8_t *)name, strlen(name), &attributes, &status);
	if (transport != 0)
		return fsalstat(ERR_FSAL_DELAY, 0);
	if (status != 0)
		return ozone_status(status);
	object = ozone_new_handle(directory->export, &attributes);
	if (object == NULL)
		return fsalstat(ERR_FSAL_SERVERFAULT, 0);
	*obj_hdl = &object->obj_handle;
	if (attrs_out != NULL)
		ozone_fill_attrs(object, attrs_out);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static fsal_status_t ozone_mkdir(struct fsal_obj_handle *dir_hdl, const char *name,
				 struct fsal_attrlist *attrs_in,
				 struct fsal_obj_handle **new_obj,
				 struct fsal_attrlist *attrs_out)
{
	struct ozone_fsal_handle *directory = container_of(dir_hdl,
									    struct ozone_fsal_handle,
								    obj_handle);
	struct ozone_nfs_attributes attributes = {0};
	const char *principal;
	size_t principal_size, name_size;
	uint32_t protection, nfs_status = 0;
	int transport;
	struct ozone_fsal_handle *created;

	(void)attrs_in;
	if (dir_hdl->type != DIRECTORY)
		return fsalstat(ERR_FSAL_NOTDIR, ENOTDIR);
	if (name == NULL)
		return fsalstat(ERR_FSAL_INVAL, EINVAL);
	name_size = strnlen(name, OZONE_NFS_MAX_NAME_BYTES + 1);
	if (name_size == 0 || name_size > OZONE_NFS_MAX_NAME_BYTES || strchr(name, '/') != NULL)
		return fsalstat(ERR_FSAL_INVAL, EINVAL);
	if (!ozone_request_identity(&principal, &principal_size, &protection))
		return fsalstat(ERR_FSAL_ACCESS, 0);
	transport = ozone_grpc_mkdir(directory->export->client, directory->export->export_uuid,
		principal, principal_size, protection, directory->wire_handle,
		sizeof(directory->wire_handle), (const uint8_t *)name, name_size,
		&attributes, &nfs_status);
	if (transport != 0)
		return fsalstat(ERR_FSAL_DELAY, transport);
	{
		fsal_status_t status = ozone_status(nfs_status);
		if (FSAL_IS_ERROR(status))
			return status;
	}
	created = ozone_new_handle(directory->export, &attributes);
	if (created == NULL)
		return fsalstat(ERR_FSAL_SERVERFAULT, 0);
	if (new_obj != NULL)
		*new_obj = &created->obj_handle;
	if (attrs_out != NULL)
		ozone_fill_attrs(created, attrs_out);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static fsal_status_t ozone_unlink(struct fsal_obj_handle *dir_hdl,
				  struct fsal_obj_handle *obj_hdl, const char *name)
{
	struct ozone_fsal_handle *directory = container_of(dir_hdl,
								    struct ozone_fsal_handle,
								    obj_handle);
	struct ozone_fsal_handle *object = container_of(obj_hdl,
								  struct ozone_fsal_handle,
								  obj_handle);
	const char *principal;
	size_t principal_size, name_size;
	uint32_t protection, nfs_status = 0;
	int transport;

	if (dir_hdl->type != DIRECTORY)
		return fsalstat(ERR_FSAL_NOTDIR, ENOTDIR);
	if (name == NULL)
		return fsalstat(ERR_FSAL_INVAL, EINVAL);
	name_size = strnlen(name, OZONE_NFS_MAX_NAME_BYTES + 1);
	if (name_size == 0 || name_size > OZONE_NFS_MAX_NAME_BYTES || strchr(name, '/') != NULL)
		return fsalstat(ERR_FSAL_INVAL, EINVAL);
	if (!ozone_request_identity(&principal, &principal_size, &protection))
		return fsalstat(ERR_FSAL_ACCESS, 0);
	transport = ozone_grpc_remove(directory->export->client, directory->export->export_uuid,
		principal, principal_size, protection, directory->wire_handle,
		sizeof(directory->wire_handle), object->wire_handle, sizeof(object->wire_handle),
		(const uint8_t *)name, name_size, &nfs_status);
	return transport == 0 ? ozone_status(nfs_status) : fsalstat(ERR_FSAL_DELAY, transport);
}

static fsal_status_t ozone_rename(struct fsal_obj_handle *obj_hdl,
				  struct fsal_obj_handle *olddir_hdl, const char *old_name,
				  struct fsal_obj_handle *newdir_hdl, const char *new_name)
{
	struct ozone_fsal_handle *object = container_of(obj_hdl,
								  struct ozone_fsal_handle,
								  obj_handle);
	struct ozone_fsal_handle *old_directory = container_of(olddir_hdl,
									    struct ozone_fsal_handle,
								    obj_handle);
	struct ozone_fsal_handle *new_directory = container_of(newdir_hdl,
									    struct ozone_fsal_handle,
								    obj_handle);
	const char *principal;
	size_t principal_size, old_name_size, new_name_size;
	uint32_t protection, nfs_status = 0;
	int transport;

	if (olddir_hdl->type != DIRECTORY || newdir_hdl->type != DIRECTORY)
		return fsalstat(ERR_FSAL_NOTDIR, ENOTDIR);
	if (old_name == NULL || new_name == NULL)
		return fsalstat(ERR_FSAL_INVAL, EINVAL);
	old_name_size = strnlen(old_name, OZONE_NFS_MAX_NAME_BYTES + 1);
	new_name_size = strnlen(new_name, OZONE_NFS_MAX_NAME_BYTES + 1);
	if (old_name_size == 0 || old_name_size > OZONE_NFS_MAX_NAME_BYTES || strchr(old_name, '/') != NULL ||
	    new_name_size == 0 || new_name_size > OZONE_NFS_MAX_NAME_BYTES || strchr(new_name, '/') != NULL)
		return fsalstat(ERR_FSAL_INVAL, EINVAL);
	if (!ozone_request_identity(&principal, &principal_size, &protection))
		return fsalstat(ERR_FSAL_ACCESS, 0);
	transport = ozone_grpc_rename(object->export->client, object->export->export_uuid,
		principal, principal_size, protection, object->wire_handle, sizeof(object->wire_handle),
		old_directory->wire_handle, sizeof(old_directory->wire_handle),
		(const uint8_t *)old_name, old_name_size, new_directory->wire_handle,
		sizeof(new_directory->wire_handle), (const uint8_t *)new_name, new_name_size,
		&nfs_status);
	return transport == 0 ? ozone_status(nfs_status) : fsalstat(ERR_FSAL_DELAY, transport);
}

static fsal_status_t ozone_getattrs(struct fsal_obj_handle *obj_hdl,
				    struct fsal_attrlist *attrs_out)
{
	struct ozone_fsal_handle *handle = container_of(obj_hdl,
							       struct ozone_fsal_handle,
							       obj_handle);
	struct ozone_nfs_attributes attributes = {0};
	fsal_status_t status = ozone_get_attributes(handle, &attributes);

	if (FSAL_IS_ERROR(status))
		return status;
	if (attributes.size < handle->attributes.size) {
		attributes.size = handle->attributes.size;
		if (attributes.change_id < handle->attributes.change_id)
			attributes.change_id = handle->attributes.change_id;
		attributes.modification_time_millis = handle->attributes.modification_time_millis;
	}
	ozone_copy_attributes(handle, &attributes);
	ozone_fill_attrs(handle, attrs_out);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static fsal_status_t ozone_open2(struct fsal_obj_handle *obj_hdl,
				 struct state_t *state,
				 fsal_openflags_t openflags,
				 enum fsal_create_mode createmode,
				 const char *name,
				 struct fsal_attrlist *attrs_in,
				 fsal_verifier_t verifier,
				 struct fsal_obj_handle **new_obj,
				 struct fsal_attrlist *attrs_out,
				 bool *caller_perm_check)
{
	struct ozone_fsal_handle *handle;
	struct fsal_obj_handle *opened = obj_hdl;
	struct ozone_nfs_attributes attributes = {0};
	fsal_status_t status;

	(void)state;
	(void)attrs_in;
	(void)verifier;
	if ((openflags & FSAL_O_TRUNC) != 0 && createmode == FSAL_NO_CREATE)
		return fsalstat(ERR_FSAL_NOTSUPP, EOPNOTSUPP);
	if ((openflags & (FSAL_O_READ | FSAL_O_WRITE)) == 0)
		return fsalstat(ERR_FSAL_INVAL, EINVAL);
	if (createmode != FSAL_NO_CREATE) {
		struct ozone_fsal_handle *parent = container_of(obj_hdl,
									struct ozone_fsal_handle,
									obj_handle);
		struct ozone_nfs_attributes created = {0};
		char transfer_id[OZONE_NFS_MAX_TRANSFER_ID_BYTES];
		const char *principal;
		size_t principal_size, name_size, transfer_id_size = 0;
		uint32_t protection, nfs_status = 0;
		int transport;

		if (name == NULL || parent->obj_handle.type != DIRECTORY ||
		    (createmode != FSAL_GUARDED && createmode != FSAL_UNCHECKED &&
		     createmode != FSAL_EXCLUSIVE && createmode != FSAL_EXCLUSIVE_41))
			return fsalstat(ERR_FSAL_NOTSUPP, EOPNOTSUPP);
		name_size = strnlen(name, OZONE_NFS_MAX_NAME_BYTES + 1);
		if (name_size == 0 || name_size > OZONE_NFS_MAX_NAME_BYTES || strchr(name, '/') != NULL)
			return fsalstat(ERR_FSAL_INVAL, EINVAL);
		if (!ozone_request_identity(&principal, &principal_size, &protection))
			return fsalstat(ERR_FSAL_ACCESS, 0);
		transport = ozone_grpc_create(parent->export->client, parent->export->export_uuid,
			principal, principal_size, protection, parent->wire_handle,
			sizeof(parent->wire_handle), (const uint8_t *)name, name_size,
			&created, transfer_id, sizeof(transfer_id), &transfer_id_size, &nfs_status);
		if (transport != 0)
			return fsalstat(ERR_FSAL_DELAY, transport);
		status = ozone_status(nfs_status);
		if (FSAL_IS_ERROR(status))
			return status;
		handle = ozone_new_handle(parent->export, &created);
		if (handle == NULL)
			return fsalstat(ERR_FSAL_SERVERFAULT, 0);
		opened = &handle->obj_handle;
		if (attrs_out != NULL)
			ozone_fill_attrs(handle, attrs_out);
	} else if (name != NULL) {
		status = ozone_lookup(obj_hdl, name, &opened, attrs_out);
		if (FSAL_IS_ERROR(status))
			return status;
	} else {
		handle = container_of(opened, struct ozone_fsal_handle, obj_handle);
		status = ozone_get_attributes(handle, &attributes);
		if (FSAL_IS_ERROR(status))
			return status;
		ozone_copy_attributes(handle, &attributes);
		if (attrs_out != NULL)
			ozone_fill_attrs(handle, attrs_out);
	}
	if (opened->type != REGULAR_FILE) {
		if (name != NULL)
			opened->obj_ops->put_ref(opened);
		return fsalstat(ERR_FSAL_ISDIR, EISDIR);
	}
	if (new_obj != NULL)
		*new_obj = opened;
	*caller_perm_check = false;
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static fsal_openflags_t ozone_status2(struct fsal_obj_handle *obj_hdl, struct state_t *state)
{
	fsal_openflags_t flags = 0;
	uint32_t share_access;

	(void)obj_hdl;
	if (state == NULL || state->state_type != STATE_TYPE_SHARE)
		return 0;
	share_access = state->state_data.share.share_access;
	if ((share_access & OPEN4_SHARE_ACCESS_READ) != 0)
		flags |= FSAL_O_READ;
	if ((share_access & OPEN4_SHARE_ACCESS_WRITE) != 0)
		flags |= FSAL_O_WRITE;
	return flags;
}

static fsal_status_t ozone_reopen2(struct fsal_obj_handle *obj_hdl,
				   struct state_t *state,
				   fsal_openflags_t openflags)
{
	struct ozone_fsal_handle *handle = container_of(obj_hdl,
								struct ozone_fsal_handle,
								obj_handle);
	struct ozone_nfs_attributes attributes = {0};

	(void)state;
	if ((openflags & FSAL_O_TRUNC) != 0)
		return fsalstat(ERR_FSAL_NOTSUPP, EOPNOTSUPP);
	return ozone_get_attributes(handle, &attributes);
}

static void ozone_write2(struct fsal_obj_handle *obj_hdl, bool bypass,
			 fsal_async_cb done_cb, struct fsal_io_arg *write_arg,
			 void *caller_arg)
{
	struct ozone_fsal_handle *handle = container_of(obj_hdl,
								       struct ozone_fsal_handle,
								       obj_handle);
	const char *principal;
	size_t principal_size, data_size = 0, copied = 0, i;
	uint32_t protection, count = 0, committed = 0, nfs_status = 0;
	uint8_t *data;
	int transport;
	fsal_status_t status;

	(void)bypass;
	if (write_arg->info != NULL || write_arg->iov_count < 0 ||
	    !ozone_request_identity(&principal, &principal_size, &protection)) {
		done_cb(obj_hdl, fsalstat(write_arg->info != NULL ? ERR_FSAL_NOTSUPP :
					  ERR_FSAL_ACCESS, 0), write_arg, caller_arg);
		return;
	}
	for (i = 0; i < (size_t)write_arg->iov_count; i++) {
		if (write_arg->iov[i].iov_len > OZONE_NFS_MAX_READ_BYTES - data_size) {
			done_cb(obj_hdl, fsalstat(ERR_FSAL_INVAL, 0), write_arg, caller_arg);
			return;
		}
		data_size += write_arg->iov[i].iov_len;
	}
	if (data_size == 0) {
		write_arg->io_amount = 0;
		write_arg->fsal_stable = true;
		done_cb(obj_hdl, fsalstat(ERR_FSAL_NO_ERROR, 0), write_arg, caller_arg);
		return;
	}
	data = gsh_malloc(data_size);
	if (data == NULL) {
		done_cb(obj_hdl, fsalstat(ERR_FSAL_NOMEM, 0), write_arg, caller_arg);
		return;
	}
	for (i = 0; i < (size_t)write_arg->iov_count; i++) {
		memcpy(data + copied, write_arg->iov[i].iov_base, write_arg->iov[i].iov_len);
		copied += write_arg->iov[i].iov_len;
	}
	transport = ozone_grpc_write(handle->export->client, handle->export->export_uuid,
		principal, principal_size, protection, handle->wire_handle,
		sizeof(handle->wire_handle), NULL, 0, write_arg->offset, data, data_size,
		&count, &committed, &nfs_status);
	gsh_free(data);
	status = transport == 0 ? ozone_status(nfs_status) : fsalstat(ERR_FSAL_DELAY, transport);
	if (!FSAL_IS_ERROR(status) && count > data_size)
		status = fsalstat(ERR_FSAL_SERVERFAULT, 0);
	if (!FSAL_IS_ERROR(status)) {
		write_arg->io_amount = count;
		write_arg->fsal_stable = committed >= 1;
		if (count > 0 && write_arg->offset >= 0 &&
		    (uint64_t)write_arg->offset <= UINT64_MAX - count) {
			uint64_t end = (uint64_t)write_arg->offset + count;
			struct timespec now;

			if (end > handle->attributes.size)
				handle->attributes.size = end;
			handle->attributes.change_id++;
			if (clock_gettime(CLOCK_REALTIME, &now) == 0)
				handle->attributes.modification_time_millis =
					(uint64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
		}
	}
	done_cb(obj_hdl, status, write_arg, caller_arg);
}

static fsal_status_t ozone_commit_file(struct fsal_obj_handle *obj_hdl, uint32_t finalize)
{
	struct ozone_fsal_handle *handle = container_of(obj_hdl,
									       struct ozone_fsal_handle,
									       obj_handle);
	const char *principal;
	size_t principal_size;
	uint32_t protection, nfs_status = 0;
	int transport;
	fsal_status_t status;

	if (!ozone_request_identity(&principal, &principal_size, &protection))
		return fsalstat(ERR_FSAL_ACCESS, 0);
	transport = ozone_grpc_commit(handle->export->client, handle->export->export_uuid,
		principal, principal_size, protection, handle->wire_handle,
		sizeof(handle->wire_handle), finalize, &nfs_status);
	status = transport == 0 ? ozone_status(nfs_status) : fsalstat(ERR_FSAL_DELAY, transport);
	if (!FSAL_IS_ERROR(status)) {
		struct gsh_buffdesc key = {
			.addr = handle->wire_handle,
			.len = sizeof(handle->wire_handle)
		};

		status = handle->export->export.up_ops->invalidate(
			handle->export->export.up_ops, &key, FSAL_UP_INVALIDATE_ATTRS);
	}
	return status;
}

static fsal_status_t ozone_commit2(struct fsal_obj_handle *obj_hdl, off_t offset, size_t len)
{
	(void)offset;
	(void)len;
	return ozone_commit_file(obj_hdl, 0);
}

static fsal_status_t ozone_close2(struct fsal_obj_handle *obj_hdl, struct state_t *state)
{
	(void)state;
	return ozone_commit_file(obj_hdl, 1);
}

static fsal_status_t ozone_close(struct fsal_obj_handle *obj_hdl)
{
	(void)obj_hdl;
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static fsal_status_t ozone_readdir(struct fsal_obj_handle *dir_hdl,
				   fsal_cookie_t *whence, void *dir_state,
				   fsal_readdir_cb callback, attrmask_t attrmask, bool *eof)
{
	struct ozone_fsal_handle *directory = container_of(dir_hdl,
								    struct ozone_fsal_handle,
								    obj_handle);
	struct ozone_nfs_directory_entry entries[OZONE_NFS_MAX_DIRECTORY_ENTRIES];
	uint8_t cookie[OZONE_NFS_VERIFIER_BYTES] = {0};
	uint8_t verifier[OZONE_NFS_VERIFIER_BYTES] = {0};
	uint8_t response_verifier[OZONE_NFS_VERIFIER_BYTES] = {0};
	const uint8_t *cookie_input = NULL;
	const uint8_t *verifier_input = NULL;
	size_t cookie_size = 0, verifier_size = 0;
	const char *principal;
	size_t principal_size, count = 0, i;
	uint32_t protection, status = 0, backend_eof = 0;
	int transport;

	(void)attrmask;
	memset(entries, 0, sizeof(entries));
	if (dir_hdl->type != DIRECTORY)
		return fsalstat(ERR_FSAL_NOTDIR, 0);
	if (!ozone_request_identity(&principal, &principal_size, &protection))
		return fsalstat(ERR_FSAL_ACCESS, 0);
	if (whence != NULL && *whence != 0) {
		memcpy(cookie, whence, sizeof(cookie));
		cookie_input = cookie;
		cookie_size = sizeof(cookie);
		/* Match OM's verifier to the directory change attribute Ganesha returns. */
		for (i = 0; i < sizeof(verifier); i++)
			verifier[sizeof(verifier) - 1 - i] =
				(uint8_t)(directory->attributes.change_id >> (i * 8));
		verifier_input = verifier;
		verifier_size = sizeof(verifier);
	}

	transport = ozone_grpc_readdir(directory->export->client,
		directory->export->export_uuid, principal, principal_size, protection,
		directory->wire_handle, sizeof(directory->wire_handle),
		OZONE_NFS_MAX_DIRECTORY_ENTRIES, cookie_input, cookie_size,
		verifier_input, verifier_size, entries, OZONE_NFS_MAX_DIRECTORY_ENTRIES, &count,
		response_verifier, &backend_eof, &status);
	if (transport != 0)
		return fsalstat(ERR_FSAL_DELAY, 0);
	if (status != 0)
		return ozone_status(status);
	for (i = 0; i < count; i++) {
		struct ozone_fsal_handle *object;
		struct fsal_attrlist attrs;
		fsal_cookie_t entry_cookie;
		char name[OZONE_NFS_MAX_NAME_BYTES + 1];
		enum fsal_dir_result result;

		if (entries[i].name_size == 0 ||
		    entries[i].name_size > OZONE_NFS_MAX_NAME_BYTES ||
		    entries[i].cookie_size != sizeof(entry_cookie))
			return fsalstat(ERR_FSAL_SERVERFAULT, 0);
		object = ozone_new_handle(directory->export, &entries[i].attributes);
		if (object == NULL)
			return fsalstat(ERR_FSAL_SERVERFAULT, 0);
		memcpy(name, entries[i].name, entries[i].name_size);
		name[entries[i].name_size] = '\0';
		memcpy(&entry_cookie, entries[i].cookie, sizeof(entry_cookie));
		memset(&attrs, 0, sizeof(attrs));
		attrs.request_mask = attrmask;
		ozone_fill_attrs(object, &attrs);
		result = callback(name, &object->obj_handle, &attrs, dir_state,
				  entry_cookie);
		if (result == DIR_TERMINATE) {
			backend_eof = 0;
			break;
		}
	}
	*eof = backend_eof != 0;
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static void ozone_read2(struct fsal_obj_handle *obj_hdl, bool bypass,
			fsal_async_cb done_cb, struct fsal_io_arg *read_arg,
			void *caller_arg)
{
	struct ozone_fsal_handle *handle = container_of(obj_hdl,
							       struct ozone_fsal_handle,
							       obj_handle);
	const char *principal;
	size_t principal_size, capacity = 0, bytes_read = 0, offset = 0, i;
	uint32_t protection, eof = 0, status = 0, request_size;
	uint8_t *buffer;
	int transport;
	fsal_status_t result;

	(void)bypass;
	if (read_arg->info != NULL || !ozone_request_identity(&principal,
								&principal_size, &protection)) {
		done_cb(obj_hdl, fsalstat(read_arg->info != NULL ? ERR_FSAL_NOTSUPP :
					  ERR_FSAL_ACCESS, 0), read_arg, caller_arg);
		return;
	}
	if (read_arg->iov_count < 0) {
		done_cb(obj_hdl, fsalstat(ERR_FSAL_INVAL, 0), read_arg, caller_arg);
		return;
	}
	for (i = 0; i < (size_t)read_arg->iov_count; i++) {
		if (read_arg->iov[i].iov_len > SIZE_MAX - capacity) {
			done_cb(obj_hdl, fsalstat(ERR_FSAL_INVAL, 0), read_arg, caller_arg);
			return;
		}
		capacity += read_arg->iov[i].iov_len;
	}
	if (capacity > OZONE_NFS_MAX_READ_BYTES)
		capacity = OZONE_NFS_MAX_READ_BYTES;
	request_size = (uint32_t)capacity;
	buffer = gsh_malloc(capacity == 0 ? 1 : capacity);
	if (buffer == NULL) {
		done_cb(obj_hdl, fsalstat(ERR_FSAL_NOMEM, 0), read_arg, caller_arg);
		return;
	}
	transport = ozone_grpc_read(handle->export->client, handle->export->export_uuid,
		principal, principal_size, protection, handle->wire_handle,
		sizeof(handle->wire_handle), read_arg->offset, request_size, buffer,
		capacity, &bytes_read, &eof, &status);
	result = transport == 0 ? ozone_status(status) : fsalstat(ERR_FSAL_DELAY, 0);
	if (!FSAL_IS_ERROR(result)) {
		for (i = 0; i < (size_t)read_arg->iov_count && offset < bytes_read; i++) {
			size_t copy_size = read_arg->iov[i].iov_len;

			if (copy_size > bytes_read - offset)
				copy_size = bytes_read - offset;
			memcpy(read_arg->iov[i].iov_base, buffer + offset, copy_size);
			offset += copy_size;
		}
		read_arg->io_amount = bytes_read;
		read_arg->end_of_file = eof != 0;
	}
	gsh_free(buffer);
	done_cb(obj_hdl, result, read_arg, caller_arg);
}

static fsal_status_t ozone_handle_to_wire(const struct fsal_obj_handle *obj_hdl,
					  fsal_digesttype_t output_type,
					  struct gsh_buffdesc *fh_desc)
{
	const struct ozone_fsal_handle *handle = container_of(obj_hdl,
								      struct ozone_fsal_handle,
								      obj_handle);

	if ((output_type != FSAL_DIGEST_NFSV3 && output_type != FSAL_DIGEST_NFSV4) ||
	    fh_desc->len < sizeof(handle->wire_handle))
		return fsalstat(ERR_FSAL_TOOSMALL, 0);
	memcpy(fh_desc->addr, handle->wire_handle, sizeof(handle->wire_handle));
	fh_desc->len = sizeof(handle->wire_handle);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static void ozone_handle_to_key(struct fsal_obj_handle *obj_hdl,
				struct gsh_buffdesc *fh_desc)
{
	struct ozone_fsal_handle *handle = container_of(obj_hdl,
								struct ozone_fsal_handle,
								obj_handle);

	fh_desc->addr = handle->wire_handle;
	fh_desc->len = sizeof(handle->wire_handle);
}

fsal_status_t ozone_fsal_create_root(struct ozone_fsal_export *export)
{
	struct ozone_nfs_handle identity = {0};
	struct ozone_nfs_attributes attributes = {0};
	int result;

	memcpy(identity.export_id, export->export_uuid, sizeof(identity.export_id));
	identity.volume_id = export->volume_id;
	identity.bucket_id = export->bucket_id;
	identity.object_id = export->bucket_id;
	identity.kind = OZONE_NFS_BUCKET;
	result = ozone_nfs_handle_encode(&identity, attributes.handle,
					 sizeof(attributes.handle));
	if (result != 0)
		return fsalstat(ERR_FSAL_INVAL, result);
	attributes.directory = 1;
	attributes.object_id = export->bucket_id;
	attributes.change_id = 1;
	export->root = ozone_new_handle(export, &attributes);
	if (export->root == NULL)
		return fsalstat(ERR_FSAL_NOMEM, 0);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

fsal_status_t ozone_fsal_lookup_path(struct fsal_export *export_hdl,
				     const char *path,
				     struct fsal_obj_handle **obj_hdl,
				     struct fsal_attrlist *attrs_out)
{
	struct ozone_fsal_export *export = container_of(export_hdl,
								struct ozone_fsal_export,
								export);

	if (path == NULL || strcmp(path, export->export_path) != 0)
		return fsalstat(ERR_FSAL_NOENT, ENOENT);
	ozone_get_ref(&export->root->obj_handle);
	*obj_hdl = &export->root->obj_handle;
	if (attrs_out != NULL)
		ozone_fill_attrs(export->root, attrs_out);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static fsal_status_t ozone_setattr2(struct fsal_obj_handle *obj_hdl, bool bypass,
				    struct state_t *state,
				    struct fsal_attrlist *attrib_set)
{
	(void)obj_hdl;
	(void)bypass;
	(void)state;
	(void)attrib_set;
	return fsalstat(ERR_FSAL_NOTSUPP, EOPNOTSUPP);
}

void ozone_fsal_handle_ops_init(struct fsal_obj_ops *ops)
{
	fsal_default_obj_ops_init(ops);
	ops->get_ref = ozone_get_ref;
	ops->put_ref = ozone_put_ref;
	ops->release = ozone_release;
	ops->lookup = ozone_lookup;
	ops->mkdir = ozone_mkdir;
	ops->unlink = ozone_unlink;
	ops->rename = ozone_rename;
	ops->readdir = ozone_readdir;
	ops->getattrs = ozone_getattrs;
	ops->open2 = ozone_open2;
	ops->status2 = ozone_status2;
	ops->reopen2 = ozone_reopen2;
	ops->read2 = ozone_read2;
	ops->write2 = ozone_write2;
	ops->commit2 = ozone_commit2;
	ops->close = ozone_close;
	ops->close2 = ozone_close2;
	ops->setattr2 = ozone_setattr2;
	ops->handle_to_wire = ozone_handle_to_wire;
	ops->handle_to_key = ozone_handle_to_key;
	ozone_handle_ops = *ops;
}

fsal_status_t ozone_fsal_create_handle(struct fsal_export *export_hdl,
				       struct gsh_buffdesc *fh_desc,
				       struct fsal_obj_handle **obj_hdl,
				       struct fsal_attrlist *attrs_out)
{
	struct ozone_fsal_export *export = container_of(export_hdl,
								struct ozone_fsal_export,
								export);
	struct ozone_nfs_handle identity;
	struct ozone_nfs_attributes attributes = {0};
	struct ozone_fsal_handle *handle;
	fsal_status_t status;

	if (fh_desc->len != OZONE_NFS_HANDLE_LENGTH ||
	    ozone_nfs_handle_decode(fh_desc->addr, fh_desc->len, &identity) != 0 ||
	    memcmp(identity.export_id, export->export_uuid,
		   sizeof(export->export_uuid)) != 0 ||
	    identity.volume_id != export->volume_id ||
	    identity.bucket_id != export->bucket_id)
		return fsalstat(ERR_FSAL_STALE, 0);
	{
		const char *principal;
		size_t principal_size;
		uint32_t protection, nfs_status = 0;
		int transport;

		if (!ozone_request_identity(&principal, &principal_size, &protection))
			return fsalstat(ERR_FSAL_ACCESS, 0);
		transport = ozone_grpc_getattr(export->client, export->export_uuid,
			principal, principal_size, protection, fh_desc->addr,
			fh_desc->len, &attributes, &nfs_status);
		if (transport != 0)
			return fsalstat(ERR_FSAL_DELAY, 0);
		status = ozone_status(nfs_status);
		if (FSAL_IS_ERROR(status))
			return status;
	}
	handle = ozone_new_handle(export, &attributes);
	if (handle == NULL)
		return fsalstat(ERR_FSAL_STALE, 0);
	*obj_hdl = &handle->obj_handle;
	if (attrs_out != NULL)
		ozone_fill_attrs(handle, attrs_out);
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}
