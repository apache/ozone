/**                                                                             
 * Licensed to the Apache Software Foundation (ASF) under one or more           
 * contributor license agreements.  See the NOTICE file distributed with this   
 * work for additional information regarding copyright ownership.  The ASF      
 * licenses this file to you under the Apache License, Version 2.0 (the         
 * "License"); you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at                                      
 * <p>                                                                          
 * http://www.apache.org/licenses/LICENSE-2.0                                   
 * <p>                                                                          
 * Unless required by applicable law or agreed to in writing, software          
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT     
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the     
 * License for the specific language governing permissions and limitations under
 * the License.                                                                 
 */

#ifndef __FAILURE_INJECTOR_FS_H
#define __FAILURE_INJECTOR_FS_H

#define FUSE_USE_VERSION 31

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <fuse3/fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#ifdef __FreeBSD__
#include <sys/socket.h>
#include <sys/un.h>
#endif
#include <sys/time.h>


#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include<string>
#include"failure_injector.h"
#include "run_grpc_service.h"

namespace NoiseInjector {

namespace FileSystem {

class FailureInjectorFs {
private:
    /* Helper function for mknod operation */
    static int mknod_wrapper(int dirfd, const char *path,
                             const char *link, int mode, dev_t rdev);
public:
    FailureInjectorFs(FailureInjector *);
    /*
     * Following operations correspond with their corresponding
     * Fuse operations vector.
     */

    static void *fifs_init(struct fuse_conn_info *conn,
                          struct fuse_config *cfg);

    static int fifs_getattr(const char *path, struct stat *stbuf,
                           struct fuse_file_info *fi);

    static int fifs_access(const char *path, int mask);

    static int fifs_readlink(const char *path, char *buf, size_t size);

    static int fifs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		                   off_t offset, struct fuse_file_info *fi,
		                   enum fuse_readdir_flags flags);

    static int fifs_mknod(const char *path, mode_t mode, dev_t rdev);

    static int fifs_mkdir(const char *path, mode_t mode);

    static int fifs_unlink(const char *path);

    static int fifs_rmdir(const char *path);

    static int fifs_symlink(const char *from, const char *to);

    static int fifs_rename(const char *from, const char *to,
                          unsigned int flags);

    static int fifs_link(const char *from, const char *to);

    static int fifs_chmod(const char *path, mode_t mode,
                         struct fuse_file_info *fi);

    static int fifs_chown(const char *path, uid_t uid, gid_t gid,
                         struct fuse_file_info *fi);

    static int fifs_truncate(const char *path, off_t size,
                            struct fuse_file_info *fi);

#ifdef HAVE_UTIMENSAT
    static int fifs_utimens(const char *path, const struct timespec ts[2],
                           struct fuse_file_info *fi);
#endif

    static int fifs_create(const char *path, mode_t mode,
                          struct fuse_file_info *fi);

    static int fifs_open(const char *path, struct fuse_file_info *fi);

    static int fifs_read(const char *path, char *buf, size_t size,
                        off_t offset, struct fuse_file_info *fi);

    static int fifs_write(const char *path, const char *buf, size_t size,
		                 off_t offset, struct fuse_file_info *fi);

    static int fifs_statfs(const char *path, struct statvfs *stbuf);

    static int fifs_release(const char *path, struct fuse_file_info *fi);

    static int fifs_fsync(const char *path, int isdatasync,
                         struct fuse_file_info *fi);

#ifdef HAVE_POSIX_FALLOCATE
    static int fifs_fallocate(const char *path, int mode, off_t offset,
                             off_t length, struct fuse_file_info *fi);
#endif

#ifdef HAVE_SETXATTR
    static int fifs_setxattr(const char *path, const char *name,
                            const char *value, size_t size, int flags);

    static int fifs_getxattr(const char *path, const char *name, char *value,
                            size_t size);

    static int fifs_listxattr(const char *path, char *list, size_t size);

    static int fifs_removexattr(const char *path, const char *name);
#endif /* HAVE_SETXATTR */

    static off_t fifs_lseek(const char *path, off_t off, int whence,
                           struct fuse_file_info *fi);

    /* Helper functions to setup/access Fuse operations vector */

    static bool CheckForInjectedError(std::string path, std::string op,
                               int *injected_error);

    static void FillCorruptData(char *buf, size_t size);

    void load_operations();

    const struct fuse_operations *getOperations();

    FailureInjector& GetFailureInjector();

private:
    /* Fuse operations vector */
	static struct fuse_operations mFuseOperationsVec;

    static FailureInjector *mFailureInjector;

    static RunGrpcService *mGrpcSever; 
};
}
}

#endif /* __FAILURE_INJECTOR_FS_H */
