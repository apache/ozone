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


#define FUSE_USE_VERSION 31

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <libgen.h>                                                             
#include <stdlib.h>                                                             

#include <fuse3/fuse.h>
#include <stdio.h>
#include <ctime>
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
#include "failure_injector_fs.h"
#include "failure_injector.h"
#include "run_grpc_service.h"
#include <string>
#include <iostream>
#include <sstream>
#include <strings.h>
#include <thread>
#include <cstdio>

using NoiseInjector::FailureInjector;
using NoiseInjector::RunGrpcService;

using namespace std;
using namespace NoiseInjector::FileSystem;

#define LOGFILE "/var/log/noise_injector_log.txt"

struct fuse_operations FailureInjectorFs::mFuseOperationsVec;
FailureInjector *FailureInjectorFs::mFailureInjector = NULL;
RunGrpcService *FailureInjectorFs::mGrpcSever = NULL;

FailureInjectorFs::FailureInjectorFs(FailureInjector *injector)
{
    mFailureInjector = injector;
    mGrpcSever = new RunGrpcService(mFailureInjector);
}

int FailureInjectorFs::mknod_wrapper(
    int dirfd,
    const char *path,
    const char *link,
    int mode,
    dev_t rdev)
{
    int res;
    
    if (S_ISREG(mode)) {
        res = openat(dirfd, path, O_CREAT | O_EXCL | O_WRONLY, mode);
        if (res >= 0)
        res = close(res);
    } else if (S_ISDIR(mode)) {
        res = mkdirat(dirfd, path, mode);
    } else if (S_ISLNK(mode) && link != NULL) {
        res = symlinkat(link, dirfd, path);
    } else if (S_ISFIFO(mode)) {
        res = mkfifoat(dirfd, path, mode);
    } else {
        res = mknodat(dirfd, path, mode, rdev);
    }
    
    return res;
}

void *FailureInjectorFs::fifs_init(
    struct fuse_conn_info *conn,
    struct fuse_config *cfg)
{
    extern void RunServer();
    (void) conn;
    cfg->use_ino = 1;
    
    std::freopen(LOGFILE, "w", stdout);

    /* 
     * Pick up changes from lower filesystem right away. This is
     * also necessary for better hardlink support. When the kernel
     * calls the unlink() handler, it does not know the inode of
     * the to-be-removed entry and can therefore not invalidate
     * the cache of the associated inode - resulting in an
     * incorrect st_nlink value being reported for any remaining
     * hardlinks to this inode.
     */
    cfg->entry_timeout = 0;
    cfg->attr_timeout = 0;
    cfg->negative_timeout = 0;
    
    /* Create a seperate GRPC server thread to accept failure injection. */
    std::thread th(&RunGrpcService::RunServer, mGrpcSever);
    th.detach();

    return NULL;
}

int FailureInjectorFs::fifs_getattr(
    const char *path,
    struct stat *stbuf,
    struct fuse_file_info *fi)
{
    (void) fi;
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"GETATTR", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = lstat(path, stbuf);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_access(
    const char *path,
    int mask)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"ACCESS", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = access(path, mask);
    if (res == -1)
    	return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_readlink(
    const char *path,
    char *buf,
    size_t size)
{
    int res;
    int injected_error = 0;

    if (CheckForInjectedError(path,"READLINK", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }
    
    res = readlink(path, buf, size - 1);
    if (res == -1)
        return -errno;
    
    buf[res] = '\0';
    return 0;
}

int FailureInjectorFs::fifs_readdir(
    const char *path,
    void *buf,
    fuse_fill_dir_t filler,
    off_t offset,
    struct fuse_file_info *fi,
    enum fuse_readdir_flags flags)
{
    DIR *dp;
    struct dirent *de;
    int injected_error = 0;
    
    (void) offset;
    (void) fi;
    (void) flags;

    if (CheckForInjectedError(path,"READDIR", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }
    
    dp = opendir(path);
    if (dp == NULL)
        return -errno;
    
    while ((de = readdir(dp)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
        if (filler(buf, de->d_name, &st, 0, (fuse_fill_dir_flags)0))
            break;
    }
    
    closedir(dp);
    return 0;
}

int FailureInjectorFs::fifs_mknod(
    const char *path,
    mode_t mode,
    dev_t rdev)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"MKNOD", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = mknod_wrapper(AT_FDCWD, path, NULL, mode, rdev);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_mkdir(
    const char *path,
    mode_t mode)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"MKDIR", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = mkdir(path, mode);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_unlink(const char *path)
{
	int res;
    int injected_error = 0;

    if (CheckForInjectedError(path,"UNLINK", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

	res = unlink(path);
	if (res == -1)
        return -errno;

	return 0;
}

int FailureInjectorFs::fifs_rmdir(const char *path)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"RMDIR", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = rmdir(path);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_symlink(
    const char *from,
    const char *to)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(from, "SYMLINK", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = symlink(from, to);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_rename(
    const char *from,
    const char *to,
    unsigned int flags)
{
    int res;
    int injected_error = 0;

    if (CheckForInjectedError(from, "RENAME", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }
    
    if (flags)
        return -EINVAL;
    
    res = rename(from, to);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_link(
    const char *from,
    const char *to)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(from,"LINK", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = link(from, to);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_chmod(
    const char *path,
    mode_t mode,
    struct fuse_file_info *fi)
{
    (void) fi;
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"CHMOD", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = chmod(path, mode);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_chown(
    const char *path,
    uid_t uid,
    gid_t gid,
    struct fuse_file_info *fi)
{
    (void) fi;
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"CHOWN", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = lchown(path, uid, gid);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_truncate(
    const char *path,
    off_t size,
    struct fuse_file_info *fi)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"TRUNCATE", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    if (fi != NULL)
        res = ftruncate(fi->fh, size);
    else
        res = truncate(path, size);
    if (res == -1)
        return -errno;
    
    return 0;
}

#ifdef HAVE_UTIMENSAT
int FailureInjectorFs::fifs_utimens(
    const char *path,
    const struct timespec ts[2],
    struct fuse_file_info *fi)
{
    (void) fi;
    int res;
    int injected_error = 0;

    if (CheckForInjectedError(path,"UTIMENS", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    
    /* don't use utime/utimes since they follow symlinks */
    res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
    if (res == -1)
        return -errno;
    
    return 0;
}
#endif

int FailureInjectorFs::fifs_create(
    const char *path,
    mode_t mode,
    struct fuse_file_info *fi)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"CREATE", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = open(path, fi->flags, mode);
    if (res == -1)
        return -errno;
    
    fi->fh = res;
    return 0;
}

int FailureInjectorFs::fifs_open(
    const char *path,
    struct fuse_file_info *fi)
{
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"OPEN", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    res = open(path, fi->flags);
    if (res == -1)
        return -errno;
    
    fi->fh = res;
    return 0;
}

bool FailureInjectorFs::CheckForInjectedError(
    string path,
    string op,
    int *injected_error)
{
    char *mpath = strdup(path.c_str());
    if (mFailureInjector == NULL) {
        return false;
    }

    auto failures = mFailureInjector->GetFailures(path, op);
    while (failures == NULL) {
        // Check if failures are injected in any of the parent directories.
        char *dir = dirname(mpath);
        failures = mFailureInjector->GetFailures(string(dir), op);
        if ((failures == NULL) && (!strcmp(dir, ".") || !strcmp(dir, "/"))) {
            free(mpath);
            return false;
        }
        mpath = dir;
    }
    free(mpath);

    *injected_error = 0;
    useconds_t delay = 1;
    for (auto f : *failures) {
        switch(f.code) {
            case InjectedAction::DELAY:
                delay = f.delay * MICROSECONDS_IN_A_SECOND;
                break;
            case InjectedAction::FAIL:
                if (*injected_error != CORRUPT_DATA_ERROR_CODE) {
                    *injected_error = f.error_code;
                }
                break;
            case InjectedAction::CORRUPT:
                *injected_error = CORRUPT_DATA_ERROR_CODE;
                break;
            default:
                // Ignore for now.
                break;
        }
    }
    // First create the delay.
    usleep(delay);
    
    if (*injected_error) {
        return  true;
    }
    return false;
}

void FailureInjectorFs::FillCorruptData(
    char *buf,
    size_t size)
{
    // For now just fill with some pattern based on input.
    // TBD : Fill with random data pattern.
    for (size_t i = 0; i < size; ++ i) {
        buf[i] = buf[i] + 2;
    }
}

int FailureInjectorFs::fifs_read(
    const char *path,
    char *buf,
    size_t size,
    off_t offset,
    struct fuse_file_info *fi)
{
    int fd;
    int res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path, "READ", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        if (injected_error != CORRUPT_DATA_ERROR_CODE) {
            return -injected_error;
        }
    }

    if(fi == NULL)
        fd = open(path, O_RDONLY);
    else
        fd = fi->fh;
    
    if (fd == -1)
        return -errno;
    
    res = pread(fd, buf, size, offset);
    if (res == -1)
        res = -errno;
    
    if(fi == NULL)
        close(fd);
    if ((res != -1) && (injected_error == CORRUPT_DATA_ERROR_CODE)) {
        FillCorruptData(buf, size);
        return res;
    }
    return res;
}

int FailureInjectorFs::fifs_write(
    const char *path,
    const char *buf,
    size_t size,
    off_t offset,
    struct fuse_file_info *fi)
{
    int fd;
    int res;
    int injected_error = 0;
    (void) fi;
    
    if (CheckForInjectedError(path, "WRITE", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        if (injected_error != CORRUPT_DATA_ERROR_CODE) {
            return -injected_error;
        } else {
            FillCorruptData(const_cast<char *>(buf), size);
        }
    }

    if(fi == NULL)
        fd = open(path, O_WRONLY);
    else
        fd = fi->fh;
    
    if (fd == -1)
        return -errno;
    
    res = pwrite(fd, buf, size, offset);
    if (res == -1)
        res = -errno;
    
    if(fi == NULL)
        close(fd);
    return res;
}

int FailureInjectorFs::fifs_statfs(
    const char *path,
    struct statvfs *stbuf)
{
    int res;
    int injected_error = 0;

    if (CheckForInjectedError(path, "STATFS", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }
    
    res = statvfs(path, stbuf);
    if (res == -1)
        return -errno;
    
    return 0;
}

int FailureInjectorFs::fifs_release(
    const char *path,
    struct fuse_file_info *fi)
{
    (void) path;
    close(fi->fh);
    return 0;
}

int FailureInjectorFs::fifs_fsync(
    const char *path,
    int isdatasync,
    struct fuse_file_info *fi)
{
    int injected_error = 0;

    if (CheckForInjectedError(path,"FSYNC", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    
    (void) path;
    (void) isdatasync;
    (void) fi;
    return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
int FailureInjectorFs::fifs_fallocate(
    const char *path,
    int mode,
    off_t offset,   
    off_t length,
    struct fuse_file_info *fi)
{
    int fd;
    int res;
    int injected_error = 0;
    
    (void) fi;

    if (CheckForInjectedError(path,"FALLOCATE", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }
    
    if (mode)
        return -EOPNOTSUPP;
    
    if(fi == NULL)
        fd = open(path, O_WRONLY);
    else
        fd = fi->fh;
    
    if (fd == -1)
        return -errno;
    
    res = -posix_fallocate(fd, offset, length);
    
    if(fi == NULL)
        close(fd);
    return res;
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
int FailureInjectorFs::fifs_setxattr(
    const char *path,
    const char *name,
    const char *value,
    size_t size,
    int flags)
{
    int injected_error = 0;

    if (CheckForInjectedError(path,"SETXATTR", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    int res = lsetxattr(path, name, value, size, flags);
    if (res == -1)
        return -errno;
    return 0;
}

int FailureInjectorFs::fifs_getxattr(
    const char *path,
    const char *name,
    char *value,
    size_t size)
{
    int injected_error = 0;

    if (CheckForInjectedError(path,"GETXATTR", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    int res = lgetxattr(path, name, value, size);
    if (res == -1)
        return -errno;
    return res;
}

int FailureInjectorFs::fifs_listxattr(
    const char *path,
    char *list,
    size_t size)
{
    int injected_error = 0;

    if (CheckForInjectedError(path,"LISTXATTR", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    int res = llistxattr(path, list, size);
    if (res == -1)
        return -errno;
    return res;
}

int FailureInjectorFs::fifs_removexattr(
    const char *path,
    const char *name)
{
    int injected_error = 0;

    if (CheckForInjectedError(path,"REMOVEXATTR", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    int res = lremovexattr(path, name);
    if (res == -1)
        return -errno;
    return 0;
}
#endif /* HAVE_SETXATTR */

off_t FailureInjectorFs::fifs_lseek(
    const char *path,
    off_t off,
    int whence,
    struct fuse_file_info *fi)
{
    int fd;
    off_t res;
    int injected_error = 0;
    
    if (CheckForInjectedError(path,"LSEEK", &injected_error)) {
        cout << "Returning Injected error " << injected_error << "\n";
        return -injected_error;
    }

    if (fi == NULL)
        fd = open(path, O_RDONLY);
    else
        fd = fi->fh;
    
    if (fd == -1)
        return -errno;
    
    res = lseek(fd, off, whence);
    if (res == -1)
        res = -errno;
    
    if (fi == NULL)
        close(fd);
    return res;
}

void FailureInjectorFs::load_operations() {
    mFuseOperationsVec.getattr	    = FailureInjectorFs::fifs_getattr;
    mFuseOperationsVec.readlink    = FailureInjectorFs::fifs_readlink;
    mFuseOperationsVec.mknod       = FailureInjectorFs::fifs_mknod;
    mFuseOperationsVec.mkdir       = FailureInjectorFs::fifs_mkdir;
    mFuseOperationsVec.unlink      = FailureInjectorFs::fifs_unlink;
    mFuseOperationsVec.rmdir       = FailureInjectorFs::fifs_rmdir;
    mFuseOperationsVec.symlink     = FailureInjectorFs::fifs_symlink;
    mFuseOperationsVec.rename      = FailureInjectorFs::fifs_rename;
    mFuseOperationsVec.link        = FailureInjectorFs::fifs_link;
    mFuseOperationsVec.chmod       = FailureInjectorFs::fifs_chmod;
    mFuseOperationsVec.chown       = FailureInjectorFs::fifs_chown;
    mFuseOperationsVec.truncate    = FailureInjectorFs::fifs_truncate;
    mFuseOperationsVec.open        = FailureInjectorFs::fifs_open;
    mFuseOperationsVec.read        = FailureInjectorFs::fifs_read;
    mFuseOperationsVec.write       = FailureInjectorFs::fifs_write;
    mFuseOperationsVec.statfs      = FailureInjectorFs::fifs_statfs;
    mFuseOperationsVec.flush       = NULL;
    mFuseOperationsVec.release     = FailureInjectorFs::fifs_release;
    mFuseOperationsVec.fsync       = FailureInjectorFs::fifs_fsync;

#ifdef HAVE_SETXATTR
    mFuseOperationsVec.setxattr    = FailureInjectorFs::fifs_setxattr;
    mFuseOperationsVec.getxattr    = FailureInjectorFs::fifs_getxattr;
    mFuseOperationsVec.listxattr   = FailureInjectorFs::fifs_listxattr;
    mFuseOperationsVec.removexattr = FailureInjectorFs::fifs_removexattr;
#else
    mFuseOperationsVec.setxattr    = NULL;
    mFuseOperationsVec.getxattr    = NULL;
    mFuseOperationsVec.listxattr   = NULL;
    mFuseOperationsVec.removexattr = NULL;
#endif

    mFuseOperationsVec.opendir     = NULL;
    mFuseOperationsVec.readdir     = FailureInjectorFs::fifs_readdir;
    mFuseOperationsVec.releasedir  = NULL;
    mFuseOperationsVec.fsyncdir    = NULL;
    mFuseOperationsVec.init        = FailureInjectorFs::fifs_init;
    mFuseOperationsVec.destroy     = NULL;
    mFuseOperationsVec.access      = FailureInjectorFs::fifs_access;
    mFuseOperationsVec.create      = FailureInjectorFs::fifs_create;
    mFuseOperationsVec.lock        = NULL;

#ifdef HAVE_UTIMENSAT
    mFuseOperationsVec.utimens     = FailureInjectorFs::fifs_utimens;
#else
    mFuseOperationsVec.utimens     = NULL;
#endif

    mFuseOperationsVec.bmap        = NULL;
    mFuseOperationsVec.ioctl       = NULL;
    mFuseOperationsVec.poll        = NULL;
    mFuseOperationsVec.write_buf   = NULL;
    mFuseOperationsVec.read_buf    = NULL;
    mFuseOperationsVec.flock       = NULL;
    
#ifdef HAVE_POSIX_FALLOCATE
    mFuseOperationsVec.fallocate   = FailureInjectorFs::fifs_fallocate;
#else
    mFuseOperationsVec.fallocate   = NULL;
#endif

    mFuseOperationsVec.copy_file_range = NULL;
}

const struct fuse_operations *FailureInjectorFs::getOperations() {
    return &mFuseOperationsVec;
}

int main(int argc, char *argv[])
{
    FailureInjector *injector = new FailureInjector();
	FailureInjectorFs noise_fs(injector);
     
	noise_fs.load_operations();
	umask(0);
	fuse_main(argc, argv, noise_fs.getOperations(), NULL);
}
