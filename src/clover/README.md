# Clover

## Dependencies

### Install each dependency manually

- [boost](https://www.boost.org/users/history/version_1_76_0.html)
  - Needs only `system` and `coroutine`, but Folly ([see below](#folly-dependencies)) requires additional libraries.
- [PAPI](http://icl.utk.edu/projects/papi/downloads/papi-6.0.0.tar.gz)
- [libhugetlbfs](https://github.com/libhugetlbfs/libhugetlbfs/releases/tag/2.23)
- [memcached](https://github.com/memcached/memcached/releases/tag/1.6.9)
- [libmemcached](https://launchpad.net/libmemcached/1.0/1.0.18/+download/libmemcached-1.0.18.tar.gz)
- [numactl](https://github.com/numactl/numactl)
- [concurrentqueue](https://github.com/jim90247/concurrentqueue)
  - Use the version I forked from upstream repository
- Folly: [see below](#folly)

### For CentOS/Fedora

```bash
yum install memcached memcached-devel libmemcached libmemcached-devel numactl numactl-devel mbedtls mbedtls-devel glib2 glib2-devel
yum install glibc-static bzip2 yum-utils python-devel bzip2-devel papi-devel
yum install libhugetlbfs-utils libhugetlbfs
# boost
wget https://sourceforge.net/projects/boost/files/boost/1.60.0/boost_1_60_0.tar.gz/download
./bootstrap.sh --with-libraries=system,coroutine
./b2 -aq install
```

## Hugepage

```bash
hugeadm --pool-pages-min 2MB:8192
mkdir -p /mnt/hugetlbfs
mount -t hugetlbfs none /mnt/hugetlbfs
numactl --physcpubind=0 --localalloc LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes
```

## Folly

Tested version: [v2022.01.24](https://github.com/facebook/folly/releases/tag/v2022.01.24.00)

### Folly dependencies

- [gflags](https://github.com/gflags/gflags/releases/tag/v2.2.2)
- [glog](https://github.com/google/glog/releases/tag/v0.5.0)
- [boost](https://www.boost.org/users/history/version_1_76_0.html)
  - Compile with all components
- libssl-dev: [apt package](https://packages.ubuntu.com/focal/libssl-dev)
  - BlueField-2 already have this library installed
- [fmt](https://github.com/fmtlib/fmt/releases/tag/8.1.1)
- [libevent](https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz)
- [double-conversion](https://github.com/google/double-conversion/releases/tag/v3.2.0)
  - Install this library in `/usr/*`, otherwise CMake might not be able to find it.

Dependencies can also be installed from system repository using the following command:

```bash
# list packages without installing them
python3 build/fbcode_builder/getdeps.py install-system-deps --dry-run --recursive

# install packages from system repository
python3 build/fbcode_builder/getdeps.py install-system-deps --recursive
```

### Build with CMake

```bash
# navigate to repository root
cd build
mkdir build
cd build

cmake ../.. -DCMAKE_INSTALL_PREFIX="$HOME/.local"
cmake --build . -j8
cmake --install .
```
