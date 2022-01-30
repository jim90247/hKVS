all: init

CXX := g++
LD := ${CXX}

# Flags to enable link-time optimization and GDB
#LTO := -flto
ENABLE_DGB :=

HOTS_HOME := .
MICA_HOME := ../../mica2/src
BOOST_HOME := $${HOME}/.local/boost_1_76_0

PKG_CONFIG_LIBS := libfolly fmt gflags libglog libmemcached papi

# add concurrentqueue include path
CONCURRENTQUEUE_INC := $${HOME}/.local/include/concurrentqueue

INC	:= -I ${HOTS_HOME} -I ${MICA_HOME} -I ${BOOST_HOME}/include -I ${CONCURRENTQUEUE_INC}

ifeq ($(PROFILE),)
	DEBUG :=
else
	DEBUG := -fno-omit-frame-pointer -fno-inline
endif

CPPFLAGS := ${ENABLE_DGB} ${LTO} -O2 ${DEBUG} -std=c++17 ${INC} -Wall -g \
	-Wno-unused-result \
	-Wno-unused-value \
	-Wno-unused-function \
	-Wno-sign-compare \
	-DBOOST_COROUTINES_NO_DEPRECATION_WARNING \
	$(shell pkg-config --cflags $(PKG_CONFIG_LIBS))

LDFLAGS := ${ENABLE_DGB} ${LTO} -O2 -libverbs -lrt -ldl -pthread -lnuma -g \
	-lboost_system -lboost_coroutine -lboost_thread -L ${BOOST_HOME}/lib -rdynamic \
	$(shell pkg-config --libs $(PKG_CONFIG_LIBS))

src := ibsetup.o memcached.o \
	mitsume_macro.o mitsume_util.o \
	mitsume_struct.o \
	init.o \
	memory.o client.o server.o \
	mitsume_con_alloc.o mitsume_con_thread.o \
	mitsume_clt_tool.o mitsume_clt_thread.o mitsume_clt_test.o \
	mitsume_tool.o mitsume_tool_cache.o mitsume_tool_gc.o \
	mitsume_stat.o \
	mitsume_benchmark.o \
	op_counter.o

# Handle MICA differently. MICA relies on DNDEBUG to disable datapath asserts.
#MICA_DEBUG := -DNDEBUG
#mica_src := ${MICA_HOME}/mica/util/config.o \
	${MICA_HOME}/mica/util/cityhash/city_mod.o
#${mica_src} : CPPFLAGS += ${MICA_DEBUG}

init: ${src}
	${LD} -o $@ $^ ${LDFLAGS}

PHONY: clean
clean:
	rm -f *.o init ${src}

