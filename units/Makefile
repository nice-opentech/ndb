CXXFLAGS := -std=c++11 -g2 -O2 -Wall -Wno-unused-variable
CPPFLAGS := -I.. -I../deps/include
LDLIBS := ../ndb/libndb.a
LDLIBS += ../deps/lib/libprotobuf.a ../deps/lib/librocksdb.a ../deps/lib/libcurl.a
LDLIBS += ../deps/lib/libz.a ../deps/lib/libbz2.a ../deps/lib/liblz4.a ../deps/lib/libsnappy.a
LDLIBS += -lpthread -lgcov

SOURCES := $(shell find . -name "*.cc")
OBJECTS := $(SOURCES:.cc=.o)
TARGETS := $(OBJECTS:.o=)

all: $(TARGETS)

.PHONY: clean
clean:
	rm -rf $(OBJECTS) $(TARGETS)
