CXXFLAGS := -std=c++11 -g2 -O2 -Wall -Wno-unused-variable -Wno-deprecated-declarations
CPPFLAGS := -I.. -I../deps/include
LDFLAGS := -lpthread
LDLIBS := ../deps/lib/libprotobuf.a ../deps/lib/librocksdb.a ../deps/lib/libcurl.a
LDLIBS += ../deps/lib/libz.a ../deps/lib/libbz2.a ../deps/lib/liblz4.a ../deps/lib/libsnappy.a

TARGETS := ndb libndb.a

.PHONY: release
release: LDLIBS += ../deps/lib/libjemalloc.a
release: $(TARGETS)

.PHONY: develop
develop: CXXFLAGS += -Werror
develop: $(TARGETS)

.PHONY: coverage
coverage: LDFLAGS += -lgcov
coverage: CPPFLAGS += -fprofile-arcs -ftest-coverage -fno-inline -fno-elide-constructors
coverage: $(TARGETS)

HEADERS += $(shell find . -name "*.h")
SOURCES += $(shell find . -name "*.cc" ! -name "main.cc")
OBJECTS := $(SOURCES:.cc=.o) engine/engine.pb.o
PBFILES := engine/engine.pb.h engine/engine.pb.cc

ndb: main.o libndb.a
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LDLIBS) $(LDFLAGS)

main.o: libndb.a

libndb.a: $(PBFILES) $(HEADERS) $(OBJECTS)
	$(AR) cr $@ $(OBJECTS)

$(PBFILES) : engine/engine.proto
	../deps/bin/protoc --proto_path=engine --cpp_out=engine $^

.PHONY: clean
clean:
	rm -rf *.o $(OBJECTS) $(TARGETS) $(PBFILES) nicedb nicedb.log
