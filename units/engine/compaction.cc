#include "units/units.h"
#include "ndb/engine/compaction.h"

int Test(int argc, char* argv[]) {
    CompactionMetaCache cache(2);

    CompactionMeta tmp;
    NDB_ASSERT(!cache.Get("meta:1", &tmp));

    CompactionMeta meta;
    meta.deleted = true;
    meta.version = 1;
    cache.Put("meta:1", meta);

    NDB_ASSERT(cache.Get("meta:1", &tmp));
    NDB_ASSERT(tmp.deleted == true && tmp.version == 1);

    meta.deleted = false;
    meta.version = 2;
    cache.Put("meta:2", meta);
    meta.deleted = true;
    meta.version = 3;
    cache.Put("meta:3", meta);

    // NDB_ASSERT(!cache.Get("meta:1", &tmp));
    NDB_ASSERT(cache.Get("meta:2", &tmp));
    NDB_ASSERT(tmp.deleted == false && tmp.version == 2);
    NDB_ASSERT(cache.Get("meta:3", &tmp));
    NDB_ASSERT(tmp.deleted == true && tmp.version == 3);

    return EXIT_SUCCESS;
}
