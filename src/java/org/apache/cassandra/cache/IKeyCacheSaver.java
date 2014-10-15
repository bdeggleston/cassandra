package org.apache.cassandra.cache;

import org.apache.cassandra.db.RowIndexEntry;

public interface IKeyCacheSaver extends ICacheSaver<KeyCacheKey, RowIndexEntry> {}
