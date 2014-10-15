package org.apache.cassandra.cache;

import org.apache.cassandra.db.ClockAndCount;

public interface ICounterCacheSaver extends ICacheSaver<CounterCacheKey, ClockAndCount> {}
