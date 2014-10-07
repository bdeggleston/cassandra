package org.apache.cassandra.service;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A query/mutation to be performed in a guaranteed order
 */
public interface SerializedRequest
{
    /**
     * The filter to use to fetch the value to compare for the CAS.
     */
    public IDiskAtomFilter readFilter();

    /**
     * Returns whether the provided CF, that represents the values fetched using the
     * readFilter(), match the CAS conditions this object stands for.
     */
    public boolean appliesTo(ColumnFamily current) throws InvalidRequestException;

    /**
     * The updates to perform of a CAS success. The values fetched using the readFilter()
     * are passed as argument.
     */
    public ColumnFamily makeUpdates(ColumnFamily current) throws InvalidRequestException;
}
