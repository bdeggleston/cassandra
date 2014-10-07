package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;

public class TryPrepareCallback implements IAsyncCallback<TryPrepareResponse>
{
    @Override
    public void response(MessageIn<TryPrepareResponse> msg)
    {

    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
