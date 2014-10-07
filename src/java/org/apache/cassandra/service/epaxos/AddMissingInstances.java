package org.apache.cassandra.service.epaxos;

import java.util.List;

public class AddMissingInstances implements Runnable
{

    private final EpaxosState state;
    private final List<Instance> instances;

    public AddMissingInstances(EpaxosState state, List<Instance> instances)
    {
        this.state = state;
        this.instances = instances;
    }

    @Override
    public void run()
    {
        for (Instance instance: instances)
        {
            state.addMissingInstance(instance);
        }
    }
}
