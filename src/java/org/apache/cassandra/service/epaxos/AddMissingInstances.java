package org.apache.cassandra.service.epaxos;


import java.util.Collection;

public class AddMissingInstances implements Runnable
{

    private final EpaxosState state;
    private final Collection<Instance> instances;

    public AddMissingInstances(EpaxosState state, Collection<Instance> instances)
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
