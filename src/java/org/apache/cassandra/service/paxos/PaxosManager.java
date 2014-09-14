package org.apache.cassandra.service.paxos;

import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.KeyspaceManager;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.tracing.Tracing;

import java.util.concurrent.locks.Lock;

public class PaxosManager
{
    public static final PaxosManager instance = new PaxosManager(DatabaseDescriptor.instance);

    private final Striped<Lock> locks;

    public PaxosManager(DatabaseDescriptor databaseDescriptor)
    {
        locks = Striped.lazyWeakLock(databaseDescriptor.getConcurrentWriters() * 1024);
    }

    public PrepareResponse prepare(Commit toPrepare, Tracing tracing, SystemKeyspace systemKeyspace)
    {
        Lock lock = locks.get(toPrepare.key);
        lock.lock();
        try
        {
            PaxosState state = systemKeyspace.loadPaxosState(toPrepare.key, toPrepare.update.metadata());
            if (toPrepare.isAfter(state.promised))
            {
                tracing.trace("Promising ballot {}", toPrepare.ballot);
                systemKeyspace.savePaxosPromise(toPrepare);
                return new PrepareResponse(true, state.accepted, state.mostRecentCommit);
            }
            else
            {
                tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPrepare, state.promised);
                // return the currently promised ballot (not the last accepted one) so the coordinator can make sure it uses newer ballot next time (#5667)
                return new PrepareResponse(false, state.promised, state.mostRecentCommit);
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    public Boolean propose(Commit proposal, Tracing tracing, SystemKeyspace systemKeyspace)
    {
        Lock lock = locks.get(proposal.key);
        lock.lock();
        try
        {
            PaxosState state = systemKeyspace.loadPaxosState(proposal.key, proposal.update.metadata());
            if (proposal.hasBallot(state.promised.ballot) || proposal.isAfter(state.promised))
            {
                tracing.trace("Accepting proposal {}", proposal);
                systemKeyspace.savePaxosProposal(proposal);
                return true;
            }
            else
            {
                tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, state.promised);
                return false;
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    public void commit(Commit proposal, Tracing tracing, SystemKeyspace systemKeyspace, KeyspaceManager keyspaceManager)
    {
        // There is no guarantee we will see commits in the right order, because messages
        // can get delayed, so a proposal can be older than our current most recent ballot/commit.
        // Committing it is however always safe due to column timestamps, so always do it. However,
        // if our current in-progress ballot is strictly greater than the proposal one, we shouldn't
        // erase the in-progress update.
        tracing.trace("Committing proposal {}", proposal);
        Mutation mutation = proposal.makeMutation();
        keyspaceManager.open(mutation.getKeyspaceName()).apply(mutation, true);

        // We don't need to lock, we're just blindly updating
        systemKeyspace.savePaxosCommit(proposal);
    }
}
