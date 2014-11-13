package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;

public class PrepareVerbHandler implements IVerbHandler<PrepareRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareVerbHandler.class);

    private final EpaxosState state;

    public PrepareVerbHandler(EpaxosState state)
    {
        this.state = state;
    }

    @Override
    public void doVerb(MessageIn<PrepareRequest> message, int id)
    {
        logger.debug("Prepare request received from {} for {}", message.from, message.payload.iid);
        ReadWriteLock lock = state.getInstanceLock(message.payload.iid);
        lock.writeLock().lock();
        try
        {
            Instance instance = state.loadInstance(message.payload.iid);

            // we can't participate in the prepare phase if our
            // local copy of the instance is a placeholder
            if (instance != null && instance.isPlaceholder())
                instance = null;

            if (instance != null)
            {
                try
                {
                    instance.checkBallot(message.payload.ballot);
                    state.saveInstance(instance);
                }
                catch (BallotException e)
                {
                    // don't die if the message has an old ballot value, just don't
                    // update the instance. This instance will still be useful to the requestor
                    logger.debug("Prepare request from {} for {} ballot failure. {} >= {}",
                                 message.from,
                                 message.payload.iid,
                                 instance.getBallot(),
                                 message.payload.ballot);
                }
            }

            MessageOut<Instance> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                          instance,
                                                          Instance.serializer);
            state.sendReply(reply, id, message.from);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
}
