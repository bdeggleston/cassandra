package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Messenger
{
    private final int VERSION = 0;
    private final AtomicInteger nextMsgNumber = new AtomicInteger(0);

    private final Map<InetAddress, Node> nodes = Maps.newConcurrentMap();
    private final Map<InetAddress, Map<MessagingService.Verb, IVerbHandler>> verbHandlers = Maps.newConcurrentMap();
    private final Map<Integer, IAsyncCallback> callbackMap = Maps.newConcurrentMap();

    public void registerNode(Node node)
    {
        nodes.put(node.getEndpoint(), node);

        Map<MessagingService.Verb, IVerbHandler> handlers = Maps.newEnumMap(MessagingService.Verb.class);
        handlers.put(MessagingService.Verb.PREACCEPT_REQUEST, node.getPreacceptVerbHandler());
        handlers.put(MessagingService.Verb.ACCEPT_REQUEST, node.getAcceptVerbHandler());
        handlers.put(MessagingService.Verb.COMMIT_REQUEST, node.getCommitVerbHandler());

        verbHandlers.put(node.getEndpoint(), handlers);
    }

    public List<InetAddress> getEndpoints()
    {
        return Lists.newArrayList(nodes.keySet());
    }

    /**
     * create a new message from the sender's fake ip address
     */
    private <T> MessageOut<T> wrapMessage(MessageOut<T> msg, InetAddress from)
    {
        return new MessageOut<T>(from, msg.verb, msg.payload, msg.serializer, msg.parameters);
    }

    public <T> void sendReply(MessageOut<T> msg, int id, InetAddress from, InetAddress to)
    {
        if (nodes.get(from).getState() != Node.State.UP)
            return;

        MessageIn<T> messageIn;
        DataOutputBuffer out = new DataOutputBuffer();

        // make new message to capture fake endpoint
        MessageOut<T> messageOut = wrapMessage(msg, from);
        try
        {
            messageOut.serialize(out, 0);
            messageIn = MessageIn.read(ByteStreams.newDataInput(out.getData()), VERSION, id);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        @SuppressWarnings("unchecked")
        IAsyncCallback<T> cb = callbackMap.get(id);
        callbackMap.remove(id);

        cb.response(messageIn);
    }

    @SuppressWarnings("unchecked")
    public <T> int sendRR(MessageOut<T> msg, InetAddress from, InetAddress to, IAsyncCallback cb)
    {
        int msgId = nextMsgNumber.getAndIncrement();
        if (nodes.get(to).getState() == Node.State.DOWN)
            return msgId;

        if (cb != null)
            callbackMap.put(msgId, cb);

        MessageIn<T> messageIn;
        DataOutputBuffer out = new DataOutputBuffer();
        MessageOut<T> messageOut = wrapMessage(msg, from);
        try
        {
            messageOut.serialize(out, VERSION);
            messageIn = MessageIn.read(ByteStreams.newDataInput(out.getData()), VERSION, msgId);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        verbHandlers.get(to).get(messageIn.verb).doVerb(messageIn, msgId);

        return msgId;
    }

    @SuppressWarnings("unchecked")
    public <T> void sendOneWay(MessageOut<T> msg, InetAddress from, InetAddress to)
    {
        int msgId = nextMsgNumber.getAndIncrement();
        if (nodes.get(to).getState() == Node.State.DOWN)
            return;

        MessageIn<T> messageIn;
        DataOutputBuffer out = new DataOutputBuffer();
        MessageOut<T> messageOut = wrapMessage(msg, from);
        try
        {
            messageOut.serialize(out, VERSION);
            messageIn = MessageIn.read(ByteStreams.newDataInput(out.getData()), VERSION, msgId);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        verbHandlers.get(to).get(messageIn.verb).doVerb(messageIn, msgId);
    }
}
