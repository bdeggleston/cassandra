package org.apache.cassandra.streaming.messages;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.service.epaxos.InstanceStreamReader;
import org.apache.cassandra.service.epaxos.InstanceStreamWriter;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;

public class EpaxosMessage extends StreamMessage
{
    public final UUID taskId;
    public final UUID cfId;
    public final Range<Token> range;

    public EpaxosMessage(UUID taskId, UUID cfId, Range<Token> range)
    {
        super(Type.EPAXOS);
        this.taskId = taskId;
        this.cfId = cfId;
        this.range = range;
    }

    public static Serializer<EpaxosMessage> serializer = new Serializer<EpaxosMessage>()
    {
        @Override
        public EpaxosMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInputStream input = new DataInputStream(Channels.newInputStream(in));
            EpaxosMessage message = new EpaxosMessage(UUIDSerializer.serializer.deserialize(input, version),
                                                      UUIDSerializer.serializer.deserialize(input, version),
                                                      new Range<>(Token.serializer.deserialize(input),
                                                                  Token.serializer.deserialize(input)));

            InstanceStreamReader reader = new InstanceStreamReader(message.cfId, message.range);
            reader.read(in);

            // TODO: on stream session complete, set the affected token states to normal
            return message;
        }

        @Override
        public void serialize(EpaxosMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.taskId, out, version);
            UUIDSerializer.serializer.serialize(message.cfId, out, version);
            Token.serializer.serialize(message.range.left, out);
            Token.serializer.serialize(message.range.right, out);

            InstanceStreamWriter writer = new InstanceStreamWriter(message.cfId, message.range, session.peer);
            writer.write(out.getChannel());

            session.epaxosTransferComplete(message.taskId);
        }
    };
}
