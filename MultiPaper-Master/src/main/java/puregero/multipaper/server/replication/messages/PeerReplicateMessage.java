package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerReplicateMessage extends PeerBoundMessage {
    public final byte[] rawBytes;
    public final long correlationId;

    public PeerReplicateMessage(byte[] rawBytes, long correlationId) {
        this.rawBytes = rawBytes;
        this.correlationId = correlationId;
    }

    public PeerReplicateMessage(ExtendedByteBuf buf) {
        correlationId = buf.readLong();
        int length = buf.readVarInt();
        rawBytes = new byte[length];
        buf.readBytes(rawBytes);
    }

    @Override
    public void write(ExtendedByteBuf buf) {
        buf.writeLong(correlationId);
        buf.writeVarInt(rawBytes.length);
        buf.writeBytes(rawBytes);
    }

    @Override
    public void handle(PeerBoundMessageHandler handler) {
        handler.handle(this);
    }
}
