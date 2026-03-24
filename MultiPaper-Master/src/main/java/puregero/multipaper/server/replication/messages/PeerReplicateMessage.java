package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerReplicateMessage extends PeerBoundMessage {
    public final byte[] rawBytes;

    public PeerReplicateMessage(byte[] rawBytes) {
        this.rawBytes = rawBytes;
    }

    public PeerReplicateMessage(ExtendedByteBuf buf) {
        int length = buf.readVarInt();
        rawBytes = new byte[length];
        buf.readBytes(rawBytes);
    }

    @Override
    public void write(ExtendedByteBuf buf) {
        buf.writeVarInt(rawBytes.length);
        buf.writeBytes(rawBytes);
    }

    @Override
    public void handle(PeerBoundMessageHandler handler) {
        handler.handle(this);
    }
}
