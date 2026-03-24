package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerFullSyncFileMessage extends PeerBoundMessage {
    public final String relativePath;
    public final long lastModified;
    public final byte[] data;

    public PeerFullSyncFileMessage(String relativePath, long lastModified, byte[] data) {
        this.relativePath = relativePath;
        this.lastModified = lastModified;
        this.data = data;
    }

    public PeerFullSyncFileMessage(ExtendedByteBuf buf) {
        relativePath = buf.readString();
        lastModified = buf.readLong();
        int length = buf.readVarInt();
        data = new byte[length];
        buf.readBytes(data);
    }

    @Override
    public void write(ExtendedByteBuf buf) {
        buf.writeString(relativePath);
        buf.writeLong(lastModified);
        buf.writeVarInt(data.length);
        buf.writeBytes(data);
    }

    @Override
    public void handle(PeerBoundMessageHandler handler) {
        handler.handle(this);
    }
}
