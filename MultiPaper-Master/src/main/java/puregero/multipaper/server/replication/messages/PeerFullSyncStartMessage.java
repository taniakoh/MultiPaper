package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerFullSyncStartMessage extends PeerBoundMessage {
    public final int totalFiles;

    public PeerFullSyncStartMessage(int totalFiles) {
        this.totalFiles = totalFiles;
    }

    public PeerFullSyncStartMessage(ExtendedByteBuf buf) {
        totalFiles = buf.readInt();
    }

    @Override
    public void write(ExtendedByteBuf buf) {
        buf.writeInt(totalFiles);
    }

    @Override
    public void handle(PeerBoundMessageHandler handler) {
        handler.handle(this);
    }
}
