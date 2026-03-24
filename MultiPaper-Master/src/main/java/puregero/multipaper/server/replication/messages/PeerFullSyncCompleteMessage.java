package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerFullSyncCompleteMessage extends PeerBoundMessage {
    public PeerFullSyncCompleteMessage() {}

    public PeerFullSyncCompleteMessage(ExtendedByteBuf buf) {}

    @Override
    public void write(ExtendedByteBuf buf) {}

    @Override
    public void handle(PeerBoundMessageHandler handler) {
        handler.handle(this);
    }
}
