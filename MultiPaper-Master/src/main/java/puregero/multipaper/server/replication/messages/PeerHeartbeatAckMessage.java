package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerHeartbeatAckMessage extends PeerBoundMessage {
    public PeerHeartbeatAckMessage() {}

    public PeerHeartbeatAckMessage(ExtendedByteBuf buf) {}

    @Override
    public void write(ExtendedByteBuf buf) {}

    @Override
    public void handle(PeerBoundMessageHandler handler) {
        handler.handle(this);
    }
}
