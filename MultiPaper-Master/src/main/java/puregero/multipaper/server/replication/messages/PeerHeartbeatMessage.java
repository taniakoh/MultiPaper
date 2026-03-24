package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerHeartbeatMessage extends LeaderBoundMessage {
    public PeerHeartbeatMessage() {}

    public PeerHeartbeatMessage(ExtendedByteBuf buf) {}

    @Override
    public void write(ExtendedByteBuf buf) {}

    @Override
    public void handle(LeaderBoundMessageHandler handler) {
        handler.handle(this);
    }
}
