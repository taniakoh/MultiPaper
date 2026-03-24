package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerHelloMessage extends LeaderBoundMessage {
    public final String peerId;

    public PeerHelloMessage(String peerId) {
        this.peerId = peerId;
    }

    public PeerHelloMessage(ExtendedByteBuf buf) {
        peerId = buf.readString();
    }

    @Override
    public void write(ExtendedByteBuf buf) {
        buf.writeString(peerId);
    }

    @Override
    public void handle(LeaderBoundMessageHandler handler) {
        handler.handle(this);
    }
}
