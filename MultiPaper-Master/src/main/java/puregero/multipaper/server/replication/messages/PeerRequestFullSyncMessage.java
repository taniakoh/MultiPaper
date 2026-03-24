package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerRequestFullSyncMessage extends LeaderBoundMessage {
    public final String peerId;

    public PeerRequestFullSyncMessage(String peerId) {
        this.peerId = peerId;
    }

    public PeerRequestFullSyncMessage(ExtendedByteBuf buf) {
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
