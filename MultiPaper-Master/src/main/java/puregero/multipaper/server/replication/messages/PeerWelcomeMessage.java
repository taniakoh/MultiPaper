package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerWelcomeMessage extends PeerBoundMessage {
    public final String leaderId;
    public final String leaderHost;
    public final int leaderPort;

    public PeerWelcomeMessage(String leaderId, String leaderHost, int leaderPort) {
        this.leaderId = leaderId;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
    }

    public PeerWelcomeMessage(ExtendedByteBuf buf) {
        leaderId = buf.readString();
        leaderHost = buf.readString();
        leaderPort = buf.readInt();
    }

    @Override
    public void write(ExtendedByteBuf buf) {
        buf.writeString(leaderId);
        buf.writeString(leaderHost);
        buf.writeInt(leaderPort);
    }

    @Override
    public void handle(PeerBoundMessageHandler handler) {
        handler.handle(this);
    }
}
