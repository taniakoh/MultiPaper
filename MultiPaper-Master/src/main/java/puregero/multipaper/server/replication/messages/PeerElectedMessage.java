package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

/**
 * Sent by the election winner (leader→standbys) to announce the new leader.
 * Travels via PeerConnection.broadcastToAll() as a PeerBoundMessage.
 */
public class PeerElectedMessage extends PeerBoundMessage {
    public final String leaderId;
    public final String leaderHost;
    public final int leaderPort;

    public PeerElectedMessage(String leaderId, String leaderHost, int leaderPort) {
        this.leaderId = leaderId;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
    }

    public PeerElectedMessage(ExtendedByteBuf buf) {
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
