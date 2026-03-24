package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

/**
 * Chang-Roberts ring election token. Sent via PeerClient (LeaderBoundMessage)
 * to a peer's PeerServer, which forwards it around the ring.
 */
public class PeerElectionMessage extends LeaderBoundMessage {
    public final String candidateId;

    public PeerElectionMessage(String candidateId) {
        this.candidateId = candidateId;
    }

    public PeerElectionMessage(ExtendedByteBuf buf) {
        candidateId = buf.readString();
    }

    @Override
    public void write(ExtendedByteBuf buf) {
        buf.writeString(candidateId);
    }

    @Override
    public void handle(LeaderBoundMessageHandler handler) {
        handler.handle(this);
    }
}
