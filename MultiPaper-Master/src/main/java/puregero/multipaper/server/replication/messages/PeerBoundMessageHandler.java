package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.messages.MessageHandler;

public abstract class PeerBoundMessageHandler extends MessageHandler<PeerBoundMessage> {

    @Override
    public PeerBoundMessage createDataStreamMessage(int streamId, byte[] data, int offset, int length) {
        throw new UnsupportedOperationException("Data streams not supported in peer protocol");
    }

    public abstract void handle(PeerWelcomeMessage message);
    public abstract void handle(PeerReplicateMessage message);
    public abstract void handle(PeerFullSyncStartMessage message);
    public abstract void handle(PeerFullSyncFileMessage message);
    public abstract void handle(PeerFullSyncCompleteMessage message);
    public abstract void handle(PeerHeartbeatAckMessage message);
    public abstract void handle(PeerElectedMessage message);     // no-op, kept for wire-ID stability
    // Paxos consensus messages
    public abstract void handle(PeerPrepareMessage message);
    public abstract void handle(PeerAcceptMessage message);
    public abstract void handle(PeerCommitMessage message);
    public abstract void handle(PeerLeaseExpiredMessage message);
}
