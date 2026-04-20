package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.messages.Protocol;

public class PeerBoundProtocol extends Protocol<PeerBoundMessage> {
    public PeerBoundProtocol() {
        addMessage(PeerWelcomeMessage.class, PeerWelcomeMessage::new);           // 0
        addMessage(PeerReplicateMessage.class, PeerReplicateMessage::new);       // 1
        addMessage(PeerFullSyncStartMessage.class, PeerFullSyncStartMessage::new); // 2
        addMessage(PeerFullSyncFileMessage.class, PeerFullSyncFileMessage::new); // 3
        addMessage(PeerFullSyncCompleteMessage.class, PeerFullSyncCompleteMessage::new); // 4
        addMessage(PeerHeartbeatAckMessage.class, PeerHeartbeatAckMessage::new); // 5
        addMessage(PeerElectedMessage.class, PeerElectedMessage::new);           // 6 (no-op, kept for ID stability)
        // Paxos consensus messages
        addMessage(PeerPrepareMessage.class, PeerPrepareMessage::new);           // 7
        addMessage(PeerAcceptMessage.class, PeerAcceptMessage::new);             // 8
        addMessage(PeerCommitMessage.class, PeerCommitMessage::new);             // 9
        addMessage(PeerLeaseExpiredMessage.class, PeerLeaseExpiredMessage::new); // 10
    }
}
