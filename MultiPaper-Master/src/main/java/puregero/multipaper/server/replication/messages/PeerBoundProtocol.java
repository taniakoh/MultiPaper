package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.messages.Protocol;

public class PeerBoundProtocol extends Protocol<PeerBoundMessage> {
    public PeerBoundProtocol() {
        addMessage(PeerWelcomeMessage.class, PeerWelcomeMessage::new);
        addMessage(PeerReplicateMessage.class, PeerReplicateMessage::new);
        addMessage(PeerFullSyncStartMessage.class, PeerFullSyncStartMessage::new);
        addMessage(PeerFullSyncFileMessage.class, PeerFullSyncFileMessage::new);
        addMessage(PeerFullSyncCompleteMessage.class, PeerFullSyncCompleteMessage::new);
        addMessage(PeerHeartbeatAckMessage.class, PeerHeartbeatAckMessage::new);
        addMessage(PeerElectedMessage.class, PeerElectedMessage::new);
    }
}
