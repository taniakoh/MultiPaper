package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.messages.Protocol;

public class LeaderBoundProtocol extends Protocol<LeaderBoundMessage> {
    public LeaderBoundProtocol() {
        addMessage(PeerHelloMessage.class, PeerHelloMessage::new);           // 0
        addMessage(PeerRequestFullSyncMessage.class, PeerRequestFullSyncMessage::new); // 1
        addMessage(PeerHeartbeatMessage.class, PeerHeartbeatMessage::new);   // 2
        addMessage(PeerElectionMessage.class, PeerElectionMessage::new);     // 3 (no-op, kept for ID stability)
        addMessage(PeerWriteAckMessage.class, PeerWriteAckMessage::new);     // 4
        // Paxos consensus messages
        addMessage(PeerPromiseMessage.class, PeerPromiseMessage::new);       // 5
        addMessage(PeerAcceptAckMessage.class, PeerAcceptAckMessage::new);   // 6
    }
}
