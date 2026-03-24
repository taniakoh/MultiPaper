package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.messages.Protocol;

public class LeaderBoundProtocol extends Protocol<LeaderBoundMessage> {
    public LeaderBoundProtocol() {
        addMessage(PeerHelloMessage.class, PeerHelloMessage::new);
        addMessage(PeerRequestFullSyncMessage.class, PeerRequestFullSyncMessage::new);
        addMessage(PeerHeartbeatMessage.class, PeerHeartbeatMessage::new);
        addMessage(PeerElectionMessage.class, PeerElectionMessage::new);
    }
}
