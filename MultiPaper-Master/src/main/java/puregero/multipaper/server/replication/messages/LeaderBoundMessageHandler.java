package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.messages.MessageHandler;

public abstract class LeaderBoundMessageHandler extends MessageHandler<LeaderBoundMessage> {

    @Override
    public LeaderBoundMessage createDataStreamMessage(int streamId, byte[] data, int offset, int length) {
        throw new UnsupportedOperationException("Data streams not supported in peer protocol");
    }

    public abstract void handle(PeerHelloMessage message);
    public abstract void handle(PeerRequestFullSyncMessage message);
    public abstract void handle(PeerHeartbeatMessage message);
    public abstract void handle(PeerElectionMessage message);
}
