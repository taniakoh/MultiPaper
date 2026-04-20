package puregero.multipaper.server.replication.messages;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class PeerHelloMessage extends LeaderBoundMessage {

    public static final int PROTOCOL_VERSION = 2;

    public final String peerId;
    public final int protocolVersion;

    public PeerHelloMessage(String peerId) {
        this.peerId = peerId;
        this.protocolVersion = PROTOCOL_VERSION;
    }

    public PeerHelloMessage(ExtendedByteBuf buf) {
        peerId = buf.readString();
        protocolVersion = buf.readVarInt();
    }

    @Override
    public void write(ExtendedByteBuf buf) {
        buf.writeString(peerId);
        buf.writeVarInt(protocolVersion);
    }

    @Override
    public void handle(LeaderBoundMessageHandler handler) {
        handler.handle(this);
    }
}
