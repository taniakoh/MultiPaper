package puregero.multipaper.mastermessagingprotocol.messages.serverbound;

import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;

public class RedirectToLeaderMessage extends ServerBoundMessage {

    public final String leaderHost;
    public final int leaderPort;

    public RedirectToLeaderMessage(String leaderHost, int leaderPort) {
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
    }

    public RedirectToLeaderMessage(ExtendedByteBuf byteBuf) {
        leaderHost = byteBuf.readString();
        leaderPort = byteBuf.readInt();
    }

    @Override
    public void write(ExtendedByteBuf byteBuf) {
        byteBuf.writeString(leaderHost);
        byteBuf.writeInt(leaderPort);
    }

    @Override
    public void handle(ServerBoundMessageHandler handler) {
        handler.handle(this);
    }
}
