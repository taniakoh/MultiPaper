package puregero.multipaper.server.replication;

import puregero.multipaper.mastermessagingprotocol.messages.masterbound.MasterBoundMessage;
import puregero.multipaper.mastermessagingprotocol.messages.serverbound.ServerBoundMessage;
import puregero.multipaper.server.ServerConnection;

/**
 * A no-op ServerConnection used when applying replicated writes on a standby.
 * All send methods are suppressed since standbys do not reply to replicated messages.
 */
public class NullServerConnection extends ServerConnection {

    private static final NullServerConnection INSTANCE = new NullServerConnection();

    private NullServerConnection() {
        super(null);
    }

    public static NullServerConnection get() {
        return INSTANCE;
    }

    @Override
    public void send(ServerBoundMessage message) {
        // No-op: standbys do not reply to replicated writes
    }

    @Override
    public void sendReply(ServerBoundMessage message, MasterBoundMessage inReplyTo) {
        // No-op
    }
}
