package puregero.multipaper.server.replication;

import puregero.multipaper.mastermessagingprotocol.MessageBootstrap;
import puregero.multipaper.server.replication.messages.LeaderBoundMessage;
import puregero.multipaper.server.replication.messages.LeaderBoundProtocol;
import puregero.multipaper.server.replication.messages.PeerBoundMessage;
import puregero.multipaper.server.replication.messages.PeerBoundProtocol;

/**
 * Listens for incoming connections from peer masters.
 * Each connection is handled by a {@link PeerConnection} instance.
 */
public class PeerServer extends MessageBootstrap<LeaderBoundMessage, PeerBoundMessage> {

    public PeerServer(int port) {
        super(new LeaderBoundProtocol(), new PeerBoundProtocol(),
              channel -> channel.pipeline().addLast(new PeerConnection(channel)));
        listenOn(port).addListener(f -> {
            if (f.cause() != null) {
                System.err.println("[Replication] Failed to start peer server on port " + port + ": " + f.cause().getMessage());
            } else {
                System.out.println("[Replication] Peer server listening on port " + port);
            }
        });
    }
}
