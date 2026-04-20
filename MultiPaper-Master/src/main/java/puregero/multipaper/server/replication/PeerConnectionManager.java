package puregero.multipaper.server.replication;

import puregero.multipaper.server.replication.messages.LeaderBoundMessage;
import puregero.multipaper.server.replication.messages.PeerBoundMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages all outbound {@link PeerClient} connections (one per configured peer).
 */
public class PeerConnectionManager {

    private static final List<PeerClient> clients = new CopyOnWriteArrayList<>();
    private static final Map<String, PeerClient> clientMap = new ConcurrentHashMap<>();

    public static void connectToAllPeers(List<ReplicationConfig.PeerEntry> peers) {
        for (ReplicationConfig.PeerEntry peer : peers) {
            PeerClient client = new PeerClient(peer);
            clients.add(client);
            clientMap.put(peer.id, client);
        }
    }

    /**
     * Send a PeerBoundMessage to a specific peer. Prefers the inbound PeerConnection channel
     * (peer connected inward to us); falls back to our outbound PeerClient channel.
     */
    public static void sendToPeer(String peerId, PeerBoundMessage message) {
        PeerConnection inbound = PeerConnection.getPeers().get(peerId);
        if (inbound != null) {
            inbound.send(message);
            return;
        }
        PeerClient outbound = clientMap.get(peerId);
        if (outbound != null && outbound.isConnected()) {
            // Can't send PeerBoundMessage on outbound channel — wrong direction.
            // The PeerClient sends LeaderBoundMessages to the peer's PeerServer.
            // If the inbound channel isn't established yet, we can't reach the peer this way.
            System.err.println("[Replication] No inbound channel to peer " + peerId + " for PeerBoundMessage");
        }
    }

    /**
     * Send a LeaderBoundMessage to a specific peer via the outbound PeerClient connection.
     */
    public static void sendLeaderBoundToPeer(String peerId, LeaderBoundMessage message) {
        PeerClient client = clientMap.get(peerId);
        if (client != null && client.isConnected()) {
            client.sendLeaderBound(message);
        }
    }

    /**
     * Broadcast a PeerBoundMessage to all peers that have an inbound connection to us.
     */
    public static void broadcastToAll(PeerBoundMessage message) {
        PeerConnection.broadcastToAll(message);
    }

    public static List<PeerClient> getClients() {
        return clients;
    }

    public static PeerClient getClient(String peerId) {
        return clientMap.get(peerId);
    }
}
