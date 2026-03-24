package puregero.multipaper.server.replication;

import puregero.multipaper.server.MultiPaperServer;
import puregero.multipaper.server.replication.messages.PeerElectionMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages all outbound {@link PeerClient} connections (one per configured peer).
 * Also provides ring-election routing utilities.
 */
public class PeerConnectionManager {

    private static final List<PeerClient> clients = new CopyOnWriteArrayList<>();
    private static final Map<String, PeerClient> clientMap = new ConcurrentHashMap<>();

    private static volatile boolean mcServerStarted = false;

    public static void connectToAllPeers(List<ReplicationConfig.PeerEntry> peers) {
        for (ReplicationConfig.PeerEntry peer : peers) {
            PeerClient client = new PeerClient(peer);
            clients.add(client);
            clientMap.put(peer.id, client);
        }
    }

    /**
     * Called when this node wins election.  Starts the Minecraft server listener
     * if it hasn't been started yet.
     */
    public static synchronized void onBecomeLeader() {
        if (mcServerStarted) return;
        mcServerStarted = true;
        ReplicationConfig config = ReplicationConfig.get();
        String host = (config.myHost.equals("0.0.0.0") || config.myHost.isEmpty()) ? null : config.myHost;
        System.out.println("[Replication] Promoting to leader — starting Minecraft listener on "
                + (host == null ? "0.0.0.0" : host) + ":" + config.myPort);
        new MultiPaperServer(host, config.myPort);
    }

    /**
     * Send election message to the next available peer in lexicographic ring order.
     * Returns true if sent successfully, false if no peers are reachable.
     */
    public static boolean sendElectionToNextInRing(PeerElectionMessage message) {
        List<String> ring = getRingOrder();
        String myId = ReplicationConfig.get().myId;
        int myIndex = ring.indexOf(myId);

        for (int i = 1; i < ring.size(); i++) {
            String nextId = ring.get((myIndex + i) % ring.size());
            if (nextId.equals(myId)) continue;
            PeerClient client = clientMap.get(nextId);
            if (client != null && client.isConnected()) {
                client.sendLeaderBound(message);
                return true;
            }
        }
        return false;
    }

    /**
     * Returns all peer IDs (including this node) sorted lexicographically — the ring order.
     */
    public static List<String> getRingOrder() {
        List<String> ring = new ArrayList<>();
        ring.add(ReplicationConfig.get().myId);
        for (ReplicationConfig.PeerEntry peer : ReplicationConfig.get().peers) {
            ring.add(peer.id);
        }
        Collections.sort(ring);
        return ring;
    }
}
