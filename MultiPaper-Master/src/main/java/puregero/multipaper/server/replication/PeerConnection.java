package puregero.multipaper.server.replication;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import puregero.multipaper.server.FileLocker;
import puregero.multipaper.server.replication.messages.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Handles an incoming peer connection on this master's PeerServer.
 * Receives {@link LeaderBoundMessage}s and sends {@link PeerBoundMessage}s.
 * In the leaderless model, any master can have peers connect inbound.
 */
public class PeerConnection extends LeaderBoundMessageHandler {

    private final SocketChannel channel;
    private String peerId;

    private static final Map<String, PeerConnection> peers = new ConcurrentHashMap<>();

    public PeerConnection(SocketChannel channel) {
        this.channel = channel;
    }

    public void send(PeerBoundMessage message) {
        channel.writeAndFlush(message);
    }

    public static void broadcastToAll(PeerBoundMessage message) {
        peers.values().forEach(c -> c.send(message));
    }

    public static Map<String, PeerConnection> getPeers() {
        return peers;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (peerId != null) {
            peers.remove(peerId);
            PeerMembershipTracker.get().markDead(peerId);
            System.out.println("[Replication] Peer disconnected: " + peerId);
        }
    }

    @Override
    public void handle(PeerHelloMessage message) {
        if (message.protocolVersion != PeerHelloMessage.PROTOCOL_VERSION) {
            System.err.println("[Replication] Protocol version mismatch with connecting peer: expected="
                    + PeerHelloMessage.PROTOCOL_VERSION + " got=" + message.protocolVersion + ". Closing connection.");
            channel.close();
            return;
        }
        this.peerId = message.peerId;
        peers.put(peerId, this);
        PeerMembershipTracker.get().markAlive(peerId);
        System.out.println("[Replication] Peer connected: " + peerId);
        ReplicationConfig config = ReplicationConfig.get();
        send(new PeerWelcomeMessage(config.myId, config.myHost, config.myPort));
    }

    @Override
    public void handle(PeerRequestFullSyncMessage message) {
        System.out.println("[Replication] Full sync requested by: " + message.peerId);
        CompletableFuture.runAsync(this::performFullSync);
    }

    @Override
    public void handle(PeerHeartbeatMessage message) {
        send(new PeerHeartbeatAckMessage());
    }

    @Override
    public void handle(PeerElectionMessage message) {
        // no-op: election removed in favour of leaderless quorum
    }

    @Override
    public void handle(PeerWriteAckMessage message) {
        QuorumWriteCoordinator.get().onWriteAck(peerId, message.correlationId, message.success);
    }

    @Override
    public void handle(PeerPromiseMessage message) {
        ConsensusCoordinator.get().onPromise(peerId, message.roundId, message.promised,
                message.acceptedBallotLamport, message.acceptedBallotMasterId, message.acceptedValue);
    }

    @Override
    public void handle(PeerAcceptAckMessage message) {
        ConsensusCoordinator.get().onAcceptAck(peerId, message.roundId, message.accepted);
    }

    // -------------------------------------------------------------------------
    // Full sync
    // -------------------------------------------------------------------------

    private void performFullSync() {
        List<File> files = collectReplicableFiles();
        System.out.println("[Replication] Starting full sync: " + files.size() + " files");
        send(new PeerFullSyncStartMessage(files.size()));

        File workDir = new File(".").getAbsoluteFile().getParentFile();
        for (File file : files) {
            try {
                byte[] data = FileLocker.readBytes(file);
                if (data == null || data.length == 0) continue;
                String relativePath = workDir.toURI().relativize(file.toURI()).getPath();
                send(new PeerFullSyncFileMessage(relativePath, file.lastModified(), data));
            } catch (IOException e) {
                System.err.println("[Replication] Error reading " + file + " for sync: " + e.getMessage());
            }
        }

        send(new PeerFullSyncCompleteMessage());
        System.out.println("[Replication] Full sync sent to " + peerId);
    }

    private List<File> collectReplicableFiles() {
        List<File> result = new ArrayList<>();
        File workDir = new File(".");
        File[] contents = workDir.listFiles();
        if (contents != null) {
            for (File dir : contents) {
                if (dir.isDirectory() && new File(dir, "region").isDirectory()) {
                    collectWorldFiles(dir, result);
                }
            }
        }
        File syncedFiles = new File("synced-server-files");
        if (syncedFiles.exists()) {
            collectAllFiles(syncedFiles, result);
        }
        return result;
    }

    private void collectWorldFiles(File worldDir, List<File> result) {
        for (String sub : new String[]{"region", "entities", "playerdata", "data", "advancements", "stats"}) {
            collectAllFiles(new File(worldDir, sub), result);
        }
        for (String dim : new String[]{"DIM-1", "DIM1"}) {
            File dimDir = new File(worldDir, dim);
            collectAllFiles(new File(dimDir, "region"), result);
            collectAllFiles(new File(dimDir, "entities"), result);
        }
        for (String single : new String[]{"level.dat", "uid.dat"}) {
            File f = new File(worldDir, single);
            if (f.isFile()) result.add(f);
        }
    }

    private void collectAllFiles(File dir, List<File> result) {
        if (!dir.exists() || !dir.isDirectory()) return;
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) {
            if (f.isFile()) result.add(f);
            else if (f.isDirectory()) collectAllFiles(f, result);
        }
    }
}
