package puregero.multipaper.server.replication;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import puregero.multipaper.mastermessagingprotocol.MessageBootstrap;
import puregero.multipaper.server.FileLocker;
import puregero.multipaper.server.replication.messages.*;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Outbound connection from any master to a peer's PeerServer.
 * Sends {@link LeaderBoundMessage}s (e.g. election tokens, heartbeats)
 * and receives {@link PeerBoundMessage}s (e.g. replication, welcome, full sync).
 */
@ChannelHandler.Sharable
public class PeerClient extends PeerBoundMessageHandler {

    private final ReplicationConfig.PeerEntry peer;
    private SocketChannel channel;
    private volatile boolean channelActive = false;
    private volatile long lastAckReceived = System.currentTimeMillis();

    /** Buffer for PeerReplicateMessage received during initial full sync */
    private final Queue<byte[]> replicateBuffer = new LinkedList<>();
    private volatile boolean syncing = false;

    private final MessageBootstrap<PeerBoundMessage, LeaderBoundMessage> bootstrap;

    public PeerClient(ReplicationConfig.PeerEntry peer) {
        this.peer = peer;
        bootstrap = new MessageBootstrap<>(
            new PeerBoundProtocol(),
            new LeaderBoundProtocol(),
            ch -> ch.pipeline().addLast(this)
        );
        scheduleHeartbeat();
        connect();
    }

    public void connect() {
        System.out.println("[Replication] Connecting to peer " + peer.id + " at " + peer.host + ":" + peer.port + "...");
        bootstrap.connectTo(peer.host, peer.port).addListener(future -> {
            if (future.cause() != null) {
                CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(this::connect);
            }
        });
    }

    /**
     * Send a LeaderBoundMessage outbound to the peer's PeerServer.
     * Used for election forwarding and heartbeats.
     */
    public void sendLeaderBound(LeaderBoundMessage message) {
        if (channelActive && channel != null) {
            channel.writeAndFlush(message);
        }
    }

    public String getPeerId() {
        return peer.id;
    }

    public boolean isConnected() {
        return channelActive;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        channel = (SocketChannel) ctx.channel();
        channelActive = true;
        lastAckReceived = System.currentTimeMillis();
        System.out.println("[Replication] Connected to peer " + peer.id);
        channel.writeAndFlush(new PeerHelloMessage(ReplicationConfig.get().myId));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        boolean wasActive = channelActive;
        channelActive = false;
        System.out.println("[Replication] Lost connection to peer " + peer.id);

        // If this was our leader, trigger election
        if (wasActive && peer.id.equals(MasterRole.getLeaderId())
                && MasterRole.getState() != MasterRole.State.LEADER) {
            System.out.println("[Replication] Leader " + peer.id + " disconnected, starting election");
            ElectionManager.startElection();
        }

        connect();
    }

    private void scheduleHeartbeat() {
        ReplicationConfig config = ReplicationConfig.get();
        MessageBootstrap.getEventLoopGroup().scheduleAtFixedRate(() -> {
            if (!channelActive) return;
            long elapsed = System.currentTimeMillis() - lastAckReceived;
            if (elapsed > config.heartbeatTimeoutMs) {
                System.out.println("[Replication] Heartbeat timeout to " + peer.id
                        + " (" + elapsed + "ms), triggering election");
                channelActive = false;
                if (channel != null) channel.close();
                if (peer.id.equals(MasterRole.getLeaderId())) {
                    ElectionManager.startElection();
                }
            } else {
                channel.writeAndFlush(new PeerHeartbeatMessage());
            }
        }, config.heartbeatIntervalMs, config.heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    // -------------------------------------------------------------------------
    // Handle PeerBoundMessages received from the peer
    // -------------------------------------------------------------------------

    @Override
    public void handle(PeerWelcomeMessage message) {
        System.out.println("[Replication] Welcome from " + peer.id
                + ": leader=" + message.leaderId + " at " + message.leaderHost + ":" + message.leaderPort);
        boolean wasStarting = MasterRole.getState() == MasterRole.State.STARTING;
        MasterRole.becomeStandby(message.leaderId, message.leaderHost, message.leaderPort);
        if (wasStarting && message.leaderId.equals(peer.id)) {
            // This peer IS the leader — request full sync
            syncing = true;
            channel.writeAndFlush(new PeerRequestFullSyncMessage(ReplicationConfig.get().myId));
        }
    }

    @Override
    public void handle(PeerReplicateMessage message) {
        if (syncing) {
            synchronized (replicateBuffer) {
                replicateBuffer.add(message.rawBytes);
            }
        } else {
            StandbyWriteDispatcher.dispatch(message.rawBytes);
        }
    }

    @Override
    public void handle(PeerFullSyncStartMessage message) {
        System.out.println("[Replication] Full sync starting (" + message.totalFiles + " files)");
        syncing = true;
    }

    @Override
    public void handle(PeerFullSyncFileMessage message) {
        File file = new File(message.relativePath);
        try {
            FileLocker.writeBytes(file, message.data);
        } catch (IOException e) {
            System.err.println("[Replication] Error writing synced file " + message.relativePath + ": " + e.getMessage());
        }
    }

    @Override
    public void handle(PeerFullSyncCompleteMessage message) {
        System.out.println("[Replication] Full sync complete from " + peer.id);
        syncing = false;
        // Flush buffered replicate messages
        synchronized (replicateBuffer) {
            byte[] buffered;
            while ((buffered = replicateBuffer.poll()) != null) {
                StandbyWriteDispatcher.dispatch(buffered);
            }
        }
    }

    @Override
    public void handle(PeerHeartbeatAckMessage message) {
        lastAckReceived = System.currentTimeMillis();
    }

    @Override
    public void handle(PeerElectedMessage message) {
        ElectionManager.onElectedMessage(message.leaderId, message.leaderHost, message.leaderPort);
    }
}
