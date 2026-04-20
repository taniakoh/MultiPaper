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
    private volatile boolean needsSync = true; // request full sync from first peer that welcomes us

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
        channelActive = false;
        System.out.println("[Replication] Lost connection to peer " + peer.id);
        PeerMembershipTracker.get().markDead(peer.id);
        connect();
    }

    private void scheduleHeartbeat() {
        ReplicationConfig config = ReplicationConfig.get();
        MessageBootstrap.getEventLoopGroup().scheduleAtFixedRate(() -> {
            if (!channelActive) return;
            long elapsed = System.currentTimeMillis() - lastAckReceived;
            if (elapsed > config.heartbeatTimeoutMs) {
                System.out.println("[Replication] Heartbeat timeout to " + peer.id + " (" + elapsed + "ms)");
                channelActive = false;
                PeerMembershipTracker.get().markDead(peer.id);
                if (channel != null) channel.close();
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
        System.out.println("[Replication] Handshake ACK from " + peer.id);
        PeerMembershipTracker.get().markAlive(peer.id);
        if (needsSync) {
            needsSync = false;
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
        // Send ACK only for quorum-tracked writes (correlationId >= 0)
        if (message.correlationId >= 0) {
            channel.writeAndFlush(new PeerWriteAckMessage(message.correlationId, true));
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
        PeerMembershipTracker.get().markAlive(peer.id);
    }

    @Override
    public void handle(PeerElectedMessage message) {
        // no-op: election removed in favour of leaderless quorum
    }

    @Override
    public void handle(PeerPrepareMessage message) {
        ConsensusCoordinator.get().onPrepare(message.chunkKey,
                message.ballotLamport, message.ballotMasterId, message.roundId, peer.id);
    }

    @Override
    public void handle(PeerAcceptMessage message) {
        ConsensusCoordinator.get().onAccept(message.chunkKey,
                message.ballotLamport, message.ballotMasterId, message.serverOwner, message.roundId, peer.id);
    }

    @Override
    public void handle(PeerCommitMessage message) {
        ConsensusCoordinator.get().onCommit(message.chunkKey, message.serverOwner, message.leaseExpiry);
    }

    @Override
    public void handle(PeerLeaseExpiredMessage message) {
        LeaseManager.get().applyRemoteExpiry(message.chunkKey);
    }
}
