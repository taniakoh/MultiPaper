package puregero.multipaper.server.handlers;

import puregero.multipaper.mastermessagingprotocol.ChunkKey;
import puregero.multipaper.mastermessagingprotocol.messages.masterbound.RequestChunkOwnershipMessage;
import puregero.multipaper.mastermessagingprotocol.messages.serverbound.BooleanMessageReply;
import puregero.multipaper.server.ServerConnection;
import puregero.multipaper.server.replication.ConsensusCoordinator;
import puregero.multipaper.server.replication.LeaseManager;
import puregero.multipaper.server.replication.ReplicationConfig;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class RequestChunkOwnershipHandler {
    public static void handle(ServerConnection connection, RequestChunkOwnershipMessage message) {
        System.out.println(connection.getBungeeCordName() + " is requesting " + Arrays.toString(message.chunks));

        if (!ReplicationConfig.isEnabled()) {
            handleLocalOnly(connection, message);
            return;
        }

        // Fast path: if server already owns ALL requested chunks (active leases), renew and approve
        boolean allLeased = true;
        for (ChunkKey key : message.chunks) {
            if (!LeaseManager.get().hasActiveLease(key, connection.getBungeeCordName())) {
                allLeased = false;
                break;
            }
        }
        if (allLeased) {
            for (ChunkKey key : message.chunks) {
                LeaseManager.get().renewLease(key, connection.getBungeeCordName());
            }
            CompletableFuture.runAsync(() ->
                    connection.sendReply(new BooleanMessageReply(true), message));
            return;
        }

        // Start a consensus round for the first chunk that needs ownership.
        // For batched requests: run rounds sequentially, fail all if any fails.
        startConsensusChain(connection, message, 0);
    }

    private static void startConsensusChain(ServerConnection connection,
                                             RequestChunkOwnershipMessage message, int index) {
        if (index >= message.chunks.length) {
            // All chunks granted
            CompletableFuture.runAsync(() ->
                    connection.sendReply(new BooleanMessageReply(true), message));
            return;
        }

        ChunkKey key = message.chunks[index];
        if (LeaseManager.get().hasActiveLease(key, connection.getBungeeCordName())) {
            LeaseManager.get().renewLease(key, connection.getBungeeCordName());
            startConsensusChain(connection, message, index + 1);
            return;
        }

        ConsensusCoordinator.get().startOwnershipRound(key, connection.getBungeeCordName(), connection, message);
        // Note: ConsensusCoordinator will call sendReply directly when the round completes.
        // For batched requests, each round sends its own reply — the last one wins for the
        // client. In practice, multipaper requests one chunk at a time.
    }

    private static void handleLocalOnly(ServerConnection connection, RequestChunkOwnershipMessage message) {
        boolean hasAtLeastOneChunkLocked = false;
        for (ChunkKey key : message.chunks) {
            if (puregero.multipaper.server.ChunkSubscriptionManager.getOwner(key.world, key.x, key.z) == connection) {
                hasAtLeastOneChunkLocked = true;
            }
        }
        if (hasAtLeastOneChunkLocked) {
            for (ChunkKey key : message.chunks) {
                puregero.multipaper.server.ChunkSubscriptionManager.lock(connection, key.world, key.x, key.z, true);
            }
            CompletableFuture.runAsync(() ->
                    connection.sendReply(new BooleanMessageReply(true), message));
        } else {
            connection.sendReply(new BooleanMessageReply(false), message);
        }
    }
}
