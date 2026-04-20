package puregero.multipaper.server.replication;

import puregero.multipaper.mastermessagingprotocol.messages.masterbound.*;
import puregero.multipaper.server.handlers.*;

/**
 * Applies replicated write operations received from a coordinator master.
 * Only write-type messages are dispatched; all other messages are no-ops.
 * In the leaderless model all masters can receive these, not just standbys.
 */
public class StandbyWriteDispatcher extends MasterBoundMessageHandler {

    private static final StandbyWriteDispatcher INSTANCE = new StandbyWriteDispatcher();
    private static final NullServerConnection NULL_CONN = NullServerConnection.get();

    public static void dispatch(byte[] rawBytes) {
        MasterBoundMessage message = ReplicationManager.decodeMessage(rawBytes);
        try {
            message.handle(INSTANCE);
        } catch (Exception e) {
            System.err.println("[Replication] Error applying replicated message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Write handlers — actually persist data
    @Override public void handle(WriteChunkMessage m)        { WriteChunkHandler.handle(NULL_CONN, m); }
    @Override public void handle(WritePlayerMessage m)       { WritePlayerHandler.handle(NULL_CONN, m); }
    @Override public void handle(WriteJsonMessage m)         { WriteJsonHandler.handle(NULL_CONN, m); }
    @Override public void handle(WriteLevelMessage m)        { WriteLevelHandler.handle(NULL_CONN, m); }
    @Override public void handle(WriteDataMessage m)         { WriteDataHandler.handle(NULL_CONN, m); }
    @Override public void handle(WriteStatsMessage m)        { WriteStatsHandler.handle(NULL_CONN, m); }
    @Override public void handle(WriteAdvancementsMessage m) { WriteAdvancementsHandler.handle(NULL_CONN, m); }
    @Override public void handle(WriteUidMessage m)          { WriteUidHandler.handle(NULL_CONN, m); }
    @Override public void handle(WriteTickTimeMessage m)     { WriteTickTimeHandler.handle(NULL_CONN, m); }
    @Override public void handle(UploadFileMessage m)        { UploadFileHandler.handle(NULL_CONN, m); }

    // All other message types are no-ops on standbys
    @Override public void handle(HelloMessage m) {}
    @Override public void handle(PingMessage m) {}
    @Override public void handle(CallDataStorageMessage m) {}
    @Override public void handle(ChunkChangedStatusMessage m) {}
    @Override public void handle(DownloadFileMessage m) {}
    @Override public void handle(ForceReadChunkMessage m) {}
    @Override public void handle(LockChunkMessage m) {}
    @Override public void handle(PlayerConnectMessage m) {}
    @Override public void handle(PlayerDisconnectMessage m) {}
    @Override public void handle(ReadAdvancementMessage m) {}
    @Override public void handle(ReadChunkMessage m) {}
    @Override public void handle(ReadDataMessage m) {}
    @Override public void handle(ReadJsonMessage m) {}
    @Override public void handle(ReadLevelMessage m) {}
    @Override public void handle(ReadPlayerMessage m) {}
    @Override public void handle(ReadStatsMessage m) {}
    @Override public void handle(ReadUidMessage m) {}
    @Override public void handle(RequestChunkOwnershipMessage m) {}
    @Override public void handle(RequestFilesToSyncMessage m) {}
    @Override public void handle(SetPortMessage m) {}
    @Override public void handle(StartMessage m) {}
    @Override public void handle(SubscribeChunkMessage m) {}
    @Override public void handle(SubscribeEntitiesMessage m) {}
    @Override public void handle(SyncChunkOwnerToAllMessage m) {}
    @Override public void handle(SyncChunkSubscribersMessage m) {}
    @Override public void handle(SyncEntitiesSubscribersMessage m) {}
    @Override public void handle(UnlockChunkMessage m) {}
    @Override public void handle(UnsubscribeChunkMessage m) {}
    @Override public void handle(UnsubscribeEntitiesMessage m) {}
    @Override public void handle(WillSaveChunkLaterMessage m) {}
    @Override public void handle(WillSaveEntitiesLaterMessage m) {}
    @Override public void handle(RequestEntityIdBlock m) {}
}
