package puregero.multipaper.server.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import puregero.multipaper.mastermessagingprotocol.ExtendedByteBuf;
import puregero.multipaper.mastermessagingprotocol.messages.masterbound.MasterBoundMessage;
import puregero.multipaper.mastermessagingprotocol.messages.masterbound.MasterBoundProtocol;
import puregero.multipaper.server.replication.messages.PeerReplicateMessage;

public class ReplicationManager {

    private static final MasterBoundProtocol PROTOCOL = new MasterBoundProtocol();

    public static void replicateToAll(MasterBoundMessage message) {
        if (!ReplicationConfig.isEnabled()) return;
        if (PeerConnection.getPeers().isEmpty()) return;
        byte[] encoded = encodeMessage(message);
        PeerConnection.broadcastToAll(new PeerReplicateMessage(encoded, -1L));
    }

    public static byte[] encodeMessage(MasterBoundMessage message) {
        ByteBuf buf = Unpooled.buffer();
        ExtendedByteBuf ext = new ExtendedByteBuf(buf);
        ext.writeVarInt(message.getTransactionId());
        ext.writeVarInt(PROTOCOL.getMessageId(message));
        message.write(ext);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        buf.release();
        return bytes;
    }

    public static MasterBoundMessage decodeMessage(byte[] rawBytes) {
        ByteBuf buf = Unpooled.wrappedBuffer(rawBytes);
        ExtendedByteBuf ext = new ExtendedByteBuf(buf);
        int transactionId = ext.readVarInt();
        int messageId = ext.readVarInt();
        MasterBoundMessage message = PROTOCOL.getDeserializer(messageId).apply(ext);
        message.setTransactionId(transactionId);
        return message;
    }
}
