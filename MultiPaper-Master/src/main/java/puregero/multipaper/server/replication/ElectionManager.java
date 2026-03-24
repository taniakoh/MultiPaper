package puregero.multipaper.server.replication;

import puregero.multipaper.server.replication.messages.PeerElectedMessage;
import puregero.multipaper.server.replication.messages.PeerElectionMessage;

import java.util.concurrent.atomic.AtomicBoolean;

public class ElectionManager {

    private static final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    public static void startElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            return; // Election already in progress
        }
        MasterRole.setElecting();
        String myId = ReplicationConfig.get().myId;
        System.out.println("[Replication] Starting ring election, candidate=" + myId);
        forwardElection(myId);
    }

    public static void onElectionMessage(String candidateId) {
        String myId = ReplicationConfig.get().myId;
        int cmp = candidateId.compareTo(myId);
        if (cmp > 0) {
            // Received candidate is better — forward unchanged
            electionInProgress.set(true);
            MasterRole.setElecting();
            forwardElection(candidateId);
        } else if (cmp < 0) {
            // I am a better candidate — replace with my id
            electionInProgress.set(true);
            MasterRole.setElecting();
            forwardElection(myId);
        } else {
            // candidateId == myId: I won the election
            electionInProgress.set(false);
            System.out.println("[Replication] Won election! Becoming leader.");
            ReplicationConfig config = ReplicationConfig.get();
            MasterRole.becomeLeader();
            // Announce to all connected peers via incoming PeerConnections
            PeerConnection.broadcastToAll(new PeerElectedMessage(config.myId, config.myHost, config.myPort));
            // Start accepting Minecraft server connections (if not already started)
            PeerConnectionManager.onBecomeLeader();
        }
    }

    public static void onElectedMessage(String newLeaderId, String newLeaderHost, int newLeaderPort) {
        electionInProgress.set(false);
        String myId = ReplicationConfig.get().myId;
        if (!newLeaderId.equals(myId)) {
            System.out.println("[Replication] Election complete: leader=" + newLeaderId);
            MasterRole.becomeStandby(newLeaderId, newLeaderHost, newLeaderPort);
        }
        // If newLeaderId == myId: message came back to winner, nothing to do
    }

    private static void forwardElection(String candidateId) {
        boolean sent = PeerConnectionManager.sendElectionToNextInRing(new PeerElectionMessage(candidateId));
        if (!sent) {
            // No reachable peers — self-elect immediately
            System.out.println("[Replication] No peers reachable during election, self-electing.");
            electionInProgress.set(false);
            MasterRole.becomeLeader();
            PeerConnectionManager.onBecomeLeader();
        }
    }
}
