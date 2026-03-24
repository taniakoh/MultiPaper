package puregero.multipaper.server.replication;

import java.util.concurrent.CountDownLatch;

public class MasterRole {

    public enum State {
        STARTING, LEADER, STANDBY, ELECTING
    }

    private static volatile State state = State.STARTING;
    private static volatile String leaderId = null;
    private static volatile String leaderHost = null;
    private static volatile int leaderPort = 0;
    public static final CountDownLatch initialRoleLatch = new CountDownLatch(1);

    public static State getState() {
        return state;
    }

    public static boolean isLeader() {
        return state == State.LEADER;
    }

    public static String getLeaderId() {
        return leaderId;
    }

    public static String getLeaderHost() {
        return leaderHost;
    }

    public static int getLeaderPort() {
        return leaderPort;
    }

    public static void becomeLeader() {
        ReplicationConfig config = ReplicationConfig.get();
        System.out.println("[Replication] Becoming LEADER (id=" + config.myId + ")");
        leaderId = config.myId;
        leaderHost = config.myHost;
        leaderPort = config.myPort;
        state = State.LEADER;
        initialRoleLatch.countDown();
    }

    public static void becomeStandby(String newLeaderId, String newLeaderHost, int newLeaderPort) {
        System.out.println("[Replication] Becoming STANDBY, leader=" + newLeaderId
                + " at " + newLeaderHost + ":" + newLeaderPort);
        leaderId = newLeaderId;
        leaderHost = newLeaderHost;
        leaderPort = newLeaderPort;
        state = State.STANDBY;
        initialRoleLatch.countDown();
    }

    public static void setElecting() {
        if (state != State.LEADER) {
            state = State.ELECTING;
        }
    }
}
