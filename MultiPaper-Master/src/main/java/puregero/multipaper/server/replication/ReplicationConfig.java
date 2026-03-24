package puregero.multipaper.server.replication;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReplicationConfig {

    private static ReplicationConfig instance = new ReplicationConfig();

    public boolean enabled = false;
    public String myId = "master-1";
    public String myHost = "localhost";
    public int myPort = 35353;
    public int peerPort = 0; // 0 = derive as myPort + 1
    public List<PeerEntry> peers = new ArrayList<>();
    public long heartbeatIntervalMs = 2000;
    public long heartbeatTimeoutMs = 8000;
    public long initialRoleTimeoutMs = 5000;

    public static class PeerEntry {
        public String id = "";
        public String host = "localhost";
        public int port = 35354;
    }

    public static ReplicationConfig get() {
        return instance;
    }

    public static boolean isEnabled() {
        return instance.enabled;
    }

    public int getEffectivePeerPort() {
        return peerPort > 0 ? peerPort : myPort + 1;
    }

    @SuppressWarnings("unchecked")
    public static void load(File file) {
        ReplicationConfig config = new ReplicationConfig();

        if (file.exists()) {
            try (InputStream in = new FileInputStream(file)) {
                Yaml yaml = new Yaml();
                Map<String, Object> root = yaml.load(in);
                if (root != null && root.containsKey("replication")) {
                    Map<String, Object> repl = (Map<String, Object>) root.get("replication");
                    if (repl.containsKey("enabled")) config.enabled = (boolean) repl.get("enabled");
                    if (repl.containsKey("my-id")) config.myId = (String) repl.get("my-id");
                    if (repl.containsKey("my-host")) config.myHost = (String) repl.get("my-host");
                    if (repl.containsKey("my-port")) config.myPort = (int) repl.get("my-port");
                    if (repl.containsKey("peer-port")) config.peerPort = (int) repl.get("peer-port");
                    if (repl.containsKey("heartbeat-interval-ms"))
                        config.heartbeatIntervalMs = ((Number) repl.get("heartbeat-interval-ms")).longValue();
                    if (repl.containsKey("heartbeat-timeout-ms"))
                        config.heartbeatTimeoutMs = ((Number) repl.get("heartbeat-timeout-ms")).longValue();
                    if (repl.containsKey("initial-role-timeout-ms"))
                        config.initialRoleTimeoutMs = ((Number) repl.get("initial-role-timeout-ms")).longValue();
                    if (repl.containsKey("peers")) {
                        List<Map<String, Object>> peerList = (List<Map<String, Object>>) repl.get("peers");
                        for (Map<String, Object> p : peerList) {
                            PeerEntry entry = new PeerEntry();
                            if (p.containsKey("id")) entry.id = (String) p.get("id");
                            if (p.containsKey("host")) entry.host = (String) p.get("host");
                            if (p.containsKey("port")) entry.port = (int) p.get("port");
                            config.peers.add(entry);
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("[Replication] Failed to load multipaper-master.yml: " + e.getMessage());
            }
        } else {
            try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
                pw.println("replication:");
                pw.println("  enabled: false");
                pw.println("  my-id: \"master-1\"");
                pw.println("  my-host: \"localhost\"");
                pw.println("  my-port: 35353");
                pw.println("  # peer-port: 35354  # defaults to my-port + 1");
                pw.println("  peers: []");
                pw.println("  heartbeat-interval-ms: 2000");
                pw.println("  heartbeat-timeout-ms: 8000");
                pw.println("  initial-role-timeout-ms: 5000");
            } catch (IOException e) {
                System.err.println("[Replication] Failed to write default multipaper-master.yml: " + e.getMessage());
            }
        }

        instance = config;
    }
}
