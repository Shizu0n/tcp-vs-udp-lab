import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Scalability benchmark: one OS thread vs one virtual thread per sensor, at growing sensor counts.
 * The collector and the sensors run as separate JVMs, so the numbers are the collector's alone.
 * Writes the table to benchmarks/latest-results.md (git-ignored: it is rewritten on every run).
 */
public class Benchmark {
    static final int[] SENSORS = {10, 100, 1000, 5000};
    static final int WARMUP_S = 10, MEASURE_S = 10;
    static final double RATE = 20; // readings/s per sensor (Sensor's default)
    static final String BASE = "http://localhost:" + Server.HTTP_PORT;
    static final HttpClient http = HttpClient.newHttpClient();

    /** Optional args: sensor counts to run, e.g. "5000" (default: 10 100 1000 5000). */
    public static void main(String[] args) throws Exception {
        int[] sizes = args.length == 0 ? SENSORS : java.util.Arrays.stream(args).mapToInt(Integer::parseInt).toArray();
        StringBuilder table = new StringBuilder(
                "| Threads | Sensores | Threads do SO | Memória (MB) | CPU (%) | Recebidas/s (esperado) "
                        + "| TCP média / p95 (ms) | UDP média / p95 (ms) | UDP perdidas "
                        + "| Entregas TCP: ocupada % / atraso méd. ms | Entregas UDP: ocupada % / atraso méd. ms "
                        + "| `udp` ocupada (%) | Espera dos locks (%) |\n"
                        + "|---|---|---|---|---|---|---|---|---|---|---|---|---|\n");
        for (boolean virtual : new boolean[]{false, true}) {
            for (int n : sizes) {
                String row = run(virtual, n);
                System.out.println(row);
                table.append(row).append('\n');
            }
        }
        Path out = Path.of("benchmarks", "latest-results.md");
        Files.createDirectories(out.getParent());
        Files.writeString(out, table);
        System.out.println("\nSaved to " + out);
    }

    static String run(boolean virtual, int n) throws Exception {
        Process server = java("-Dvirtual=" + virtual, "Server");
        Process sensors = null;
        try {
            waitForServer(server);
            post("/config", "{\"loss\":0,\"delay_ms\":20,\"jitter_ms\":0}"); // clean link: anything above ~21 ms is the collector's cost
            // The load generator always uses virtual threads, so it is not what runs out of threads.
            sensors = java("-Dvirtual=true", "Sensor", "--sensors", String.valueOf(n), "--no-rudp"); // same load as the published numbers
            Thread.sleep(WARMUP_S * 1000L);
            post("/reset", "");
            metrics(); // primes the CPU-load counter for the window
            Thread.sleep(MEASURE_S * 1000L);
            Map<String, String> m = metrics();
            double perSecond = Double.parseDouble(m.get("tcp_received")) / MEASURE_S;
            return String.format(Locale.ROOT, "| %s | %d | %s | %s | %s | %.0f (%.0f) | %s / %s | %s / %s | %s | %s / %s | %s / %s | %s | %s |",
                    virtual ? "virtual" : "SO", n, m.get("os_threads"), m.get("committed_mb"), m.get("cpu_pct"),
                    perSecond, n * RATE, m.get("tcp_lat_avg_ms"), m.get("tcp_lat_p95_ms"),
                    m.get("udp_lat_avg_ms"), m.get("udp_lat_p95_ms"), m.get("udp_lost"),
                    m.get("tcp_net_busy_pct"), m.get("tcp_net_lag_avg_ms"),
                    m.get("udp_net_busy_pct"), m.get("udp_net_lag_avg_ms"),
                    m.get("udp_busy_pct"), m.get("lock_wait_pct"));
        } finally {
            // Sensors first: killed clients reset their connections, so the ports free up for the next run.
            if (sensors != null) {
                sensors.destroy();
                sensors.waitFor();
            }
            server.destroy();
            server.waitFor();
            Thread.sleep(500);
        }
    }

    static Process java(String... args) throws IOException {
        List<String> cmd = new ArrayList<>(List.of(ProcessHandle.current().info().command().orElse("java"),
                "-cp", System.getProperty("java.class.path")));
        cmd.addAll(List.of(args));
        return new ProcessBuilder(cmd).redirectOutput(Redirect.DISCARD).redirectError(Redirect.DISCARD).start();
    }

    static void waitForServer(Process server) throws Exception {
        for (int i = 0; i < 50; i++) {
            if (!server.isAlive()) throw new IllegalStateException("collector exited (port " + Server.HTTP_PORT + ", 9001 or 9002 in use?)");
            try {
                metrics();
                return;
            } catch (IOException e) {
                Thread.sleep(200);
            }
        }
        throw new IllegalStateException("collector did not answer within 10 s");
    }

    static Map<String, String> metrics() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + "/metrics")).build();
        return Server.numberFields(http.send(req, HttpResponse.BodyHandlers.ofString()).body());
    }

    static void post(String path, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(BASE + path)).POST(HttpRequest.BodyPublishers.ofString(body)).build();
        int status = http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status != 204) throw new IllegalStateException(path + " returned " + status);
    }
}
