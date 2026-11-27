import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.random.RandomGenerator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Collector: receives the same sensor readings over TCP and UDP, simulates an
 * impaired network in front of each protocol, and serves a live dashboard (SSE).
 *
 * Loopback never loses packets, so the impairment is simulated here:
 * - UDP: a lost datagram is dropped; the others get independent delays.
 * - TCP: a lost segment is retransmitted after an RTO (doubling on each new
 *   loss), and nothing is delivered before the previous segment, which is the
 *   head-of-line blocking real TCP shows under loss.
 */
public class Server {
    static final int TCP_PORT = 9001;
    static final int UDP_PORT = 9002;
    static final int RUDP_PORT = 9003; // reliable UDP: readings in, ACKs back out
    static final int HTTP_PORT = 8765; // not 8080: the most contested dev port (Docker proxies, other servers)
    static final double RTO = 0.2;     // initial retransmission timeout, s (Linux minimum is 200 ms)
    static final double WINDOW = 2.0;  // seconds of history behind rate and latency stats
    // A gap only counts as lost if still open after GRACE: UDP reorders, so a
    // missing reading may just be late. Must exceed the max simulated UDP delay
    // (1000 + 1000 ms after Config.clean).
    static final double GRACE = 2.5;
    static final int MAX_GAP = 1000;   // largest seq jump tracked as individual gaps
    // One thread per sensor: an OS thread by default, a virtual thread with -Dvirtual=true (Benchmark compares both).
    static final Thread.Builder PER_SENSOR_THREADS = Boolean.getBoolean("virtual") ? Thread.ofVirtual() : Thread.ofPlatform();

    // ---- Network model ----------------------------------------------------

    record Config(double loss, double delayMs, double jitterMs) {
        /** Validate dashboard input; loss is capped so TCP backoff stays bounded. */
        static Config clean(Map<String, String> raw) {
            return new Config(clamp(raw, "loss", 0.5), clamp(raw, "delay_ms", 1000), clamp(raw, "jitter_ms", 1000));
        }

        private static double clamp(Map<String, String> raw, String key, double hi) {
            String text = raw.get(key);
            if (text == null) throw new IllegalArgumentException("missing " + key);
            double value = Double.parseDouble(text); // NumberFormatException is an IllegalArgumentException
            if (!Double.isFinite(value)) throw new IllegalArgumentException(key + " must be finite");
            return Math.min(Math.max(value, 0), hi);
        }

        String toJson() {
            return String.format(Locale.ROOT, "{\"loss\":%s,\"delay_ms\":%s,\"jitter_ms\":%s}", loss, delayMs, jitterMs);
        }
    }

    /** One-way delay of a packet in seconds: base + uniform jitter. */
    static double networkDelay(Config cfg, RandomGenerator rng) {
        return (cfg.delayMs() + rng.nextDouble() * cfg.jitterMs()) / 1000;
    }

    /** Null if the datagram is lost, else its delay. UDP never retransmits. */
    static Double udpFate(Config cfg, RandomGenerator rng) {
        return rng.nextDouble() < cfg.loss() ? null : networkDelay(cfg, rng);
    }

    /** Time (s) a TCP segment reaches the application (head-of-line blocking). */
    static double tcpDeliveryTime(double now, double lastDelivery, Config cfg, RandomGenerator rng) {
        double extra = 0, rto = RTO;
        while (rng.nextDouble() < cfg.loss()) {
            extra += rto;
            rto *= 2;
        }
        return Math.max(lastDelivery, now + networkDelay(cfg, rng) + extra);
    }

    // ---- Wire format ------------------------------------------------------

    /** One reading. Wire format: "run;sensor;seq;tsMicros;value", one per TCP line or UDP datagram. */
    record Reading(long run, int sensor, long seq, long tsMicros, double value) {
        String format() {
            return String.format(Locale.ROOT, "%d;%d;%d;%d;%.2f", run, sensor, seq, tsMicros, value);
        }

        /** Untrusted network input: a reading, or null if malformed. */
        static Reading parse(String line) {
            String[] f = line.strip().split(";");
            if (f.length != 5) return null;
            try {
                return new Reading(Long.parseLong(f[0]), Integer.parseInt(f[1]), Long.parseLong(f[2]),
                        Long.parseLong(f[3]), Double.parseDouble(f[4]));
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }

    /** Wall-clock time in microseconds since the epoch, shared by sensors and collector. */
    static long nowMicros() {
        return ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
    }

    // ---- Statistics -------------------------------------------------------

    record SensorKey(long run, int sensor) {}

    record Gap(SensorKey sensor, long seq) {}

    record Snapshot(long received, long lost, double lossPct, long outOfOrder, long duplicates, double rate,
                    double latAvgMs, double latP95Ms, double latMaxMs, double jitterMs) {
        String toJson() {
            return String.format(Locale.ROOT, "{\"received\":%d,\"lost\":%d,\"loss_pct\":%.2f,\"out_of_order\":%d,"
                            + "\"duplicates\":%d,\"rate\":%.1f,\"lat_avg_ms\":%.2f,\"lat_p95_ms\":%.2f,"
                            + "\"lat_max_ms\":%.2f,\"jitter_ms\":%.2f}",
                    received, lost, lossPct, outOfOrder, duplicates, rate, latAvgMs, latP95Ms, latMaxMs, jitterMs);
        }
    }

    /** Not thread-safe: callers hold the protocol's lock ({@link Proto#lock}). */
    static class Stats {
        long received, lost, outOfOrder, duplicates;
        final Map<SensorKey, Long> maxSeq = new HashMap<>();
        final Map<SensorKey, Long> firstSeq = new HashMap<>();  // lowest seq seen: nothing below it is expected yet
        final Map<Gap, Double> missing = new HashMap<>();       // gap -> when it was noticed (s)
        final ArrayDeque<double[]> recent = new ArrayDeque<>(); // {arrival s, latency s} inside WINDOW
        double jitter;
        Double lastLatency;

        void add(Reading r, double arrival) {
            SensorKey key = new SensorKey(r.run(), r.sensor());
            Long top = maxSeq.get(key);
            boolean newest = top == null || r.seq() > top;
            Long first = firstSeq.get(key);
            boolean beforeFirst = first != null && r.seq() < first; // sent right after a reset, overtaken by a later one
            if (!newest && !beforeFirst && missing.remove(new Gap(key, r.seq())) == null) {
                duplicates++; // already counted: a retransmission whose ACK was lost (reliable UDP)
                return;
            }
            if (newest) {
                if (top != null) { // cap the gap: seq comes from the network
                    for (long gap = Math.max(top + 1, r.seq() - MAX_GAP); gap < r.seq(); gap++) {
                        missing.put(new Gap(key, gap), arrival);
                    }
                }
                maxSeq.put(key, r.seq());
                firstSeq.putIfAbsent(key, r.seq());
            } else {
                outOfOrder++; // a late reading filling a gap
                if (beforeFirst) { // the readings between it and the old first are now expected too
                    for (long gap = Math.max(r.seq() + 1, first - MAX_GAP); gap < first; gap++) {
                        missing.put(new Gap(key, gap), arrival);
                    }
                    firstSeq.put(key, r.seq());
                }
            }
            received++;
            // Latency compares two clocks: only exact when sensor and collector
            // share a machine (the "no global clock" problem from the lecture).
            double latency = arrival - r.tsMicros() / 1e6;
            if (lastLatency != null) jitter += (Math.abs(latency - lastLatency) - jitter) / 16; // RFC 3550
            lastLatency = latency;
            recent.add(new double[]{arrival, latency});
        }

        Snapshot snapshot(double now) {
            while (!recent.isEmpty() && recent.peekFirst()[0] < now - WINDOW) recent.pollFirst();
            double[] lats = recent.stream().mapToDouble(x -> x[1]).sorted().toArray();
            int before = missing.size();
            missing.values().removeIf(noticed -> noticed < now - GRACE);
            lost += before - missing.size();
            long total = received + lost;
            boolean any = lats.length > 0;
            return new Snapshot(received, lost, total == 0 ? 0 : 100.0 * lost / total, outOfOrder, duplicates,
                    lats.length / WINDOW,
                    any ? 1000 * Arrays.stream(lats).average().orElse(0) : 0,
                    any ? 1000 * lats[(int) (0.95 * (lats.length - 1))] : 0,
                    any ? 1000 * lats[lats.length - 1] : 0,
                    1000 * jitter);
        }
    }

    // ---- Shared state -----------------------------------------------------

    /**
     * The compared protocols. Each has its own lock and its own delivery thread, so their
     * deliveries run in parallel and never wait on each other (Benchmark: one shared thread
     * saturated at 5000 sensors). RUDP is "reliable UDP": UDP plus ACKs and retransmission.
     */
    enum Proto {
        TCP, UDP, RUDP;

        final Object lock = new Object();
        final Network network = new Network();
        Stats stats = new Stats(); // guarded by lock

        String key() {
            return name().toLowerCase();
        }
    }

    /** Runs action holding every protocol lock, always taken in declaration order: a fixed order rules out deadlock. */
    static void withAllLocks(Runnable action) {
        withLocksFrom(0, action);
    }

    private static void withLocksFrom(int i, Runnable action) {
        if (i == Proto.values().length) {
            action.run();
            return;
        }
        synchronized (Proto.values()[i].lock) {
            withLocksFrom(i + 1, action);
        }
    }

    // Bumped on reset (under every lock): readings still "in the air" from before the
    // reset, e.g. under the previous scenario's delays, are not counted.
    static volatile long epoch = 0;
    // When the last reset happened. Readings sent before it are not counted either: a reliable-UDP
    // retransmission of an older reading arrives after the reset with a fresh epoch, and counting it
    // opened false gaps (measured: 35 "lost" vs 11 for plain UDP, more received than TCP).
    // shortcut: compares the sensor's clock with ours, so skew between machines shifts the cut by the skew.
    static volatile long resetMicros = 0;
    static volatile Config config = new Config(0, 20, 10);

    /**
     * Runs every simulated delivery on one thread, at its due time and in due
     * order, so Stats see one reading at a time. Not ScheduledExecutorService:
     * on Windows its waits round up to a 15.6 ms tick (measured: +8 ms average,
     * p95 17 ms on a 5 ms link), while Object.wait gets 1 ms resolution when the
     * wait is not a multiple of 10 ms.
     */
    static class Network implements Runnable {
        record Task(long dueNanos, long order, Runnable action) {}

        private final PriorityQueue<Task> queue = new PriorityQueue<>(
                Comparator.comparingLong(Task::dueNanos).thenComparingLong(Task::order));
        private long added = 0;
        // Instrumentation for /metrics, written only by this network thread.
        volatile long idleNanos, lagNanos, lagMaxNanos, delivered;
        private long seenIdle, seenLag, seenDelivered; // values at the previous /metrics window

        /** Busy share and scheduling lag since the previous call (callers serialize: Server.metricsJson). */
        String windowJson(String name, long windowNanos) {
            long idle = idleNanos, lag = lagNanos, done = delivered;
            String json = String.format(Locale.ROOT, "\"%s_busy_pct\":%.0f,\"%s_lag_avg_ms\":%.1f,\"%s_lag_max_ms\":%.0f",
                    name, 100 - 100.0 * (idle - seenIdle) / windowNanos,
                    name, done == seenDelivered ? 0 : (lag - seenLag) / 1e6 / (done - seenDelivered),
                    name, lagMaxNanos / 1e6);
            seenIdle = idle;
            seenLag = lag;
            seenDelivered = done;
            lagMaxNanos = 0; // shortcut: racy reset of the network thread's max, fine for a per-window estimate
            return json;
        }

        synchronized void after(double seconds, Runnable action) {
            queue.add(new Task(System.nanoTime() + (long) (seconds * 1e9), added++, action));
            notify();
        }

        @Override public void run() {
            while (true) {
                Runnable action;
                synchronized (this) {
                    Task next;
                    while ((next = queue.peek()) == null || next.dueNanos() > System.nanoTime()) {
                        long ms = next == null ? 0 : Math.max(1, (next.dueNanos() - System.nanoTime()) / 1_000_000);
                        if (ms % 10 == 0 && ms > 0) ms--; // keep the 1 ms resolution; the loop re-checks
                        long idleFrom = System.nanoTime();
                        try {
                            wait(ms); // 0 = until a task is added
                        } catch (InterruptedException e) {
                            return;
                        }
                        idleNanos += System.nanoTime() - idleFrom;
                    }
                    Task task = queue.poll();
                    long lag = System.nanoTime() - task.dueNanos(); // how late this delivery runs
                    lagNanos += lag;
                    lagMaxNanos = Math.max(lagMaxNanos, lag);
                    delivered++;
                    action = task.action();
                }
                action.run();
            }
        }
    }

    static double nowSeconds() {
        return nowMicros() / 1e6;
    }

    static final LongAdder lockWaitNanos = new LongAdder(); // time deliveries wait to enter the stats lock
    static volatile long udpIdleNanos;                      // time the udp thread sits in receive()

    static void deliver(Proto proto, Reading r, long sentEpoch) {
        long waitFrom = System.nanoTime();
        synchronized (proto.lock) {
            lockWaitNanos.add(System.nanoTime() - waitFrom);
            if (sentEpoch == epoch && r.tsMicros() >= resetMicros) proto.stats.add(r, nowSeconds());
        }
    }

    static void after(Proto proto, double seconds, Runnable task) {
        proto.network.after(seconds, task);
    }

    // ---- Sockets ----------------------------------------------------------

    /** Plain UDP (reliable == false) or reliable UDP, which ACKs every reading back to its sender. */
    static void udpLoop(DatagramSocket socket, boolean reliable) {
        Proto proto = reliable ? Proto.RUDP : Proto.UDP;
        byte[] buffer = new byte[1024];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            long idleFrom = System.nanoTime();
            try {
                socket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            if (!reliable) udpIdleNanos += System.nanoTime() - idleFrom;
            Reading r = Reading.parse(new String(packet.getData(), 0, packet.getLength(), UTF_8));
            Double delay = udpFate(config, ThreadLocalRandom.current()); // the reading crosses the impaired network
            if (r == null || delay == null) continue;
            long sentEpoch = epoch;
            SocketAddress sender = packet.getSocketAddress();
            after(proto, delay, () -> {
                deliver(proto, r, sentEpoch);
                if (!reliable) return;
                // The ACK crosses the same impaired network on its way back, so it can be lost or late too.
                Double back = udpFate(config, ThreadLocalRandom.current());
                if (back != null) after(proto, back, () -> sendAck(socket, sender, r.seq()));
            });
        }
    }

    /** ACK wire format: the acknowledged seq, as text. */
    static void sendAck(DatagramSocket socket, SocketAddress to, long seq) {
        byte[] ack = Long.toString(seq).getBytes(UTF_8);
        try {
            socket.send(new DatagramPacket(ack, ack.length, to));
        } catch (IOException e) {
            // sensor gone: it will stop waiting for this ACK on its own
        }
    }

    static void tcpLoop(ServerSocket server) {
        while (true) {
            try {
                Socket socket = server.accept();
                PER_SENSOR_THREADS.start(() -> handleTcp(socket));
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    static void handleTcp(Socket socket) {
        // Deliveries due at the same instant may run in any order, so each one
        // delivers the oldest pending reading instead of "its own" reading.
        double last = 0;
        long connEpoch = epoch;
        Queue<Reading> pending = new ConcurrentLinkedQueue<>();
        try (socket; BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), UTF_8))) {
            for (String line; (line = in.readLine()) != null; ) { // framing: one reading per line
                Reading r = Reading.parse(line);
                if (r == null) continue;
                if (connEpoch != epoch) {
                    // Reset = fresh network: new readings must not queue behind
                    // the previous scenario's backlog (that backlog is discarded).
                    last = 0;
                    connEpoch = epoch;
                    pending = new ConcurrentLinkedQueue<>();
                }
                double now = System.nanoTime() / 1e9;
                last = tcpDeliveryTime(now, last, config, ThreadLocalRandom.current());
                pending.add(r);
                Queue<Reading> queue = pending;
                long sentEpoch = connEpoch;
                after(Proto.TCP, last - now, () -> deliver(Proto.TCP, queue.poll(), sentEpoch));
            }
        } catch (IOException e) {
            // sensor went away
        }
    }

    // ---- Dashboard --------------------------------------------------------

    private static final Pattern NUMBER_FIELD = Pattern.compile("\"(\\w+)\"\\s*:\\s*([-+\\w.]+)");

    /** The dashboard posts a flat JSON object of numbers; this is all the JSON we read. */
    static Map<String, String> numberFields(String json) {
        Map<String, String> fields = new HashMap<>();
        Matcher m = NUMBER_FIELD.matcher(json);
        while (m.find()) fields.put(m.group(1), m.group(2));
        return fields;
    }

    static void handleHttp(HttpExchange ex) throws IOException {
        try {
            switch (ex.getRequestMethod() + " " + ex.getRequestURI().getPath()) {
                case "GET /" -> send(ex, 200, "text/html; charset=utf-8", Files.readAllBytes(Path.of("dashboard.html")));
                case "GET /events" -> streamEvents(ex);
                case "GET /metrics" -> send(ex, 200, "application/json", metricsJson().getBytes(UTF_8));
                case "POST /config" -> {
                    try {
                        config = Config.clean(numberFields(new String(ex.getRequestBody().readAllBytes(), UTF_8)));
                    } catch (IllegalArgumentException e) {
                        send(ex, 400, "text/plain", "expected loss, delay_ms, jitter_ms".getBytes(UTF_8));
                        return;
                    }
                    ex.sendResponseHeaders(204, -1);
                }
                case "POST /reset" -> {
                    withAllLocks(() -> {
                        epoch++;
                        resetMicros = nowMicros();
                        for (Proto p : Proto.values()) p.stats = new Stats();
                    });
                    ex.sendResponseHeaders(204, -1);
                }
                default -> send(ex, 404, "text/plain", "not found".getBytes(UTF_8));
            }
        } finally {
            ex.close();
        }
    }

    static void send(HttpExchange ex, int status, String type, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", type);
        ex.sendResponseHeaders(status, body.length);
        ex.getResponseBody().write(body);
    }

    // Counter values at the previous /metrics call: busy shares are computed over the window between calls.
    private static long lastCall = System.nanoTime(), lastUdpIdle, lastLockWait;

    /** Process load plus delivery stats, as flat JSON for Benchmark. Virtual threads are not OS threads, so they are not counted. */
    static synchronized String metricsJson() {
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        Map<Proto, Snapshot> snaps = snapshots();
        Snapshot t = snaps.get(Proto.TCP), u = snaps.get(Proto.UDP);
        long now = System.nanoTime(), window = now - lastCall;
        long udpIdle = udpIdleNanos, lockWait = lockWaitNanos.sum();
        String json = String.format(Locale.ROOT, "{\"os_threads\":%d,\"committed_mb\":%.0f,\"cpu_pct\":%.0f,"
                        + "\"tcp_received\":%d,\"tcp_lat_avg_ms\":%.1f,\"tcp_lat_p95_ms\":%.1f,"
                        + "\"udp_received\":%d,\"udp_lost\":%d,\"udp_lat_avg_ms\":%.1f,\"udp_lat_p95_ms\":%.1f,"
                        + "%s,%s,\"udp_busy_pct\":%.0f,\"lock_wait_pct\":%.1f}",
                ManagementFactory.getThreadMXBean().getThreadCount(), os.getCommittedVirtualMemorySize() / 1048576.0,
                100 * Math.max(0, os.getProcessCpuLoad()),
                t.received(), t.latAvgMs(), t.latP95Ms(), u.received(), u.lost(), u.latAvgMs(), u.latP95Ms(),
                Proto.TCP.network.windowJson("tcp_net", window), Proto.UDP.network.windowJson("udp_net", window),
                100 - 100.0 * (udpIdle - lastUdpIdle) / window,
                100.0 * (lockWait - lastLockWait) / window);
        lastCall = now;
        lastUdpIdle = udpIdle;
        lastLockWait = lockWait;
        return json;
    }

    /** One consistent snapshot of every protocol (all locks held at once). */
    static Map<Proto, Snapshot> snapshots() {
        Map<Proto, Snapshot> snaps = new EnumMap<>(Proto.class);
        withAllLocks(() -> {
            double now = nowSeconds();
            for (Proto p : Proto.values()) snaps.put(p, p.stats.snapshot(now));
        });
        return snaps;
    }

    static void streamEvents(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "text/event-stream");
        ex.getResponseHeaders().set("Cache-Control", "no-cache");
        ex.sendResponseHeaders(200, 0); // length 0 = chunked: the stream never ends
        OutputStream out = ex.getResponseBody();
        try {
            while (true) {
                StringBuilder json = new StringBuilder("{");
                snapshots().forEach((p, s) -> json.append('"').append(p.key()).append("\":").append(s.toJson()).append(','));
                json.append("\"epoch\":").append(epoch).append(",\"config\":").append(config.toJson()).append('}');
                out.write(("data: " + json + "\n\n").getBytes(UTF_8));
                out.flush();
                Thread.sleep(500);
            }
        } catch (IOException | InterruptedException e) {
            // browser tab closed
        }
    }

    /** Binds every socket and starts the collector threads; returns once sensors can connect. */
    static void start() throws IOException {
        // Bind first: if a port is taken we fail before starting threads that would keep the JVM alive.
        DatagramSocket udp = new DatagramSocket(UDP_PORT);
        DatagramSocket rudp = new DatagramSocket(RUDP_PORT);
        ServerSocket tcp = new ServerSocket(TCP_PORT, 1000); // deep accept queue: thousands of sensors connect at once
        HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", HTTP_PORT), 0);
        http.setExecutor(Executors.newCachedThreadPool()); // each open dashboard holds a thread for its stream
        http.createContext("/", Server::handleHttp);
        http.start();
        for (Proto p : Proto.values()) new Thread(p.network, p.key() + "-network").start();
        new Thread(() -> udpLoop(udp, false), "udp").start();
        new Thread(() -> udpLoop(rudp, true), "rudp").start();
        new Thread(() -> tcpLoop(tcp), "tcp").start();
        System.out.printf("TCP :%d  UDP :%d  UDP confiavel :%d  dashboard http://localhost:%d%n",
                TCP_PORT, UDP_PORT, RUDP_PORT, HTTP_PORT);
    }

    public static void main(String[] args) throws IOException {
        start();
    }
}
