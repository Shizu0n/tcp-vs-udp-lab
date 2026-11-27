import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;

/** Plain-Java checks of the network model and the accounting (no JUnit needed). */
public class ServerTest {
    static int failures = 0;

    static void check(String name, boolean ok) {
        System.out.println((ok ? "ok    " : "FAIL  ") + name);
        if (!ok) failures++;
    }

    static boolean near(double a, double b) {
        return Math.abs(a - b) < 1e-9;
    }

    /** nextDouble() replays a script, so each loss and jitter draw is chosen by the test. */
    static class FakeRng implements RandomGenerator {
        final double[] script;
        int next = 0;

        FakeRng(double... script) {
            this.script = script;
        }

        @Override public double nextDouble() {
            return script[next++];
        }

        @Override public long nextLong() {
            throw new UnsupportedOperationException();
        }
    }

    static final Server.Config CFG = new Server.Config(0.5, 20, 10);

    static Server.Stats feed(long... seqs) {
        Server.Stats s = new Server.Stats();
        for (long seq : seqs) s.add(new Server.Reading(1, 0, seq, 10_000_000, 25), 10.05); // sent at 10 s
        return s;
    }

    static boolean rejects(Map<String, String> raw) {
        try {
            Server.Config.clean(raw);
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    public static void main(String[] args) {
        // Network model
        check("udp drops", Server.udpFate(CFG, new FakeRng(0.1)) == null);
        check("udp delays", near(Server.udpFate(CFG, new FakeRng(0.9, 0.0)), 0.020));
        check("tcp backoff doubles",
                near(Server.tcpDeliveryTime(100, 0, CFG, new FakeRng(0.1, 0.1, 0.9, 0.0)), 100 + 0.020 + 0.2 + 0.4));
        check("tcp never overtakes previous segment",
                near(Server.tcpDeliveryTime(100, 105, CFG, new FakeRng(0.9, 0.0)), 105));

        // Dashboard input
        check("config is clamped", Server.Config.clean(Map.of("loss", "3", "delay_ms", "-5", "jitter_ms", "1e9"))
                .equals(new Server.Config(0.5, 0, 1000)));
        check("config rejects NaN", rejects(Map.of("loss", "NaN", "delay_ms", "1", "jitter_ms", "1")));
        check("config rejects missing field", rejects(Map.of("loss", "0.1")));
        check("dashboard body is read", Server.numberFields("{\"loss\":0.05,\"delay_ms\":20,\"jitter_ms\":10}")
                .equals(Map.of("loss", "0.05", "delay_ms", "20", "jitter_ms", "10")));

        // Wire format (untrusted input)
        Server.Reading r = new Server.Reading(7, 2, 41, 123_456_789, 25.5);
        check("reading round-trips", r.equals(Server.Reading.parse(r.format())));
        check("rejects garbage", Server.Reading.parse("not a reading") == null);
        check("rejects bad number", Server.Reading.parse("1;0;x;1;1") == null);
        check("rejects missing field", Server.Reading.parse("1;0;1;1") == null);

        // Accounting
        Server.Snapshot reordered = feed(0, 1, 3, 2).snapshot(10.05 + Server.GRACE + 0.1);
        check("reorder is not loss", reordered.lost() == 0 && reordered.outOfOrder() == 1);
        Server.Stats gap = feed(0, 2, 3);
        Server.Snapshot now = gap.snapshot(10.05);
        check("gap may still be in flight", now.lost() == 0);
        check("latency measured", Math.abs(now.latAvgMs() - 50) < 1e-6);
        Server.Snapshot later = gap.snapshot(10.05 + Server.GRACE + 0.1);
        check("gap is loss after grace", later.lost() == 1 && near(later.lossPct(), 25));
        check("huge seq jump is capped", feed(0, 1_000_000_000).missing.size() == Server.MAX_GAP);

        // Reliable UDP: a retransmitted copy is dropped, a late reading still fills its gap
        Server.Stats dup = feed(0, 1, 1, 3, 2, 2);
        check("duplicate is not counted twice", dup.received == 4 && dup.duplicates == 2 && dup.outOfOrder == 1);
        // Right after a reset the first reading seen may not be the oldest one in flight
        Server.Stats early = feed(5, 7, 3, 6, 4, 3);
        check("reading older than the first is late, not duplicate",
                early.received == 5 && early.outOfOrder == 3 && early.duplicates == 1);

        // Retransmission timeout (RFC 6298): SRTT + 4 x RTTVAR, clamped
        Sensor.Rto rto = new Sensor.Rto();
        check("rto starts at its initial value", rto.nanos() == Sensor.Rto.INITIAL);
        rto.sample(100_000_000L); // first sample: SRTT 100 ms, RTTVAR 50 ms
        check("rto after first sample is 300 ms", rto.nanos() == 300_000_000L);
        rto.sample(100_000_000L); // steady RTT shrinks RTTVAR to 37.5 ms
        check("rto shrinks when rtt is steady", rto.nanos() == 250_000_000L);
        Sensor.Rto fast = new Sensor.Rto();
        fast.sample(1_000_000L); // 1 ms RTT: the timeout still covers the timer granularity G
        check("rto never drops below rtt + G", fast.nanos() == 1_000_000L + Sensor.Rto.G);
        fast.backoff();
        check("timeout doubles when the timer fires", fast.nanos() == 2 * (1_000_000L + Sensor.Rto.G));
        fast.sample(1_000_000L);
        check("a valid sample ends the backoff", fast.nanos() < 2 * (1_000_000L + Sensor.Rto.G));

        // Untrusted input caps: an endless TCP line, invented senders, huge seq gaps
        try {
            java.io.BufferedReader in = new java.io.BufferedReader(new java.io.StringReader("1;0;1;1;25\r\nlast"));
            check("bounded readLine reads lines", "1;0;1;1;25".equals(Server.readLine(in)) && "last".equals(Server.readLine(in))
                    && Server.readLine(in) == null);
        } catch (java.io.IOException e) {
            check("bounded readLine reads lines", false);
        }
        try {
            Server.readLine(new java.io.BufferedReader(new java.io.StringReader("x".repeat(Server.MAX_LINE + 1))));
            check("endless line is rejected", false);
        } catch (java.io.IOException e) {
            check("endless line is rejected", true);
        }
        Server.Stats flood = new Server.Stats();
        for (int i = 0; i <= Server.MAX_SENSORS; i++) flood.add(new Server.Reading(1, i, 0, 10_000_000, 25), 10.05);
        check("invented senders are capped", flood.maxSeq.size() == Server.MAX_SENSORS);
        Server.Stats gaps = new Server.Stats();
        for (int i = 0; i < Server.MAX_MISSING / Server.MAX_GAP + 5; i++) {
            gaps.add(new Server.Reading(2, i, 0, 10_000_000, 25), 10.05);
            gaps.add(new Server.Reading(2, i, 1_000_000, 10_000_000, 25), 10.05);
        }
        check("open gaps are capped", gaps.missing.size() == Server.MAX_MISSING);

        // Delivery scheduler: due order, ties in arrival order
        Server.Network network = new Server.Network();
        Thread runner = new Thread(network);
        runner.setDaemon(true);
        runner.start();
        List<String> ran = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(3);
        network.after(0.03, () -> { ran.add("late"); done.countDown(); });
        network.after(0.01, () -> { ran.add("tie-1"); done.countDown(); });
        network.after(0.01, () -> { ran.add("tie-2"); done.countDown(); });
        try {
            check("scheduler runs every task", done.await(2, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            check("scheduler runs every task", false);
        }
        check("scheduler keeps due order", ran.equals(List.of("tie-1", "tie-2", "late")));

        System.out.println(failures == 0 ? "all passed" : failures + " failed");
        System.exit(failures == 0 ? 0 : 1);
    }
}
