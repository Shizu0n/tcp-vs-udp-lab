import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simulated sensors: each one sends the same reading over TCP, over UDP and over
 * reliable UDP (UDP + ACKs + retransmission) so the collector compares the
 * protocols on identical traffic.
 *
 * Run on another machine with --host <collector IP> to use a real network.
 */
public class Sensor {
    /** Reliable UDP gives up on a reading after this: on a live dashboard an older reading is useless. */
    static final long GIVE_UP_NANOS = 500_000_000L;

    /** A reliable-UDP reading not yet acknowledged; attempts = retransmissions so far. */
    record Pending(byte[] data, long firstSent, long lastSent, int attempts) {}

    /** Retransmission timeout from measured round trips: RFC 6298 (SRTT + max(G, 4 x RTTVAR)), as TCP computes it. */
    static class Rto {
        // G covers Windows' ~15.6 ms timer tick: without it a steady RTT drives RTTVAR to ~0 and every
        // slightly late ACK triggers a needless retransmission (measured: 465 duplicates on a lossless LAN).
        // TCP's 200 ms floor is not used: readings expire at 500 ms.
        static final long G = 20_000_000L, MAX = 1_000_000_000L, INITIAL = 100_000_000L;
        private double srtt = -1, rttvar;
        private long rto = INITIAL;

        synchronized void sample(long rttNanos) {
            if (srtt < 0) {
                srtt = rttNanos;
                rttvar = rttNanos / 2.0;
            } else {
                rttvar = 0.75 * rttvar + 0.25 * Math.abs(srtt - rttNanos);
                srtt = 0.875 * srtt + 0.125 * rttNanos;
            }
            rto = Math.min((long) (srtt + Math.max(G, 4 * rttvar)), MAX); // a valid sample also ends any backoff
        }

        /**
         * A retransmission timer fired: double the timeout and keep it until a valid sample arrives
         * (RFC 6298, 5.5). Without this, a timeout that became too short never recovers: every reading
         * is retransmitted before its ACK, and Karn's rule discards all of their samples (measured:
         * 3,646 duplicates after switching from the LAN scenario to a 300 ms link).
         */
        synchronized void backoff() {
            rto = Math.min(rto * 2, MAX);
        }

        synchronized long nanos() {
            return rto;
        }
    }

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int sensors = 5;
        double rate = 20; // readings per second per sensor
        boolean reliable = true;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = args[++i];
                case "--sensors" -> sensors = Integer.parseInt(args[++i]);
                case "--rate" -> rate = Double.parseDouble(args[++i]);
                case "--no-rudp" -> reliable = false; // Benchmark measures TCP and UDP only
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        InetAddress address = InetAddress.getByName(host);
        long run = ThreadLocalRandom.current().nextLong(1L << 30); // lets the collector tell restarts apart
        System.out.printf("%d sensors x %s/s -> %s (they retry until the collector is up)%n", sensors, rate, host);
        List<Thread> threads = new ArrayList<>();
        for (int id = 0; id < sensors; id++) {
            int sensorId = id;
            double r = rate;
            boolean rel = reliable;
            threads.add(Server.PER_SENSOR_THREADS.start(() -> runSensor(sensorId, run, address, r, rel)));
        }
        for (Thread t : threads) t.join();
    }

    static void runSensor(int id, long run, InetAddress address, double rate, boolean reliable) {
        long seq = 0, backoffMs = 100;
        long periodNanos = (long) (1e9 / rate);
        while (true) {
            try (Socket tcp = new Socket(address, Server.TCP_PORT);
                 DatagramSocket udp = new DatagramSocket();
                 DatagramSocket rudp = reliable ? new DatagramSocket() : null) {
                backoffMs = 100; // connected: the next failure starts with a short wait again
                tcp.setTcpNoDelay(true); // disable Nagle so TCP sends each reading at once, like UDP does
                Writer out = new BufferedWriter(new OutputStreamWriter(tcp.getOutputStream(), UTF_8));
                Map<Long, Pending> pending = new ConcurrentHashMap<>();
                if (rudp != null) {
                    rudp.setSoTimeout(5); // the ACK thread wakes at least every 5 ms to check for overdue readings
                    Server.PER_SENSOR_THREADS.start(() -> rudpLoop(rudp, address, pending, new Rto()));
                }
                long next = System.nanoTime(); // restart the schedule: no burst to "catch up" after an outage
                for (; ; seq++) {
                    double value = 25 + 5 * Math.sin(seq / 40.0 + id) + ThreadLocalRandom.current().nextGaussian() * 0.3;
                    String line = new Server.Reading(run, id, seq, Server.nowMicros(), value).format();
                    out.write(line + "\n"); // TCP is a byte stream: the newline frames each reading
                    out.flush();
                    byte[] data = line.getBytes(UTF_8); // UDP keeps datagram boundaries
                    udp.send(new DatagramPacket(data, data.length, address, Server.UDP_PORT));
                    if (rudp != null) {
                        long now = System.nanoTime();
                        pending.put(seq, new Pending(data, now, now, 0)); // before sending, or the ACK could win the race
                        rudp.send(new DatagramPacket(data, data.length, address, Server.RUDP_PORT));
                    }
                    // Sleep until the next slot on an absolute schedule so timer
                    // rounding does not accumulate and lower the real rate.
                    next += periodNanos;
                    long wait = next - System.nanoTime();
                    if (wait > 0) Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                }
            } catch (IOException e) {
                // Collector down, restarting, or its accept queue full: retry with exponential backoff.
            } catch (InterruptedException e) {
                return;
            }
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                return;
            }
            backoffMs = Math.min(backoffMs * 2, 5000);
        }
    }

    /** Reliable-UDP side of one sensor: reads ACKs and retransmits readings whose ACK is overdue. */
    static void rudpLoop(DatagramSocket socket, InetAddress address, Map<Long, Pending> pending, Rto rto) {
        byte[] buffer = new byte[32];
        while (!socket.isClosed()) {
            try {
                DatagramPacket ack = new DatagramPacket(buffer, buffer.length);
                socket.receive(ack);
                Pending acked = pending.remove(Long.parseLong(new String(ack.getData(), 0, ack.getLength(), UTF_8)));
                // Karn's rule: the ACK of a retransmitted reading may answer either copy, so it is not a valid sample.
                if (acked != null && acked.attempts() == 0) rto.sample(System.nanoTime() - acked.lastSent());
            } catch (SocketTimeoutException | NumberFormatException e) {
                // no ACK this round, or garbage: go check for overdue readings
            } catch (IOException e) {
                return; // socket closed: the sensor is reconnecting
            }
            long now = System.nanoTime(), timeout = rto.nanos();
            boolean fired = false;
            for (Map.Entry<Long, Pending> e : pending.entrySet()) {
                Pending p = e.getValue();
                if (now - p.firstSent() > GIVE_UP_NANOS) {
                    pending.remove(e.getKey()); // too old to matter: give up, the collector will count it lost
                } else if (now - p.lastSent() > timeout) {
                    try {
                        socket.send(new DatagramPacket(p.data(), p.data().length, address, Server.RUDP_PORT));
                    } catch (IOException ex) {
                        return;
                    }
                    pending.put(e.getKey(), new Pending(p.data(), p.firstSent(), now, p.attempts() + 1));
                    fired = true;
                }
            }
            if (fired) rto.backoff(); // once per check, not per reading: one late ACK burst doubles it once
        }
    }
}
