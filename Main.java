import java.awt.Desktop;
import java.net.URI;

/** Runs everything at once: collector, simulated sensors and the dashboard in the default browser. */
public class Main {
    public static void main(String[] args) throws Exception {
        Server.start(); // returns once the sockets are bound, so the sensors can connect
        String url = "http://localhost:" + Server.HTTP_PORT;
        if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
            Desktop.getDesktop().browse(URI.create(url));
        } else {
            System.out.println("Open " + url + " in a browser.");
        }
        Sensor.main(args); // takes Sensor's options (--sensors, --rate); runs until Ctrl-C
    }
}
