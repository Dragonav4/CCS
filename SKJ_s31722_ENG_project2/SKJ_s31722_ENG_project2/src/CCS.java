import java.io.*;
import java.net.*;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralized Computing Server (CCS)
 * Implements service discovery via UDP and client handling via TCP.
 */
public class CCS {
    private static final String DISCOVERY_MESSAGE = "CCS DISCOVER";
    private static final String FOUND_MESSAGE = "CCS FOUND";

    //--Statistics---
    private static final AtomicLong clientCount = new AtomicLong(0);
    private static final AtomicLong intervalClientCount = new AtomicLong(0);
    private static final AtomicLong requestCount = new AtomicLong(0);
    private static final AtomicLong intervalRequestCount = new AtomicLong(0);
    private static final AtomicLong incorrectRequestCount = new AtomicLong(0);
    private static final AtomicLong intervalIncorrectRequestCount = new AtomicLong(0);
    private static final AtomicLong resultSum = new AtomicLong(0);
    private static final AtomicLong intervalResultSum = new AtomicLong(0);
    private static final ConcurrentHashMap<String, AtomicLong> operationCount = new ConcurrentHashMap<>();
    private static final int STATISTICS_INTERVAL = 10; //10
    private static final ConcurrentHashMap<String, AtomicLong> intervalOperationCount = new ConcurrentHashMap<>();
//    private static final ConcurrentHashMap<String, Boolean> activeClients = new ConcurrentHashMap<>();
private static final Set<String> activeClients =
        Collections.synchronizedSet(new HashSet<String>());



    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java CCS <port>");
            return;
        }

        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Port must be an Integer");
            return;
        }

        // Managing a pool of threads
        ExecutorService clientHandlers = Executors.newCachedThreadPool();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            DatagramSocket udpSocket = new DatagramSocket(port); // listen for UDP messages
            ServerSocket tcpSocket = new ServerSocket(port); // handle client connections
            System.out.println("CCS started on port: " + port);
            System.out.println("Server is running on IP: " + InetAddress.getLocalHost().getHostAddress());

            Thread udpThread = new Thread(() -> handleDiscovery(udpSocket));
            udpThread.start();

            Thread tcpThread = new Thread(() -> handleClients(tcpSocket, clientHandlers));
            tcpThread.start();

            scheduler.scheduleAtFixedRate(CCS::printStatistics, STATISTICS_INTERVAL, STATISTICS_INTERVAL, TimeUnit.SECONDS);

            udpThread.join();
            tcpThread.join();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Server shutting down...");
                scheduler.shutdown();
                clientHandlers.shutdown();
                try {
                    if (!clientHandlers.awaitTermination(10, TimeUnit.SECONDS)) {
                        clientHandlers.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    clientHandlers.shutdownNow();
                }
            }));

        } catch (IOException | InterruptedException e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            clientHandlers.shutdown();
            scheduler.shutdown();
        }
    }

    /**
     * Handles discovery messages sent via UDP.
     *
     * @param udpSocket DatagramSocket for listening to UDP messages.
     */
    public static void handleDiscovery(DatagramSocket udpSocket) {
        try {
            byte[] buffer = new byte[12]; //1024
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                udpSocket.receive(packet);
                System.out.println("Gate bytes:" + packet.getLength());
                String message = new String(packet.getData(), 0, packet.getLength());
                if (packet.getLength() > 12) {
                    System.out.println("Received oversized message. Ignoring.");
                    continue;
                }

                if (message.equals(DISCOVERY_MESSAGE)) {
                    System.out.println("Discovery request received from: " + packet.getAddress());
                    byte[] response = FOUND_MESSAGE.getBytes();
                    DatagramPacket responsePacket = new DatagramPacket(response, response.length, packet.getAddress(), packet.getPort());
                    udpSocket.send(responsePacket);
                    System.out.println("Response sent to: " + packet.getAddress());
                } else {
                    System.out.println("Invalid discovery message received: " + message);
                }
            }
        } catch (IOException e) {
            System.err.println("UDP Discovery Error: " + e.getMessage());
        }
    }

    /**
     * Handles incoming TCP client connections.
     *
     * @param tcpSocket      ServerSocket for accepting client connections.
     * @param clientHandlers ExecutorService for managing client threads.
     */
    private static void handleClients(ServerSocket tcpSocket, ExecutorService clientHandlers) {
        try {
            while (true) {
                Socket clientSocket = tcpSocket.accept();
                String clientIP = clientSocket.getInetAddress().getHostAddress();

                if (!activeClients.contains(clientIP)) {
                    clientCount.incrementAndGet();
                    intervalClientCount.incrementAndGet();
                    System.out.println("New client connected: " + clientIP);
                    System.out.println("Active clients: " + activeClients);
                } else {
                    System.out.println("Client reconnected from: " + clientIP);
                }
                clientHandlers.submit(() -> handleClient(clientSocket), activeClients.remove(clientIP));
            }
        } catch (IOException e) {
            System.err.println("TCP Error: " + e.getMessage());
        }
    }

    /**
     * Handles a single client connection.
     *
     * @param clientSocket Socket representing the client connection.
     */
    public static void handleClient(Socket clientSocket) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true)) {
            try {
                clientSocket.setSoTimeout(20000);
            } catch (SocketException e) {
                System.out.println("Client timed out: " + clientSocket.getInetAddress());
            }

            String request;
            while ((request = reader.readLine()) != null) {
                String[] parts = request.split(" ");
                if (parts.length != 3) {
                    writer.println("ERROR");
                    incorrectRequestCount.incrementAndGet();
                    intervalIncorrectRequestCount.incrementAndGet();
                    continue;
                }

                String operation = parts[0];
                int arg1, arg2;
                try {
                    arg1 = Integer.parseInt(parts[1]);
                    arg2 = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    writer.println("ERROR");
                    incorrectRequestCount.incrementAndGet();
                    intervalIncorrectRequestCount.incrementAndGet();
                    continue;
                }

                int result;
                switch (operation.toUpperCase()) {
                    case "ADD":
                        result = arg1 + arg2;
                        operationCount.computeIfAbsent("ADD", k -> new AtomicLong(0)).incrementAndGet();

                        break;
                    case "SUB":
                        result = arg1 - arg2;
                        operationCount.computeIfAbsent("SUB", k -> new AtomicLong(0)).incrementAndGet();
                        break;
                    case "MUL":
                        result = arg1 * arg2;
                        operationCount.computeIfAbsent("MUL", k -> new AtomicLong(0)).incrementAndGet();
                        break;
                    case "DIV":
                        if (arg2 == 0) {
                            writer.println("ERROR");
                            incorrectRequestCount.incrementAndGet();
                            intervalIncorrectRequestCount.incrementAndGet();
                            continue;
                        }
                        result = arg1 / arg2;
                        operationCount.computeIfAbsent("DIV", k -> new AtomicLong(0)).incrementAndGet();
                        break;
                    default:
                        writer.println("ERROR");
                        incorrectRequestCount.incrementAndGet();
                        intervalIncorrectRequestCount.incrementAndGet();
                        continue;
                }
                resultSum.addAndGet(result);
                intervalResultSum.addAndGet(result);

                requestCount.incrementAndGet();
                intervalRequestCount.incrementAndGet();
                writer.println(result);
            }
        } catch (IOException e) {
            System.err.println("Client handling error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
        }
    }

    /**
     * Periodically prints server statistics.
     */
    public static void printStatistics() {
        System.out.println("--- Statistics ---");
        System.out.printf("Connected clients: %d | DIFF: %d%n",
                clientCount.get(),
                intervalClientCount.getAndSet(0));
        System.out.printf("Total requests: %d | DIFF: %d%n",
                requestCount.get(),
                intervalRequestCount.getAndSet(0));
        System.out.printf("Incorrect requests: %d | DIFF: %d%n",
                incorrectRequestCount.get(),
                intervalIncorrectRequestCount.getAndSet(0));
        System.out.printf("Sum of results: %d | DIFF: %d%n",
                resultSum.get(),
                intervalResultSum.getAndSet(0));

        operationCount.forEach((op, count) -> {
            long intervalCount = intervalOperationCount.getOrDefault(op, new AtomicLong(0)).getAndSet(0);
            System.out.printf("%s: %d | DIFF: %d%n", op, count.get(), intervalCount);
        });
        System.out.println();
    }
}
