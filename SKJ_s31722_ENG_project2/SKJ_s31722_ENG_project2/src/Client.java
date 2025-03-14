import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Client program for interacting with the Centralized Computing System (CCS).
 *
 * This client:
 * - Discovers the CCS server via UDP broadcast.
 * - Connects to the CCS server using TCP.
 * - Sends arithmetic requests to the server and processes responses.
 */
public class Client {

    private static final String DISCOVER_MESSAGE = "CCS DISCOVER";
    private static final String FOUND_MESSAGE = "CCS FOUND";
    private static final int DISCOVERY_TIMEOUT = 30000; // in milliseconds

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java Client <port>");
            return;
        }

        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Error: <port> must be an integer.");
            return;
        }

        try {
            // Discover the CCS server via UDP
            InetAddress serverAddress = discoverServer(port);
            if (serverAddress == null) {
                System.err.println("Failed to discover CCS server.");
                return;
            }

            // Connect to the CCS server via TCP
            try (Socket socket = new Socket(serverAddress, port);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                 Scanner scanner = new Scanner(System.in)) {

                System.out.println("Connected to CCS server at " + serverAddress);

                // Interaction loop: send requests and receive responses
                while (true) {
                    System.out.print("Enter request (<OPER> <ARG1> <ARG2>) or 'exit': ");
                    String request = scanner.nextLine();

                    if (request.equalsIgnoreCase("exit")) {
                        System.out.println("Exiting client.");
                        break;
                    }

                    writer.println(request); // Send request to the server
                    String response = reader.readLine(); // Read response from the server

                    if (response == null) {
                        System.err.println("Connection closed by server.");
                        break;
                    }

                    System.out.println("Response: " + response);
                }
            }

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Discovers the CCS server via UDP broadcast.
     *
     * @param port The UDP port used for discovery.
     * @return The server's IP address if discovered; null otherwise.
     * @throws IOException If an error occurs during discovery.
     */
    private static InetAddress discoverServer(int port) throws IOException {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setBroadcast(true);

            // Send discovery message via calculated broadcast address
            byte[] message = DISCOVER_MESSAGE.getBytes();
            InetAddress broadcastAddress = getLocalIPAddressBroadcast();

//            List<InetAddress> listAllAdresses = listAllBroadcastAddresses();


            DatagramPacket packet = new DatagramPacket(
                    message, message.length, broadcastAddress, port); // message, message.length, broadcastAddress, port);
            socket.send(packet);
            System.out.println("Bytes:"+message.length);
            System.out.println("Broadcast discovery message sent to: " + broadcastAddress);

            // Wait for a response
            socket.setSoTimeout(DISCOVERY_TIMEOUT);
            byte[] buffer = new byte[1024];
            DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);

            try {
                socket.receive(responsePacket);
                // Verify the response content
                String response = new String(responsePacket.getData(), 0, responsePacket.getLength());
                if (FOUND_MESSAGE.equals(response)) {
                    System.out.println("Server discovered at: " + responsePacket.getAddress());
                    return responsePacket.getAddress();
                } else {
                    System.err.println("Unexpected response: " + response);
                }
            } catch (SocketTimeoutException e) {
                System.err.println("Discovery timed out.");
            }
        }

        return null;
    }

    /**
     * Calculates the broadcast address for the local network using bitwise operations.
     *
     * @return The broadcast address as an {@link InetAddress}.
     * @throws IOException If an error occurs while calculating the broadcast address.
     */
//    private static InetAddress calculateBroadcastAddress() throws IOException {
//        InetAddress localIPAddress = getLocalIPAddress();//InetAddress.getLocalHost();
//        NetworkInterface networkInterface = NetworkInterface.getByInetAddress(localIPAddress);
//
//        if (networkInterface == null) {
//            throw new IOException("No network interface found for local address.");
//        }
//
//        short prefixLength = -1;
//        for (InterfaceAddress address : networkInterface.getInterfaceAddresses()) {
//            if (address.getAddress() instanceof Inet4Address) {
//                prefixLength = address.getNetworkPrefixLength();
//            }
//        }
//
//        if (prefixLength == -1) {
//            throw new IOException("Could not determine network prefix length.");
//        }
//
//        return bitwiseCalculation(localIPAddress, prefixLength);
//    }

//    /**
//     * Performs bitwise operations to calculate the broadcast address from the given IP address
//     * and subnet prefix length.
//     *
//     * @param IPAddress    The local IP address as an {@link InetAddress}.
//     * @param prefixLength The prefix length of the subnet mask.
//     * @return The calculated broadcast address as an {@link InetAddress}.
//     * @throws IOException If the broadcast address cannot be calculated.
//     */
//    private static InetAddress bitwiseCalculation(InetAddress IPAddress, short prefixLength) throws IOException {
//        byte[] IPBytes = IPAddress.getAddress();
//        int subnetMask = -1 << (32 - prefixLength);
//        byte[] maskBytes = {
//                (byte) ((subnetMask >> 24) & 0xFF),
//                (byte) ((subnetMask >> 16) & 0xFF),
//                (byte) ((subnetMask >> 8) & 0xFF),
//                (byte) (subnetMask & 0xFF)
//        };
//        byte[] broadcastAddress = new byte[4];
//        for (int i = 0; i < 4; i++) {
//            broadcastAddress[i] = (byte) (IPBytes[i] | ~maskBytes[i]);
//        }
//        try {
//            return InetAddress.getByAddress(broadcastAddress);
//        } catch (UnknownHostException e) {
//            throw new IOException("Broadcast address calculation failed.", e);
//        }
//    }

    private static List<InetAddress> listAllBroadcastAddresses() throws SocketException {
        List<InetAddress> broadcastList = new ArrayList<>();
        Enumeration<NetworkInterface> interfaces
                = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();

            if (networkInterface.isLoopback() || !networkInterface.isUp()) {
                continue;
            }

            networkInterface.getInterfaceAddresses().stream()
                    .map(a -> a.getBroadcast())
                    .filter(Objects::nonNull)
                    .forEach(broadcastList::add);
        }
        return broadcastList;
    }


    private static InetAddress getLocalIPAddressBroadcast() throws IOException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            if (networkInterface.isLoopback() || !networkInterface.isUp()) continue;
            for (InterfaceAddress addr : networkInterface.getInterfaceAddresses()) {
                if (addr.getAddress() instanceof Inet4Address) {
                    return addr.getBroadcast();
                }
            }
        }
        throw new IOException("No valid local IP address found.");
    }
}
