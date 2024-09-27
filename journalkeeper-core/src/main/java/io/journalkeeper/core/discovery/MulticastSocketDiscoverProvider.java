package io.journalkeeper.core.discovery;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MulticastSocketDiscoverProvider implements DiscoverProvider, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MulticastSocketDiscoverProvider.class);

    private Properties properties;
    private final int MAX_PACKET_SIZE = 1024;
    private final Map<String, List<URI>> tokenToNodes = new ConcurrentHashMap<>();
    private volatile boolean stop = false;
    private static final int SOCKET_TIMEOUT_MS = 1000; // 1秒超时
    private MulticastSocket socket;
    private InetAddress group;
    @Override
    public void init(Properties properties) {
        try {   
            this.properties = properties;
            int multicastPort = Integer.parseInt(properties.getProperty("multicast.port"));
            String multicastAddress = properties.getProperty("multicast.address");
            socket = new MulticastSocket(multicastPort);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            group = InetAddress.getByName(multicastAddress);
            socket.joinGroup(group);
        
            // 启动监听线程
            Thread listenerThread = new Thread(this::listen);
            listenerThread.setName("MulticastSocketDiscoverProvider-Listener");
            listenerThread.setDaemon(true);
                listenerThread.start();
        } catch (IOException e) {
            throw new RuntimeException("初始化失败", e);
        }
    }

    @Override
    public void register(URI uri, String token) {
        int multicastPort = Integer.parseInt(properties.getProperty("multicast.port"));
        try {
            String message = uri.toString() + "," + token;
            byte[] buf = message.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, multicastPort);
            socket.send(packet);
        } catch (IOException e) {
            throw new RuntimeException("注册节点失败", e);
        }
    }

    @Override
    public List<URI> discoverNodes(String token) {
        return tokenToNodes.getOrDefault(token, Collections.emptyList());
    }

    private void listen() {
        logger.info("开始节点发现监听，group: {}, port: {}", group, socket.getLocalPort());
        byte[] buf = new byte[MAX_PACKET_SIZE];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        this.stop = false;
        while (!stop) {
            try {
                socket.receive(packet);
                String received = new String(packet.getData(), 0, packet.getLength());
                String[] parts = received.split(",");
                String token = parts[1];
                URI uri = URI.create(parts[0]);
                tokenToNodes.computeIfAbsent(token, t -> new ArrayList<>()).add(uri);
                logger.info("收到节点注册: " + uri + " -> " + token);
            } catch (SocketTimeoutException ignored) {
                // 超时是正常的，继续循环
            } catch (IOException e) {
                if (!stop) {
                    logger.warn("接收数据包时发生错误", e);
                }
            }
        }
        logger.info("停止节点发现监听，group: {}, port: {}", group, socket.getLocalPort());
    }
    @Override
    public void close() {
        this.stop = true;
        if (socket != null) {
            try {
                socket.leaveGroup(group);
                socket.close();
            } catch (IOException e) {
                logger.warn("关闭Socket时发生错误", e);
            }
        }
    }


}
