/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.remoting.transport;

import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Ipv4工具
 *
 * @author hexiaofeng
 */
public class IpUtil {

    /**
     * 内网地址
     */
    public static final Lan INTRANET = new Lan("172.16.0.0/12;192.168.0.0/16;10.0.0.0/8");
    public static final String IPV4_PORT_SEPARATOR = ":";
    public static final String IPV6_PORT_SEPARATOR = "_";

    /**
     * 管理IP
     */
    public static String MANAGE_IP = "10.";
    /**
     * 网卡
     */
    public static final String NET_INTERFACE;
    /**
     * 是否启用IPV6
     */
    public static final boolean PREFER_IPV6;

    static {
        // 从环境变量里面获取默认的网卡和管理网络
        NET_INTERFACE = System.getProperty("nic");
        MANAGE_IP = System.getProperty("manage_ip", MANAGE_IP);
        PREFER_IPV6 = Boolean.parseBoolean(System.getProperty("java.net.preferIPv6Stack", "false"));


    }


    /**
     * 得到指定网卡上的地址
     *
     * @param nic     网卡
     * @param exclude 排除的地址
     * @return 地址列表
     */
    public static List<String> getLocalIps(final String nic, final String exclude) {
        try {
            List<String> result = getIps(nic);
            // 只有一个IP
            int count = result.size();
            if (count <= 1) {
                return result;
            }
            if (exclude != null && !exclude.isEmpty()) {
                String ip;
                // 多个IP，排除IP
                for (int i = count - 1; i >= 0; i--) {
                    ip = result.get(i);
                    if (ip.startsWith(exclude)) {
                        // 删除排除的IP
                        result.remove(i);
                        count--;
                        if (count == 1) {
                            // 确保有一个IP
                            break;
                        }
                    }
                }
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getIps(String nic) throws SocketException {
        List<String> result = new ArrayList<>();
        NetworkInterface ni;
        Enumeration<InetAddress> ias;
        InetAddress address;
        Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
        while (netInterfaces.hasMoreElements()) {
            ni = netInterfaces.nextElement();
            if (nic != null && !nic.isEmpty() && !ni.getName().equals(nic)) {
                continue;
            }
            ias = ni.getInetAddresses();
            while (ias.hasMoreElements()) {
                address = ias.nextElement();
                if (!address.isLoopbackAddress() &&
                        //回送地址：它是分配给回送接口的地址
                        !address.isAnyLocalAddress() &&
                        //多播地址：也称为 Anylocal 地址或通配符地址
                        ((address instanceof Inet4Address && !PREFER_IPV6) || (address instanceof Inet6Address && PREFER_IPV6))) {
                    result.add(address.getHostAddress());
                }
            }
        }
        return result;
    }

    /**
     * 得到本机内网地址
     *
     * @param nic      网卡
     * @param manageIp 管理段IP地址
     * @return 本机地址
     */
    public static String getLocalIp(final String nic, final String manageIp) {
        List<String> ips = getLocalIps(nic, manageIp);
        if (!ips.isEmpty()) {
            if (ips.size() == 1) {
                return ips.get(0);
            }
            if (!PREFER_IPV6) {
                for (String ip : ips) {
                    if (INTRANET.contains(ip)) {
                        return ip;
                    }
                }
            }
            return ips.get(0);
        }
        return null;
    }


    /**
     * 得到本机内网地址
     *
     * @return 本机地址
     */
    public static String getLocalIp() {
        return getLocalIp(NET_INTERFACE, MANAGE_IP);
    }

    /**
     * 把地址对象转换成字符串
     *
     * @param address 地址
     * @return 地址字符串
     */
    public static String toAddress(final SocketAddress address) {
        if (address == null) {
            return null;
        }
        if (address instanceof InetSocketAddress && !((InetSocketAddress) address).isUnresolved()) {
            InetSocketAddress isa = (InetSocketAddress) address;

            StringBuilder builder = new StringBuilder(50);
            builder.append(isa.getAddress().getHostAddress());
            String separator = isValidIpV4Address(isa.getAddress().getHostAddress()) ? IPV4_PORT_SEPARATOR : IPV6_PORT_SEPARATOR;
            builder.append(separator).append(isa.getPort());
            return builder.toString();
        } else {
            return address.toString();
        }
    }


    /**
     * 分解IP,只支持IPV4
     *
     * @param ip ip地址
     * @return 分段
     */
    public static int[] parseIp(final String ip) {
        if (!isValidIpV4Address(ip)) {
            return null;
        }

        if (ip.isEmpty()) {
            return null;
        }
        int[] parts = new int[4];
        int index = 0;
        int start = -1;
        int end = -1;
        int part;
        char[] chars = ip.toCharArray();
        char ch;
        for (int i = 0; i < chars.length; i++) {
            if (index > 3) {
                // 超过了4个数字
                return null;
            }
            ch = chars[i];
            switch (ch) {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    if (start == -1) {
                        start = i;
                    }
                    end = i;
                    if (end - start > 2) {
                        // 超长了，最多3个数字
                        return null;
                    }
                    break;
                case '.':
                    // 分隔符
                    if (start == -1) {
                        // 前面必须有字符
                        return null;
                    }
                    part = Integer.parseInt(new String(chars, start, end - start + 1));
                    if (part > 255) {
                        return null;
                    }
                    parts[index++] = part;
                    start = -1;
                    end = -1;
                    break;
                default:
                    return null;
            }
        }
        if (start > -1) {
            part = Integer.parseInt(new String(chars, start, end - start + 1));
            if (part > 255) {
                return null;
            }
            parts[index] = part;
            return index == 3 ? parts : null;
        } else {
            // 以.结尾
            return null;
        }
    }

    /**
     * 把IP地址转换成长整型，只支持IPv4
     *
     * @param ip IP地址
     * @return 长整形
     */
    public static long toLong(final String ip) {
        int[] data = parseIp(ip);
        if (data == null) {
            throw new IllegalArgumentException(String.format("invalid ip %s", ip));
        }
        long result = 0;
        result += ((long) data[0]) << 24;
        result += ((long) (data[1]) << 16);
        result += ((long) (data[2]) << 8);
        result += (data[3]);
        return result;
    }

    /**
     * 把长整形转换成IP地址，只支持IPv4
     *
     * @param ip 长整型
     * @return IP字符串
     */
    public static String toIp(long ip) {
        long part1 = (ip) >>> 24;
        long part2 = (ip & 0x00FFFFFF) >>> 16;
        long part3 = (ip & 0x0000FFFF) >>> 8;
        long part4 = ip & 0x000000FF;
        //直接右移24位
        return String.valueOf(part1) + '.' +
                //将高8位置0，然后右移16位
                part2 + '.' +
                //将高16位置0，然后右移8位
                part3 + '.' +
                //将高24位置0
                part4;
    }


    /**
     * Takes a string and parses it to see if it is a valid IPV4 address.
     * @param value IP地址
     * @return true, if the string represents an IPV4 address in dotted
     * notation, false otherwise
     */
    public static boolean isValidIpV4Address(String value) {

        int periods = 0;
        int i;
        int length = value.length();

        if (length > 15) {
            return false;
        }
        char c;
        StringBuilder word = new StringBuilder();
        for (i = 0; i < length; i++) {
            c = value.charAt(i);
            if (c == '.') {
                periods++;
                if (periods > 3) {
                    return false;
                }
                if (word.length() == 0) {
                    return false;
                }
                if (Integer.parseInt(word.toString()) > 255) {
                    return false;
                }
                word.delete(0, word.length());
            } else if (!Character.isDigit(c)) {
                return false;
            } else {
                if (word.length() > 2) {
                    return false;
                }
                word.append(c);
            }
        }

        if (word.length() == 0 || Integer.parseInt(word.toString()) > 255) {
            return false;
        }

        return periods == 3;
    }


}
