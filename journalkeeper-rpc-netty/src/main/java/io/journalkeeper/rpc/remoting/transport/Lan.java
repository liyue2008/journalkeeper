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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;

/**
 * 局域网,多个网段由逗号或分号隔开
 * 单个网段格式如下：
 * 172.168.1.0/24
 * 172.168.1.0-172.168.1.255
 * 172.168.1.1
 * 172.168.1.*
 */
public class Lan {
    // 多个网段
    private final List<Segment> segments = new ArrayList<>();
    // ID
    private final int id;
    // 名称
    private final String name;

    public Lan(String ips) {
        this(0, null, ips);
    }

    public Lan(int id, String name, String ips) {
        this.id = id;
        this.name = name;
        if (ips != null && !ips.isEmpty()) {
            StringTokenizer tokenizer = new StringTokenizer(ips, ",;", false);
            String segment;
            while (tokenizer.hasMoreTokens()) {
                segment = tokenizer.nextToken();
                if (segment != null && !segment.isEmpty()) {
                    segments.add(new Segment(segment));
                }
            }
        }
    }

    public List<Segment> getSegments() {
        return segments;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /**
     * 是否包含指定IP
     *
     * @param ip IP
     * @return 布尔值
     */
    public boolean contains(String ip) {
        if (ip == null || ip.isEmpty()) {
            return false;
        }
        for (Segment segment : segments) {
            if (segment.contains(ip)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Lan lan = (Lan) o;

        if (id != lan.id) {
            return false;
        }
        if (!segments.equals(lan.segments)) {
            return false;
        }
        return Objects.equals(name, lan.name);

    }

    @Override
    public int hashCode() {
        int result = segments.hashCode();
        result = 31 * result + id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(100);
        int count = 0;
        for (Segment segment : segments) {
            if (count++ > 0) {
                builder.append(';');
            }
            builder.append(segment.toString());
        }
        return builder.toString();
    }
}