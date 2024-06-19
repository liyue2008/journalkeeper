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
package io.journalkeeper.persistence.local.journal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

@SuppressWarnings("UnusedReturnValue")
public interface StoreFile extends Timed {

    /**
     * 对应的文件
     * @return StoreFile对应的文件
     */
    File file();

    /**
     * 文件起始全局位置
     * @return 文件起始全局位置
     */
    long position();

    /**
     * 卸载文件缓存页
     * @return 成功返回true，如果文件存在脏数据返回是吧
     */
    boolean unload();

    /**
     * 强制接卸，丢弃可能存在的脏数据
     */
    void forceUnload();

    /**
     * 是否有缓存页
     * @return 是否有缓存页
     */
    boolean hasPage();

    /**
     * 用给定的位置和长度读取数据
     * @param position 文件内的相对位置
     * @param length 数据长度，当长度小于0时则自动判断数据长度
     * @return 读取出来的buffer
     * @throws IOException 发生IO异常时抛出
     */
    ByteBuffer read(int position, int length) throws IOException;

    /**
     * 写入一段ByteBuffer，保证原子性
     * @param buffer 待写入的buffer
     * @return 写入之后文件的全局写入位置
     * @throws IOException 发生IO异常时抛出
     */
    int append(ByteBuffer buffer) throws IOException;

    /**
     * 写入一段ByteBuffer，保证原子性
     * @param buffer 待写入的buffer
     * @return 写入之后文件的全局写入位置
     * @throws IOException 发生IO异常时抛出
     */
    int append(List<ByteBuffer> buffer) throws IOException;


    /**
     * 将内存中的数据写入磁盘中
     * @return 本次写入数据的大小
     * @throws IOException 发生IO异常时抛出
     */
    int flush() throws IOException;

    /**
     * 回滚到指定位置，未刷盘的数据直接丢弃，已刷盘的数据需要截断。
     * @param position 回滚位置
     * @throws IOException 发生IO异常时抛出
     */
    void rollback(int position) throws IOException;

    /**
     * 内存中的数据是否和磁盘一致
     * @return true: 一致，false：不一致
     */
    boolean isClean();

    /**
     * 写入位置
     * @return 写入位置
     */
    int writePosition();

    /**
     * 文件中数据大小（不含文件头）
     * @return 文件中数据大小
     */
    int fileDataSize();

    /**
     * 刷盘位置
     * @return 刷盘位置
     */
    int flushPosition();

    /**
     * 文件创建时间
     * @return 文件创建时间
     */
    long timestamp();

    int size();

    /**
     * 结束写入，文件变为只读。
     */
    void closeWrite();

    /**
     * 强制把PageBuffer的数据写入磁盘
     * @throws IOException 发生IO异常时抛出
     */
    void force() throws IOException;

    Long readLong(int position) throws IOException;
}
