package io.journalkeeper.core.persistence.remote;

import java.nio.file.FileSystem;
import java.util.Properties;

/**
 * 分布式文件系统
 * @author LiYue
 * Date: 2020/5/12
 */
public interface DistributedFileSystem {
    FileSystem getFileSystem(Properties properties);
}
