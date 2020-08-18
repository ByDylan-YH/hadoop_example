package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Author:BYDylan
 * Date:2020/5/1
 * Description:HDFS合并小文件
 */
class HDFSMerge {
    private static final String HDFS_PATH = "hdfs://192.168.100.109:8020";

    public static void main(String[] args) {
        Configuration configuration;
        FileSystem fs;
        FSDataOutputStream os;
        LocalFileSystem local;
        FileStatus[] fileStatuses;
        try {
            configuration = new Configuration();
            fs = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
            os = fs.create(new Path("/aaaa/1234.txt"));
            local = FileSystem.getLocal(configuration);
            fileStatuses = local.listStatus(new Path("file:/H:\\网易云音乐"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus.getPath());
                FSDataInputStream is = local.open(fileStatus.getPath());
                IOUtils.copyBytes(is, os, 4096);
                os.flush();
                is.close();
            }
            os.close();
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
