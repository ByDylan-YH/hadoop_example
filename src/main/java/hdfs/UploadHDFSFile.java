package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
/**
 * Author:BYDylan
 * Date:2020/5/1
 * Description:上传HDFS文件
 */
class UploadHDFSFile {
    public static void main(String[] args) {
        FileSystem fs = null;
        BufferedInputStream in = null;
        OutputStream out = null;
        try {
            fs = FileSystem.get(new URI("hdfs://192.168.1.201:9000"), new Configuration());
            in = new BufferedInputStream(new FileInputStream("E:\\README.txt"));
            out = fs.create(new Path("/data/README.txt"));
            byte[] buffer = new byte[1024];
            int len;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            try {
                out.flush();
                fs.close();
                out.close();
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
