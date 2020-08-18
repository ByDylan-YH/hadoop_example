package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
/**
 * Author:BYDylan
 * Date:2020/5/1
 * Description:下载HDFS文件
 */
class DownloadHDFSFile {
    public static void main(String[] args) {
        FileSystem fs = null;
        FSDataInputStream fin = null;
        OutputStream out = null;
        try {
            fs = FileSystem.get(new URI("hdfs://192.168.1.201:9000"), new Configuration());
            fin = fs.open(new Path("/data/test.txt"));
            out = new FileOutputStream("E://test.txt");
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fin.read(buffer)) > 0) {
            //读取数据
            out.write(buffer, 0, len);
        }
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            try {
                out.flush();
                out.close();
                fin.close();
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
