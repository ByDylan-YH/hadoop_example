package mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {
    public static void main(String[] args) throws Exception {
//        System.out.println("输入参数:" + args[0] + " , " + args[1]);
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://192.168.1.201:9000");
        //1.创建一个任务
        Job job = Job.getInstance(conf);
        //指定任务的入口
        //hadoop jar 1.jar /HDFS路径 --args[0]  / HDFS的路径---args[1]    主程序的类名
        job.setJarByClass(WordCountMain.class);

        //2.指定任务的map
        job.setMapperClass(WordCountMapper.class);
        //指定map的输出数据类型
        job.setMapOutputKeyClass(Text.class);//k2类型
        job.setMapOutputValueClass(IntWritable.class);//v2类型

        //3.指定任务的reduce
        job.setReducerClass(WordCountReducer.class);
        //指定reduce的输出数据类型
        job.setOutputKeyClass(Text.class);//k4的类型
        job.setOutputValueClass(IntWritable.class);//v4的类型

        //4.指定任务的输入输出路径
        ///HDFS路径 --args[0]  / HDFS的路径---args[1]    主程序的类名
        FileInputFormat.setInputPaths(job, new Path(args[0]));//输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出路径

        //5.最后执行任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
