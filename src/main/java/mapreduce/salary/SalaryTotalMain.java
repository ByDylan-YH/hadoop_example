package mapreduce.salary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalaryTotalMain {
    public static void main(String[] args) throws Exception {
        //1、创建一个任务
        Job job = Job.getInstance(new Configuration());
        //指定任务的入口(程序的入口)
        //hadoop jar 1.jar /HDFS的路径 ---args[0]    / HDFS的路径---args[1]    主程序的类名
        job.setJarByClass(SalaryTotalMain.class);

        //2、指定任务的map
        job.setMapperClass(SalaryTotalMapper.class);
        //指定map的输出数据类型
        job.setMapOutputKeyClass(IntWritable.class);//k2的类型
        job.setMapOutputValueClass(IntWritable.class);//v2的类型

        //3、指定任务的reducer
        job.setReducerClass(SalaryTotalReducer.class);
        //指定reducer的输出数据类型
        job.setOutputKeyClass(IntWritable.class);//k4的类型
        job.setOutputValueClass(IntWritable.class);//v4的类型

        //4、指定任务的输入和输出路径
        //任务是job，给任务设置输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));//输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出路径

        //5、执行任务
        job.waitForCompletion(true);
    }
}
