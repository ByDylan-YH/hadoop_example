package mapreduce.distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author:BY
 * Date:2019/3/13
 * Description:
 */
class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key3, Iterable<NullWritable> values3, Context context) throws IOException, InterruptedException {
        //combiner自带去重功能,所以这里直接写出
        context.write(key3, NullWritable.get());
    }
}
