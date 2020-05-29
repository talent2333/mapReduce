package indexSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 19:22
 */
public class IndexReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private int sum;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        for (IntWritable value : values) {
            sum++;
        }
        context.write(key, new IntWritable(sum));
        sum = 0;
    }
}
