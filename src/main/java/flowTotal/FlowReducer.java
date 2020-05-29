package flowTotal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/21 17:14
 */
public class FlowReducer extends Reducer<FlowBean, Text,Text,LongWritable> {

    @Override
    /**
     * @Description 规约
     * @param key   流量对象(sorted)
     * @param values    手机号码
     * @param context   输出(手机号码，流量对象)
     * @Return void
     * @Date 2020/5/21 17:52
     */
    protected void reduce(FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        LongWritable k = new LongWritable();
        for (Text value : values) {

            long k_tmp = key.getSumFlow();
            k.set(k_tmp);

            context.write(value, k);
        }
    }
}
