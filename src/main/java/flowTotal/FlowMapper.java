package flowTotal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/21 17:14
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean k;
    private Text v = new Text();

    /**
     * @Description 映射
     * @param key   流量对象(sorted)
     * @param values    手机号码
     * @param context   输出(手机号码，流量对象)
     * @Return void
     * @Date 2020/5/21 17:52
    @Override
     */
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        int len = fields.length;
        String number = fields[1];
        long upflow = Long.parseLong(fields[len-3]);
        long downflow = Long.parseLong(fields[len-2]);
        k = new FlowBean(upflow,downflow);
        v.set(number);

        context.write(k, v);
    }
    @Test
    public void test001(){
//        WritableComparator
    }
}
