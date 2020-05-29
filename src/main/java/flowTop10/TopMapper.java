package flowTop10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 22:24
 */
public class TopMapper extends Mapper<LongWritable, Text, Text, TopBean> {

    private Text k = new Text();
    private TopBean v = new TopBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        int len = fields.length;
        long totalFlow = Long.parseLong(fields[len - 3]) + Long.parseLong(fields[len - 2]);
        String phone = fields[1];
        k.set(phone);
        v.setTotalFlow(totalFlow);

        context.write(k, v);
    }

}
