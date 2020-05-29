package flowStatistics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/20 19:41
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    private Text k = new Text();
    private FlowBean v =new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        int len = fields.length;
        //设置手机号码
        k.set(fields[1]);
        //设置上行和下行数据封装到对象FlowBean
        v.setUpFlow(Long.parseLong(fields[len-3]));
        v.setDownFlow(Long.parseLong(fields[len-2]));
        context.write(k,v);
    }
}
