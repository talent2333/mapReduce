package flowStatistics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/20 19:57
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private Long sumUpFlow;
    private Long sumDownFlow;

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        FlowBean v = new FlowBean();
        sumUpFlow = 0L;
        sumDownFlow = 0L;
        for (FlowBean value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        Text k = key;
        v.setUpFlow(sumUpFlow);
        v.setDownFlow(sumDownFlow);
        context.write(k, v);
    }
}
