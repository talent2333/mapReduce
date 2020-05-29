package flowTotal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/22 11:02
 */
public class FlowCombiner  extends Reducer<FlowBean,Text,FlowBean, Text> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Text v = new Text();
        for (Text value : values) {
            v.set("myphone:"+value.toString());
            context.write(key,v);
;        }

    }
}
