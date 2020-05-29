package OutputTest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/24 11:37
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String line = key.toString();
        line = line + "\r\n";

        k.set(line);
        for (NullWritable value : values) {
            context.write(k, NullWritable.get());
        }
    }
}
