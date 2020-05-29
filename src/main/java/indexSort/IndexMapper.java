package indexSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 19:22
 */
public class IndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String name;
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException {

        FileSplit fs = (FileSplit) context.getInputSplit();
        name = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");
        String str = "";
        for (String field : fields) {
            if ("atguigu".equals(field)) {
                str = field + "--" + name;
            } else if ("pingping".equals(field)) {
                str = field + "--" + name;
            } else if ("ss".equals(field)) {
                str = field + "--" + name;
            } else {
                continue;
            }
            k.set(str);
            context.write(k, v);
        }

    }
}
