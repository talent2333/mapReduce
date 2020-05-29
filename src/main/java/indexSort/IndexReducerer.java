package indexSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 20:10
 */
public class IndexReducerer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private Text k = new Text();

    /**
     * @param key     ss--a.txt
     * @param values  3
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        StringBuilder newLine = new StringBuilder();
        String str = new String();

        for (IntWritable value : values) {
            //get fileName's suffix --> e.g. a.txt
            String strMid = key.toString().split("--")[1];
            //set the middle of the final string
            str = strMid + "-->" + value.toString() + " ";
            newLine.append(str);
        }

        //get the keyword
        String prefixStr = key.toString().split("--")[0] + "\t";
        newLine.insert(0, prefixStr);

        k.set(newLine.toString());
        context.write(k, NullWritable.get());

    }
}
