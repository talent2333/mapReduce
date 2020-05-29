package logTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 14:40
 */
public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        boolean result = parseLog(line, context);

        //return when  you got illegal log
        if (!result){
            return;
        }
        k.set(line);
        context.write(k,NullWritable.get());

    }
    private boolean parseLog(String line,Context context){
        String[] fields = line.split(" ");
        if (fields.length > 11){
            context.getCounter("map","true").increment(1);
            return true;
        }else {
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}
