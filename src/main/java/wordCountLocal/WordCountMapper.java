package wordCountLocal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/20 10:58
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v =new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        //排除空行
        if(line.trim().length()==0){
            return;
        }
        //排除多空格
        String[] words = line.split("\\s+");
        for (String word : words) {
            k.set(word);
            context.write(k,v);
        }

    }
}
