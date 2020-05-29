package mapJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

/**
 * @Description MapJoin:在处理大表之前，维护小表数据到内存中
 * @Author talent2333
 * @Date 2020/5/26 11:44
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdMap = new HashMap<>();

    private Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //load cache file
        URI[] cacheFiles = context.getCacheFiles();
        URI cacheFile = cacheFiles[0];
        //read file
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(cacheFile));
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        try {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split("\t");
                //put ur cache file's key/value into pdMap
                pdMap.put(fields[0], fields[1]);

            }
        } catch (IOException e) {
            // ... handle IO exception
        } finally {
            IOUtils.closeStream(br);
            IOUtils.closeStream(in);
            IOUtils.closeStream(fs);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //计数器
        context.getCounter("counterGroup","counter").increment(1);

        String[] fields = value.toString().split("\t");
        //join operation : name ==> pid
        fields[1] = pdMap.get(fields[1]);
        String result = "id:" + fields[0] + "\tpid:" + fields[1] + "\tname:" + fields[2];

        outK.set(result);
        context.write(outK,NullWritable.get());

    }
}
