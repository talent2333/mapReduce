package flowTop10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 22:24
 */
public class TopReducer extends Reducer<Text, TopBean, Text, LongWritable> {

    private TreeMap<Long, String> map = new TreeMap<>();
    private int num;
    private Text k = new Text();
    private LongWritable v = new LongWritable();

    /**
     * @param key     phone
     * @param values  All amout of flow
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<TopBean> values, Context context)
            throws IOException, InterruptedException {
        Long firstKey = 0L;
        for (TopBean value : values) {
            if (num < 10) {
                map.put(value.getTotalFlow(), key.toString());
                num++;
            }
            if (num == 10 && (firstKey = map.firstKey()) < value.getTotalFlow()) {
                map.remove(firstKey);
                map.put(value.getTotalFlow(), key.toString());
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Iterator<Map.Entry<Long, String>> iter = map.entrySet().iterator();
        while (iter.hasNext()){
            Map.Entry<Long, String> next = iter.next();
//          key->totalFlow   value->phone
            Long key = next.getKey();
            String value = next.getValue();

            k.set(value);
            v.set(key);
            context.write(k, v);
        }
    }

}
