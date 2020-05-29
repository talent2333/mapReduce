package reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Description method 1
 * @Author talent2333
 * @Date 2020/5/26 9:09
 */
public class JoinReducer extends Reducer<JoinBean, NullWritable,JoinBean,NullWritable> {

    @Override
    protected void reduce(JoinBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

            Iterator<NullWritable> iters = values.iterator();
            iters.next();
            String name = key.getName();
            while (iters.hasNext()){
                iters.next();
                key.setName(name);
                context.write(key,NullWritable.get());
            }
    }
}
