package groupingComparatorOrder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/23 15:18
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        OrderBean bean = new OrderBean();

        bean.setId(Integer.parseInt(fields[0]));
        bean.setPrice(Double.parseDouble(fields[1]));

        context.write(bean,NullWritable.get());

    }
}
