package reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description method2
 * @Author talent2333
 * @Date 2020/5/26 13:55
 */
public class JoinReducerer extends Reducer<JoinBean, NullWritable, JoinBean, NullWritable> {

    List<JoinBean> orders = new ArrayList<>();
    String name;

    @Override
    protected void reduce(JoinBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        for (NullWritable value : values) {
            if ("order".equals(key.getFlag())) {
                JoinBean newOrder = new JoinBean();
                try {
                    BeanUtils.copyProperties(newOrder, key);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
                orders.add(newOrder);
            } else if ("pd".equals(key.getFlag())) {
                name = key.getName();
            }
        }
        for (JoinBean order : orders) {
            order.setName(name);
            context.write(order, NullWritable.get());
        }
        //清空集合数据
        orders.clear();

    }
}
