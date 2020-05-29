package groupingComparatorOrder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/23 15:24
 */
public class OrderWritableComparator extends WritableComparator {

    protected OrderWritableComparator() {
        super(OrderBean.class, true);
    }

    /**
     * @Description 辅助分组
     * @param a
     * @param b
     * @Return int
     * @Date 2020/5/23 17:08
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        if (aBean.getId() > bBean.getId()) {
            return 1;
        } else if (aBean.getId() < bBean.getId()) {
            return -1;
        } else {
            return 0;
        }
    }
}
