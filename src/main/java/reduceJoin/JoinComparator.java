package reduceJoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description KEY改为pid
 * @Author talent2333
 * @Date 2020/5/26 10:28
 */
public class JoinComparator extends WritableComparator {

    protected JoinComparator() {
        super(JoinBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        JoinBean bean1 = (JoinBean) a;
        JoinBean bean2 = (JoinBean) b;
        String pid1 = bean1.getPid();
        String pid2 = bean2.getPid();

        return pid1.compareTo(pid2);
    }
}
