package indexSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/26 20:18
 */
public class IndexComparator extends WritableComparator {

    protected IndexComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        Text ta = (Text) a;
        Text tb = (Text) b;
        String sub1 = ta.toString().substring(0, 2);
        String sub2 = tb.toString().substring(0, 2);
        return sub1.compareTo(sub2);

    }
}
