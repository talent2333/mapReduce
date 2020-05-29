package commonFriends;

import com.sun.xml.internal.ws.org.objectweb.asm.MethodAdapter;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * @Description TODO
 * @Author talent2333
 * @Date 2020/5/27 22:52
 */
public class FriendReducer extends Reducer<Text, Text,Text, NullWritable> {

    private Text k = new Text();
    //那些人有好友
    private TreeSet<String> commonTree = new TreeSet<>();

    /**
     * @Description
     * @param key       共同好友
     * @param values    我是谁
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        TreeSet<String> peopleTree = new TreeSet<>();
        //人人人
        for (Text value : values) {
            String me = value.toString();
            commonTree.add(me);
            peopleTree.add(me);
        }
        //共同好友的个数统计
        int length = key.toString().split(" ").length;
        //人人人
        StringBuilder sb = new StringBuilder();
        peopleTree.stream().forEach(o->sb.append(o).append(" "));
        String people = sb.toString().trim();

        StringBuilder k_sb = new StringBuilder();
//        k_sb.append(key.toString()).append("是").append(people).append("的共同好友");
        k_sb.append(people).append("都有")
                .append(length).append("个共同好友: ")
                .append(key.toString());

        k.set(k_sb.toString());
        context.write(k,NullWritable.get());

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        Iterator<String> iter = commonTree.iterator();
        while (iter.hasNext()){
            String next = iter.next();
            sb.append(next).append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(" has common friends!");
        k.set(sb.toString());

        context.write(k,NullWritable.get());
    }

    @Test
    public void test1(){
        StringBuilder sb = new StringBuilder("abc");
        sb.append("def");
        sb.deleteCharAt(sb.length()-1);
        System.out.println(sb);
    }
}
