package commonFriends;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * @Description Myfriend
 * @Author talent2333
 * @Date 2020/5/27 22:51
 */
public class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {


    private Text k = new Text();
    private Text v1 = new Text();
    private Text v2 = new Text();
    //维护自己和好友组的kv数据
    private HashMap<String, String[]> friendMap = new HashMap<>();

    /**
     * @param key   朋友
     * @param value 自己
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] split = value.toString().split(":");
        String me = split[0];
        String[] friends = split[1].split(",");

        friendMap.put(me, friends);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        //双迭代器遍历前后两行的朋友表数据
        //首行迭代器
        Iterator<Map.Entry<String, String[]>> iter = friendMap.entrySet().iterator();
        //次行迭代器
        Iterator<Map.Entry<String, String[]>> iter2 = friendMap.entrySet().iterator();
        List<String> commonFriendList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        String key1, key2;
        int step = 0;

        while (iter.hasNext()) {
            Map.Entry<String, String[]> next1 = iter.next();
            step++;
            key1 = next1.getKey();
            String[] value1 = next1.getValue();
            for (int i = 0; i < step; i++) {
                iter2.next();
            }
            //和下一行进行比较
            while (iter2.hasNext()) {
                Map.Entry<String, String[]> next2 = iter2.next();
                key2 = next2.getKey();
                String[] value2 = next2.getValue();
                //取出当前行的朋友列表
                for (String s1 : value1) {
                    for (String s2 : value2) {
                        if (s1.equals(s2))
                            commonFriendList.add(s1);
                    }
                }
                //共同好友列表为空则跳过本次循环
                if (commonFriendList.isEmpty())
                    continue;
                //写出友人A和友人B的共同好友
                sb = new StringBuilder();
                for (String person : commonFriendList) {
                    sb.append(person).append(" ");
                }
                //被当做好友的朋友
                k.set(sb.toString().trim());
                //人人人
                v1.set(key1);
                v2.set(key2);
                context.write(k, v1);
                context.write(k, v2);
                //每次迭代都需要清空共同好友列表
                commonFriendList.clear();
                k.clear();
                v1.clear();
                v2.clear();
            }
            //重置iterator2
            iter2 = friendMap.entrySet().iterator();
        }
    }
}
