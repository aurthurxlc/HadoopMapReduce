package cn.gitos.aurthur.mr.tablejoin;

import cn.gitos.aurthur.base.BaseDriver;
import cn.gitos.aurthur.base.HadoopUtil;
import cn.gitos.aurthur.base.JobInitModel;
import cn.gitos.aurthur.mr.BaseMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Aurthur on 2017/1/13.
 * 表单关联示例
 */
public class TableJoin implements BaseMapReduce {
    private static int time = 0;

    /*
     * map将输出分割child和parent，然后正序输出一次作为右表
     * 反序输出一次作为左表，需要注意的是在输出的value中必须加上左右表的区别标识
     */
    private static class TableJoinMapper extends Mapper<Object, Text, Text, Text> {
        String child_name;// 孩子名称
        String parent_name;// 父母名称
        String relation_type;// 左右表标识

        // 实现map函数
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 输入的一行预处理文本
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] values = new String[2];
            int i = 0;
            while (itr.hasMoreTokens()) {
                values[i] = itr.nextToken();
                i++;
            }
            if (values[0].compareTo("child") != 0) {
                child_name = values[0];
                parent_name = values[1];
                // 输出左表
                relation_type = "1";
                context.write(new Text(parent_name), new Text(relation_type +
                        "+" + child_name + "+" + parent_name));
                // 输出右表
                relation_type = "2";
                context.write(new Text(child_name), new Text(relation_type +
                        "+" + child_name + "+" + parent_name));
            }
        }
    }

    private static class TableJoinReducer extends Reducer<Text, Text, Text, Text> {
        int grandchild_num = 0;
        int grandparent_num = 0;
        String child_name;
        String parent_name;
        char relation_type;

        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            grandchild_num = 0;
            grandparent_num = 0;
            // 输出表头
            if (0 == time) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }

            String[] grandchild = new String[10];
            String[] grandparent = new String[10];

            for (Text val : values) {
                child_name = "";
                parent_name = "";

                String record = val.toString();
                int len = record.length();
                int i = 2;
                if (0 == len) {
                    continue;
                }
                // 取得左右表标识
                relation_type = record.charAt(0);
                // 定义孩子和父母变量

                // 获取 value-list 中 value 的 child_name
                while (record.charAt(i) != '+') {
                    child_name += record.charAt(i);
                    i++;
                }
                i = i + 1;
                // 获取 value-list 中 value 的 parent_name
                while (i < len) {
                    parent_name += record.charAt(i);
                    i++;
                }

                // 左表，取出 child 放入 grandchildren
                if ('1' == relation_type) {
                    grandchild[grandchild_num] = child_name;
                    grandchild_num++;
                }

                // 右表，取出 parent 放入 grandparent
                if ('2' == relation_type) {
                    grandparent[grandparent_num] = parent_name;
                    grandparent_num++;
                }
            }

            // grandchild和grandparent数组求笛卡尔儿积
            if (0 != grandchild_num && 0 != grandparent_num) {
                for (int m = 0; m < grandchild_num; m++) {
                    for (int n = 0; n < grandparent_num; n++) {
                        // 输出结果
                        if (grandchild[m] != null && grandparent[n] != null) {
                            context.write(new Text(grandchild[m]), new Text(grandparent[n]));
                        }
                    }
                }
            }
        }
    }

    public void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HadoopUtil.getConfiguration();
        String inPath = "hdfs://node1:9000/data/tablejoin";
        String outPath = "hdfs://node1:9000/output/tablejoin/TableJoin/";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "TableJoin", TableJoin.class
                , null, TableJoin.TableJoinMapper.class, Text.class, Text.class, null, null
                , TableJoin.TableJoinReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
