package cn.gitos.aurthur.mr.dedup;

import cn.gitos.aurthur.base.BaseDriver;
import cn.gitos.aurthur.base.HadoopUtil;
import cn.gitos.aurthur.base.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Aurthur on 2017/1/13.
 * 去重处理
 */
public class DeDup {
    private static Text null_data = new Text("");

    //map将输入中的value复制到输出数据的key上，并直接输出
    private static class DeDupMapper extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();//每行数据
        private static Text data = new Text();


        //实现map函数
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            line = value;
            data.set(line.toString().replace("\t", "").replace(" ", ""));
            context.write(data, null_data);
        }
    }

    //reduce将输入中的key复制到输出数据的key上，并直接输出
    private static class DeDupReducer extends Reducer<Text, Text, Text, Text> {
        //实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, null_data);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HadoopUtil.getConfiguration();
        String inPath = "hdfs://node1:9000/data/dedup/";
        String outPath = "hdfs://node1:9000/output/dedup/DeDup";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "DeDup", DeDup.class
                , null, DeDup.DeDupMapper.class, Text.class, Text.class, null, null
                , DeDup.DeDupReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
