package cn.gitos.aurthur.mr.sort;

import cn.gitos.aurthur.base.BaseDriver;
import cn.gitos.aurthur.base.HadoopUtil;
import cn.gitos.aurthur.base.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Aurthur on 2017/1/13.
 * 排序示例
 */
public class Sort {
    //map将输入中的value化成IntWritable类型，作为输出的key
    private static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable();
        private static IntWritable one = new IntWritable(1);

        //实现map函数
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, one);
        }
    }

    //reduce将输入中的key复制到输出数据的key上，
    //然后根据输入的value-list中元素的个数决定key的输出次数
    //用全局line_num来代表key的位次
    private static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable line_num = new IntWritable(1);

        //实现reduce函数
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(line_num, key);
                line_num.set(line_num.get() + 1);
            }
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HadoopUtil.getConfiguration();
        String inPath = "hdfs://node1:9000/data/sort";
        String outPath = "hdfs://node1:9000/output/sort/Sort/";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "Sort", Sort.class
                , null, Sort.SortMapper.class, IntWritable.class, IntWritable.class, null, null
                , Sort.SortReducer.class, IntWritable.class, IntWritable.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}