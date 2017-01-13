package cn.gitos.aurthur.mr.invertedindex;

import cn.gitos.aurthur.base.BaseDriver;
import cn.gitos.aurthur.mr.BaseMapReduce;
import cn.gitos.aurthur.base.HadoopUtil;
import cn.gitos.aurthur.base.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Aurthur on 2017/1/12.
 * 倒排索引示例
 */
public class InvertedIndex implements BaseMapReduce {
    private static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            //FileSplit类从context上下文中得到，可以获得当前读取的文件的路径
            FileSplit fileSplit = (FileSplit) context.getInputSplit();

            //根据/分割取最后一块即可得到当前的文件名
            String[] fileNames = fileSplit.getPath().toString().split("/");
            String fileName = fileNames[fileNames.length - 1];
            for (String d : data) {
                k.set(d + "->" + fileName);
                v.set("1");
                context.write(k, v);
            }
        }
    }

    private static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void reduce(
                Text key,
                java.lang.Iterable<Text> values,
                org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            //分割文件名和单词
            String[] wordAndPath = key.toString().split("->");
            //统计出现次数
            int counts = 0;
            for (Text t : values) {
                counts += Integer.parseInt(t.toString());
            }
            //组成新的key-value输出
            k.set(wordAndPath[0]);
            v.set(wordAndPath[1] + "->" + counts);
            context.write(k, v);
        }
    }

    private static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        private Text v = new Text();

        protected void reduce(
                Text key,
                java.lang.Iterable<Text> values,
                org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String res = "";
            for (Text text : values) {
                res += text.toString() + "\r";
            }
            v.set(res);
            context.write(key, v);
        }
    }

    public void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HadoopUtil.getConfiguration();
        String inPath = "hdfs://node1:9000/data/test.file";
        String outPath = "hdfs://node1:9000/output/test.file/InvertedIndex/";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "InvertedIndex", InvertedIndex.class
                , null, InvertedIndexMapper.class, Text.class, Text.class, null, InvertedIndexCombiner.class
                , InvertedIndexReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}