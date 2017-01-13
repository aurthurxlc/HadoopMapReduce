package cn.gitos.aurthur.mr.score;

import cn.gitos.aurthur.base.BaseDriver;
import cn.gitos.aurthur.base.HadoopUtil;
import cn.gitos.aurthur.base.JobInitModel;
import cn.gitos.aurthur.mr.BaseMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Aurthur on 2017/1/13.
 * 简单的计算平均成绩示例，Map 利用 WordCount 思想
 */
public class Score implements BaseMapReduce {
    private static class ScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        int score = 0;

        // 实现map函数
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 将输入的纯文本文件的数据转化成String
            String line = value.toString();
            // 将输入的数据首先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
            // 分别对每一行进行处理
            while (tokenizerArticle.hasMoreElements()) {
                // 每行按空格划分
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                String strName = tokenizerLine.nextToken();// 学生姓名部分
                String strScore = tokenizerLine.nextToken();// 成绩部分
                Text name = new Text(strName);
                score = Integer.parseInt(strScore);
                // 输出姓名和成绩
                context.write(name, new IntWritable(score));
            }
        }
    }

    private static class ScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        int average = 0;

        // 实现reduce函数
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();// 计算总分
                count++;// 统计总的科目数
            }
            average = sum / count;// 计算平均成绩
            context.write(key, new IntWritable(average));
        }
    }

    public void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HadoopUtil.getConfiguration();
        String inPath = "hdfs://node1:9000/data/score";
        String outPath = "hdfs://node1:9000/output/score/Score/";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "Score", Score.class
                , null, Score.ScoreMapper.class, Text.class, IntWritable.class, null, null
                , Score.ScoreReducer.class, Text.class, IntWritable.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
