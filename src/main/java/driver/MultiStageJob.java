package driver;

import mapper.OrderMapper;
import mapper.TradeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.TradeReducer;

public class MultiStageJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            System.err.println("Usage: MultiStageJob <orderInputPath> <tradeInputPath> <intermediateOutputPath> <finalOutputPath> <securityID> <timeStart> <timeEnd>"); //TODO 目前只设计了trade和offer各只有一个路径的情况，如果需要读取上午和下午的文件只需要稍作修改即可
            System.exit(-1);
        }

        // 设置参数
        String orderInputPath = args[0];
        String tradeInputPath = args[1];
        String intermediateOutputPath = args[2];
        String finalOutputPath = args[3];
        String securityID = args[4];
        String timeStart = args[5];
        String timeEnd = args[6];

        // Step 1: Job to process order data
        Configuration conf1 = new Configuration();
        conf1.set("securityID", securityID);

        Job job1 = Job.getInstance(conf1, "Process Order Data");
        job1.setJarByClass(MultiStageJob.class);
        job1.setMapperClass(OrderMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job1, new Path(orderInputPath));
        FileOutputFormat.setOutputPath(job1, new Path(intermediateOutputPath));

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 failed");
            System.exit(1);
        }
        //第一个任务执行完成才会开始执行第二个任务

        // Step 2: Job to process trade data with order data
        Configuration conf2 = new Configuration();
        conf2.set("securityID",securityID);
        conf2.set("timeStart",timeStart);
        conf2.set("timeEnd",timeEnd);

        Job job2 = Job.getInstance(conf2, "Process Trade Data");

        job2.setJarByClass(MultiStageJob.class);
        job2.setMapperClass(TradeMapper.class);
        job2.setReducerClass(TradeReducer.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(tradeInputPath));
        FileOutputFormat.setOutputPath(job2, new Path(finalOutputPath));

        FileSystem fs = FileSystem.get(conf2);
        FileStatus [] fileStatuses = fs.listStatus(new Path(intermediateOutputPath)); //列出中间路径的文件
        for (FileStatus status : fileStatuses){
            String filename = status.getPath().getName();
            if (filename.startsWith("part-")){
                job2.addCacheFile(status.getPath().toUri());
            }
        }

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        System.exit(0);
    }
}