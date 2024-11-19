package driver;

import mapper.FinalCalculationMapper;
import mapper.OrderMapper;
import mapper.SumMapper;
import mapper.TradeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.FinalCalculationReducer;
import reducer.SumThenLabelReducer;
import reducer.TradeReducer;

public class MultiStageJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 10) {
            System.err.println("Usage: MultiStageJob <orderInputPath> <tradeInputPath> <intermediateOutputPath1> <intermediateOutputPath2> <intermediateOutputPath3> <finalOutputPath> <securityID> <circulatingStock> <timeStart> <timeEnd> "); //TODO 目前只设计了trade和offer各只有一个路径的情况，如果需要读取上午和下午的文件只需要稍作修改即可
            System.exit(-1);
        }

        // 设置参数
        String orderInputPath = args[0];
        String tradeInputPath = args[1];
        String intermediateOutputPath1 = args[2];
        String intermediateOutputPath2 = args[3];
        String intermediateOutputPath3 = args[4];
        String finalOutputPath = args[5];
        String securityID = args[6];
        String circulatingStock = args[7];
        String timeStart = args[8];
        String timeEnd = args[9];


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
        FileOutputFormat.setOutputPath(job1, new Path(intermediateOutputPath1));

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
        FileOutputFormat.setOutputPath(job2, new Path(intermediateOutputPath2));

        FileSystem fs = FileSystem.get(conf2);
        FileStatus [] fileStatuses = fs.listStatus(new Path(intermediateOutputPath1)); //列出中间路径的文件
        for (FileStatus status : fileStatuses){
            String filename = status.getPath().getName();
            if (filename.startsWith("part-")){
                job2.addCacheFile(status.getPath().toUri());
            }
        }

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        // Step 3: Job to process summation output with SumMapper and SumThenLabelReducer
        Configuration conf3 = new Configuration();
        conf3.set("circulatingStock", circulatingStock);

        Job job3 = Job.getInstance(conf3, "Sum and Label Data");

        job3.setJarByClass(MultiStageJob.class);
        job3.setMapperClass(SumMapper.class);
        job3.setReducerClass(SumThenLabelReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(intermediateOutputPath2));
        FileOutputFormat.setOutputPath(job3, new Path(intermediateOutputPath3));

        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }

        // Step 3: Job to process final output with FinalCalculationMapper and FinalCalculationReducer
        Configuration conf4 = new Configuration();
        conf3.set("timeStart", timeStart);
        conf3.set("timeEnd", timeEnd);

        Job job4 = Job.getInstance(conf4, "Final Calculation");

        job4.setJarByClass(MultiStageJob.class);
        job4.setMapperClass(FinalCalculationMapper.class);
        job4.setReducerClass(FinalCalculationReducer.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);



        FileInputFormat.addInputPath(job4, new Path(intermediateOutputPath3));
        FileOutputFormat.setOutputPath(job4, new Path(finalOutputPath));

        if (!job4.waitForCompletion(true)) {
            System.exit(1);
        }




        System.exit(0);
    }
}