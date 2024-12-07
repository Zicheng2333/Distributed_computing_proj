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
import utils.TimeWindowUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MultiStageJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            System.err.println("Usage: MultiStageJob <orderInputPath> <tradeInputPath> <intermediateOutputPath1> <intermediateOutputPath2> <intermediateOutputPath3> <finalOutputPath> <securityID> <circulatingStock> <interval>");
            System.exit(-1);
        }

        // 获取参数
        String orderInputPath = args[0];
        String tradeInputPath = args[1];
        String intermediateOutputPath1 = args[2];
        String intermediateOutputPath2 = args[3];
        String intermediateOutputPath3 = args[4];
        String finalOutputPath = args[5];
        String securityID = args[6];
        String circulatingStock = args[7];
        String interval = args[8];

        List<String> timeWindows = TimeWindowUtils.generateTimeWindows(interval);

        // Step 1: Job to process order data
        Configuration conf1 = new Configuration();
        conf1.set("securityID", securityID);
        Job job1 = Job.getInstance(conf1, "Process Order Data and Trade Data");
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

        // 在进入 timeWindows 循环前，将中间输出的文件作为缓存文件收集起来
        Configuration cacheConf = new Configuration();
        FileSystem fs = FileSystem.get(cacheConf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(intermediateOutputPath1));
        List<URI> cacheFiles = new ArrayList<>();
        for (FileStatus status : fileStatuses) {
            String filename = status.getPath().getName();
            if (filename.startsWith("part-")) {
                cacheFiles.add(status.getPath().toUri());
            }
        }

        // 使用线程池并行执行各个 timeWindow 的任务
        // 这里以 FixedThreadPool 为例，可根据实际需求调节线程数
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(timeWindows.size(), 4));
        List<Future<Boolean>> futures = new ArrayList<>();

        for (String timeWindow : timeWindows) {
            Callable<Boolean> task = () -> {
                try {
                    System.out.println("##########################processing time window:" + timeWindow);

                    String[] windowTimes = timeWindow.split("-");
                    String windowStart = windowTimes[0];
                    String windowEnd = windowTimes[1];

                    if (windowEnd.equals("20190102113000000")){
                        windowEnd = "20190102113000001";
                    } else if(windowEnd.equals("20190102150000000")){
                        windowEnd ="20190102150000001";
                    }

                    String windowOutputPath = intermediateOutputPath2 + "/" + timeWindow.replace("-", "_");

                    // Job 2
                    Configuration conf2 = new Configuration();
                    conf2.set("securityID",securityID);
                    conf2.set("timeStart", windowStart);
                    conf2.set("timeEnd", windowEnd);

                    Job job2 = Job.getInstance(conf2, "Process Trade Data - Window " + timeWindow);
                    job2.setJarByClass(MultiStageJob.class);
                    job2.setMapperClass(TradeMapper.class);
                    job2.setReducerClass(TradeReducer.class);
                    job2.setMapOutputKeyClass(NullWritable.class);
                    job2.setMapOutputValueClass(Text.class);
                    job2.setOutputKeyClass(NullWritable.class);
                    job2.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job2, new Path(tradeInputPath));
                    FileOutputFormat.setOutputPath(job2, new Path(windowOutputPath));

                    // 为 job2 添加缓存文件（使用同一份缓存文件，不再重复遍历）
                    for (URI uri : cacheFiles) {
                        job2.addCacheFile(uri);
                    }

                    if (!job2.waitForCompletion(true)) {
                        System.err.println("Job 2 failed for window: " + timeWindow);
                        return false;
                    }

                    // Job 3
                    Configuration conf3 = new Configuration();
                    conf3.set("circulatingStock", circulatingStock);

                    Job job3 = Job.getInstance(conf3, "Sum and Label Data - Window " + timeWindow);

                    job3.setJarByClass(MultiStageJob.class);
                    job3.setMapperClass(SumMapper.class);
                    job3.setReducerClass(SumThenLabelReducer.class);
                    job3.setMapOutputKeyClass(Text.class);
                    job3.setMapOutputValueClass(Text.class);
                    job3.setOutputKeyClass(Text.class);
                    job3.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job3, new Path(windowOutputPath));
                    String job3OutputPath = intermediateOutputPath3 + "/" + timeWindow.replace("-", "_");
                    FileOutputFormat.setOutputPath(job3, new Path(job3OutputPath));

                    if (!job3.waitForCompletion(true)) {
                        System.err.println("Job 3 failed for window: " + timeWindow);
                        return false;
                    }

                    // Job 4
                    Configuration conf4 = new Configuration();
                    conf4.set("timeStart", windowStart);
                    conf4.set("timeEnd", windowEnd);

                    Job job4 = Job.getInstance(conf4, "Final Calculation - Window " + timeWindow);

                    job4.setJarByClass(MultiStageJob.class);
                    job4.setMapperClass(FinalCalculationMapper.class);
                    job4.setReducerClass(FinalCalculationReducer.class);
                    job4.setMapOutputKeyClass(Text.class);
                    job4.setMapOutputValueClass(Text.class);
                    job4.setOutputKeyClass(Text.class);
                    job4.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job4, new Path(job3OutputPath));
                    String job4OutputPath = finalOutputPath + "/" + timeWindow.replace("-", "_");
                    FileOutputFormat.setOutputPath(job4, new Path(job4OutputPath));

                    if (!job4.waitForCompletion(true)) {
                        System.err.println("Job 4 failed for window: " + timeWindow);
                        return false;
                    }

                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            };
            futures.add(executorService.submit(task));
        }

        // 等待所有任务结束
        executorService.shutdown();
        boolean allSuccess = true;
        for (Future<Boolean> future : futures) {
            if (!future.get()) {
                allSuccess = false;
            }
        }

        if (!allSuccess) {
            System.err.println("Some window tasks failed.");
            System.exit(1);
        }

        System.exit(0);
    }
}