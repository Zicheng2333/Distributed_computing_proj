package driver;

import mapper.*;
import reducer.FinalCalculationReducer;
import reducer.SumThenLabelReducer;
import reducer.TradeReducer;
import utils.TimeWindowUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MultiStageJob {

    private static BufferedWriter logWriter = null;

    private static void logMessage(String msg) throws IOException {
        long currentTime = System.currentTimeMillis();
        // 使用SimpleDateFormat将毫秒级时间戳转换成人类可读的日期时间
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formattedTime = sdf.format(new java.util.Date(currentTime));
        String logStr = "[" + formattedTime + "] " + msg;
        System.out.println(logStr);
        if (logWriter != null) {
            logWriter.write(logStr);
            logWriter.newLine();
            logWriter.flush();
        }
    }

    public static void main(String[] args) throws Exception {
        // 初始化日志文件
        try {
            logWriter = new BufferedWriter(new FileWriter("multi_stage_timing.log", true));
        } catch (IOException e) {
            System.err.println("Failed to open log file for writing.");
            System.exit(1);
        }

        if (args.length != 10) {
            System.err.println("Usage: MultiStageJob <orderInputPath> <tradeInputPath> <intermediateOutputPath1> "
                    + "<intermediateOutputPath2> <intermediateOutputPath3> <finalOutputPath> <securityID> "
                    + "<circulatingStock> <interval> <intermediateTradeOutputBasePath>");
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
        String intermediateTradeOutputBasePath = args[9];

        List<String> timeWindows = TimeWindowUtils.generateTimeWindows(interval);

        logMessage("Processing order data");

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
            logMessage("Job 1 failed");
            System.exit(1);
        }

        // Step 2: 预处理交易数据，将符合securityID的交易数据按时间窗口输出到对应目录
        StringBuilder sb = new StringBuilder();
        for (String tw : timeWindows) {
            sb.append(tw).append(",");
        }

        Configuration confPre = new Configuration();
        confPre.set("securityID", securityID);
        confPre.set("timeWindows", sb.toString());

        logMessage("Start preprocessing trade data");

        Job preprocessJob = Job.getInstance(confPre, "Preprocess Trade Data by Windows");
        preprocessJob.setJarByClass(MultiStageJob.class);
        preprocessJob.setMapperClass(TradePreprocessingMapper.class);
        // 无Reduce任务
        preprocessJob.setMapOutputKeyClass(Text.class);
        preprocessJob.setMapOutputValueClass(Text.class);
        preprocessJob.setOutputKeyClass(Text.class);
        preprocessJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(preprocessJob, new Path(tradeInputPath));
        FileOutputFormat.setOutputPath(preprocessJob, new Path(intermediateTradeOutputBasePath));

        if (!preprocessJob.waitForCompletion(true)) {
            logMessage("Trade Preprocessing Job failed");
            System.exit(1);
        }

        logMessage("Adding cache files");

        // 准备Cache Files
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

        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(timeWindows.size(), 6));
        List<Future<Boolean>> futures = new ArrayList<>();

        for (String timeWindow : timeWindows) {
            Callable<Boolean> task = () -> {
                try {
                    logMessage("Start processing time window: " + timeWindow);

                    String[] windowTimes = timeWindow.split("-");
                    String windowStart = windowTimes[0];
                    String windowEnd = windowTimes[1];

                    if (windowEnd.equals("20190102113000000")) {
                        windowEnd = "20190102113000001";
                    } else if (windowEnd.equals("20190102150000000")) {
                        windowEnd = "20190102150000001";
                    }

                    String windowOutputPath = intermediateOutputPath2 + "/" + timeWindow.replace("-", "_");
                    String windowDirName = "window_" + timeWindow.replace("-", "_");
                    Path windowInputPath = new Path(intermediateTradeOutputBasePath, windowDirName);

                    // Job 2
                    Configuration conf2 = new Configuration();
                    conf2.set("securityID", securityID);
                    conf2.set("timeStart", windowStart);
                    conf2.set("timeEnd", windowEnd);

                    Job job2 = Job.getInstance(conf2, "Process Trade Data - Window " + timeWindow);
                    job2.setJarByClass(MultiStageJob.class);
                    // 使用TradeMapper处理预先分好的数据
                    job2.setMapperClass(TradeMapper.class);
                    job2.setReducerClass(TradeReducer.class);
                    job2.setMapOutputKeyClass(NullWritable.class);
                    job2.setMapOutputValueClass(Text.class);
                    job2.setOutputKeyClass(NullWritable.class);
                    job2.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job2, windowInputPath);
                    FileOutputFormat.setOutputPath(job2, new Path(windowOutputPath));

                    // 为 job2 添加缓存文件
                    for (URI uri : cacheFiles) {
                        job2.addCacheFile(uri);
                    }

                    if (!job2.waitForCompletion(true)) {
                        logMessage("Job 2 failed for window: " + timeWindow);
                        return false;
                    }

                    logMessage("Start processing time window job3: " + timeWindow);

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
                        logMessage("Job 3 failed for window: " + timeWindow);
                        return false;
                    }

                    logMessage("Start processing time window job4: " + timeWindow);

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
                        logMessage("Job 4 failed for window: " + timeWindow);
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

        // 等待所有时间窗口任务结束
        executorService.shutdown();
        boolean allSuccess = true;
        for (Future<Boolean> future : futures) {
            if (!future.get()) {
                allSuccess = false;
            }
        }

        if (!allSuccess) {
            logMessage("Some window tasks failed.");
            System.exit(1);
        }

        logMessage("All windows processed successfully.");
        logWriter.close();
        System.exit(0);

        // 程序正常结束前关闭日志
        //（实际上System.exit(0)会直接退出JVM，可以将日志关闭写在System.exit(0)之前的地方）
        // 这里加上只是为了示意，如果真正执行到System.exit(0)日志文件也可以不手动关闭。
    }
}