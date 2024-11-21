package reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SumThenLabelReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Long> circulatingStockMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles != null) {
            for (Path cacheFile : cacheFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toString()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split("\t");
                        circulatingStockMap.put(fields[0], Long.parseLong(fields[1]));
                    }
                }
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalTradeQty = 0;  // 总成交量
        double totalTradeAmount = 0;  // 总成交金额
        String tradeType = null;  // 买卖类型

        // 遍历所有值并累加
        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            double price = Double.parseDouble(fields[0]);  // 成交价格
            long tradeQty = Long.parseLong(fields[1]);     // 成交量
            tradeType = fields[2];                         // 买卖类型（取最后一次值即可）

            // 累加成交量和金额
            totalTradeQty += tradeQty;
            totalTradeAmount += tradeQty * price;
        }

        // 获取流通股数量
        long circulatingStock = Long.parseLong(context.getConfiguration().get("circulatingStock"));
        double tradeVolumeRatio = (double) totalTradeQty / circulatingStock;

        // 分类标签逻辑
        String label = "";
        if (tradeVolumeRatio >= 0.003 || totalTradeQty >= 200000 || totalTradeAmount >= 1000000) {
            label = "超大";
        } else if (tradeVolumeRatio >= 0.001 || totalTradeQty >= 60000 || totalTradeAmount >= 300000) {
            label = "大";
        } else if (tradeVolumeRatio >= 0.00017 || totalTradeQty >= 10000 || totalTradeAmount >= 50000) {
            label = "中";
        } else {
            label = "小";
        }

        // 输出 key 和分类结果
        context.write(key, new Text(totalTradeQty + "\t" + totalTradeAmount + "\t" + label + "\t" + tradeType));
    }
}
