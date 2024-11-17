package reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
// 该类将同索引主动单合并，并根据标准分类为超大，大，中，小
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
        long totalTradeQty = 0;
        double totalTradeAmount = 0;
        String tradeType = null;
        HashSet<String> uniqueTrades = new HashSet<>();

        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            double price = Double.parseDouble(fields[0]);
            long tradeQty = Long.parseLong(fields[1]);
            tradeType = fields[2]; // 获取买卖类型

            String uniqueKey = key.toString();
            if (!uniqueTrades.contains(uniqueKey)) {
                uniqueTrades.add(uniqueKey);
                totalTradeQty += tradeQty;
                totalTradeAmount += tradeQty * price;
            }
        }

        long circulatingStock = Long.parseLong(context.getConfiguration().get("circulatingStock"));
        double tradeVolumeRatio = (double) totalTradeQty / circulatingStock;

            String label;
            if (tradeVolumeRatio >=  0.003 || totalTradeQty > 200000 || totalTradeAmount > 1000000) {
                label = "超大";
            } else if (tradeVolumeRatio > 0.001 || totalTradeQty > 60000 || totalTradeAmount > 300000) {
                label = "大";
            } else if (tradeVolumeRatio > 0.00017 || totalTradeQty > 10000 || totalTradeAmount > 50000) {
                label = "中";
            } else {
                label = "小";
            }

            context.write(key, new Text(totalTradeQty + "\t" + totalTradeAmount + "\t" + label + "\t" + tradeType));

    }

}
