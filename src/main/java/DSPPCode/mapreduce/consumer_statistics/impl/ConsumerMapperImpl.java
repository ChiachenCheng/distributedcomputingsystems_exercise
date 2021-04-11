package DSPPCode.mapreduce.consumer_statistics.impl;

import DSPPCode.mapreduce.consumer_statistics.question.ConsumerMapper;
import DSPPCode.mapreduce.consumer_statistics.question.Consumer;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConsumerMapperImpl extends ConsumerMapper{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] cons = value.toString().split("\\s+");
        int l = cons.length;
        boolean isv = false;
        if (cons[l-1] == VIP)
            isv = true;
        int m = 0;
        try {
            m = Integer.parseInt(cons[l-2]);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        Consumer consumer = new Consumer(cons[0], m, isv);
        context.write(new Text(cons[l-1]), consumer);
    }
}
