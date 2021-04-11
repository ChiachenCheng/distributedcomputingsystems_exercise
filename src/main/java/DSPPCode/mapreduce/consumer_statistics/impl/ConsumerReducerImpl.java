package DSPPCode.mapreduce.consumer_statistics.impl;

import DSPPCode.mapreduce.consumer_statistics.question.Consumer;
import DSPPCode.mapreduce.consumer_statistics.question.ConsumerReducer;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConsumerReducerImpl extends ConsumerReducer{
    HashMap<String, Boolean> Customer = new HashMap<>();
    @Override
    protected void reduce(Text key, Iterable<Consumer> values, Context context) throws IOException, InterruptedException{
        BigInteger people = BigInteger.ZERO;
        BigInteger money = BigInteger.ZERO;
        for (Consumer value:values){
            money = money.add(BigInteger.valueOf(value.getMoney()));
            if (Customer.containsKey(value.getId()))
                continue;
            Customer.putIfAbsent(value.getId(), true);
            people = people.add(BigInteger.ONE);
        }
        String ans = key.toString() + "\t" + people.toString() + "\t" + money.toString();
        context.write(new Text(ans), NullWritable.get());
    }
}
