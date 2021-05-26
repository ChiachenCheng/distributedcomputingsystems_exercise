package DSPPCode.flink.water_problem.impl;

import DSPPCode.flink.water_problem.question.WaterProblem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import java.math.BigInteger;
import org.apache.flink.api.java.tuple.Tuple4;

public class WaterProblemImpl extends WaterProblem {

  @Override
  public DataStream<String> execute(DataStream<String> dataStream) {
    DataStream<Tuple4<Boolean, Long, Long, Long>> input = dataStream.map(
        new MapFunction<String, Tuple4<Boolean, Long, Long, Long>>() {
          @Override
          public Tuple4<Boolean, Long, Long, Long> map(String s) throws Exception {
            System.err.println(s);
            String[] ns = s.split(",");
            Boolean b = false;
            if (ns[0].equals("true"))
              b = true;
            Tuple4<Boolean, Long, Long, Long> ret = new Tuple4<Boolean, Long, Long, Long>
                (b, Long.parseLong(ns[1]), Long.parseLong(ns[2]), Long.parseLong(ns[3]));
            return ret;
          }
        }
    );
    DataStream<Tuple4<Boolean, Long, Long, Long>> cal = input.keyBy(1).flatMap(
        new FlatMapFunction<Tuple4<Boolean, Long, Long, Long>, Tuple4<Boolean, Long, Long, Long>>() {
          long line = 0, speed = 0, lasttime = 0;
          @Override
          public void flatMap(
              Tuple4<Boolean, Long, Long, Long> tuple4,
              Collector<Tuple4<Boolean, Long, Long, Long>> collector) throws Exception {
            if(tuple4.f0){
              Tuple4<Boolean, Long, Long, Long> col = new Tuple4<Boolean, Long, Long, Long>(true,
                  tuple4.f1, tuple4.f2, line + (tuple4.f2 - lasttime) * speed);
              collector.collect(col);
            } else {
              line += (tuple4.f2 - lasttime) * speed;
              speed = tuple4.f3;
            }
          }
        }
    );
    DataStream<String> ans = cal.map(
        new MapFunction<Tuple4<Boolean, Long, Long, Long>, String>() {
          @Override
          public String map(Tuple4<Boolean, Long, Long, Long> tuple4)
              throws Exception {
            return tuple4.f1.toString() + "," + tuple4.f2.toString() + "," + tuple4.f3.toString();
          }
        }
    );
    // SingleOutputStreamOperator<Tuple4<Boolean, Integer, Integer, Integer>> ans;
    // ans = input.keyBy(1).reduce(
    //     new ReduceFunction<Tuple4<Boolean, Integer, Integer, Integer>>() {
    //       @Override
    //       public Tuple4<Boolean, Integer, Integer, Integer> reduce(
    //           Tuple4<Boolean, Integer, Integer, Integer> booleanIntegerIntegerIntegerTuple4,
    //           Tuple4<Boolean, Integer, Integer, Integer> t1) throws Exception {
    //         if
    //         return null;
    //       }
    //     }
    // );
    System.err.println("--------------");
    ans.print();
    return ans;
  }
}
