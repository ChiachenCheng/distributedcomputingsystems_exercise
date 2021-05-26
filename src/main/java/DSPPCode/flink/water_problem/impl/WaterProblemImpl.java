package DSPPCode.flink.water_problem.impl;

import DSPPCode.flink.water_problem.question.WaterProblem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import scala.Tuple4;
import java.math.BigInteger;

public class WaterProblemImpl extends WaterProblem {

  @Override
  public DataStream<String> execute(DataStream<String> dataStream) {
    DataStream<Tuple4<Boolean, BigInteger, BigInteger, BigInteger>> input = dataStream.map(
        new MapFunction<String, Tuple4<Boolean, BigInteger, BigInteger, BigInteger>>() {
          @Override
          public Tuple4<Boolean, BigInteger, BigInteger, BigInteger> map(String s) throws Exception {
            System.err.println(s);
            String[] ns = s.split(",");
            Boolean b = false;
            if (ns[0].equals("true"))
              b = true;
            Tuple4<Boolean, BigInteger, BigInteger, BigInteger> ret = new Tuple4<Boolean, BigInteger, BigInteger, BigInteger>
                (b, BigInteger.(Integer.parseInt(ns[1]), Integer.parseInt(ns[2]), Integer.parseInt(ns[3]));
            return null;
          }
        }
    );
    DataStream<Tuple4<Boolean, Integer, Integer, Integer>> ans = input.keyBy(1).flatMap(
        new FlatMapFunction<Tuple4<Boolean, Integer, Integer, Integer>, Tuple4<Boolean, Integer, Integer, Integer>>() {
          int line = 0, speed = 0, lasttime = 0;
          @Override
          public void flatMap(
              Tuple4<Boolean, Integer, Integer, Integer> tuple4,
              Collector<Tuple4<Boolean, Integer, Integer, Integer>> collector) throws Exception {
            if(tuple4._1()){

            } else {

            }
            collector
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
    return null;
  }
}
