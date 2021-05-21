package DSPPCode.flink.gcd.impl;

import DSPPCode.flink.gcd.question.GCD;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class GCDImpl extends GCD{
  public DataStream<Tuple3<String, Integer, Integer>> calGCD(IterativeStream<Tuple3<String, Integer, Integer>> iteration){
        // |创建反馈流|
        // |选择第三位置不为0的元组，例如(A, 2, 1)|
        DataStream<Tuple3<String, Integer, Integer>> feedback =
            iteration.filter(
                new FilterFunction<Tuple3<String, Integer, Integer>>() {
                  @Override
                  public boolean filter(Tuple3<String, Integer, Integer> value) throws Exception {
                    return value.f2 != 0;
                  }
                });
        // |实现迭代步逻辑，计算下一个GCD|
        DataStream<Tuple3<String, Integer, Integer>> iteratedStream =
          feedback.flatMap(
              new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
                @Override
                public void flatMap(
                    Tuple3<String, Integer, Integer> value, Collector<Tuple3<String, Integer, Integer>> out)
                    throws Exception {
                  // |例如迭代算子的输入输入为(A, 2, 1)，此处转换将(A, 2, 1)转换为(A, 1, 0)|
                  Tuple3<String, Integer, Integer> feedbackValue =
                      new Tuple3(value.f0, value.f2, value.f1 % value.f2);
                  out.collect(feedbackValue);
                }
              });
        iteration.closeWith(iteratedStream);
        return iteratedStream;
  }
}