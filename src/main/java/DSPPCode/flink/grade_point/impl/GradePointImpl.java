package DSPPCode.flink.grade_point.impl;

import DSPPCode.flink.grade_point.question.GradePoint;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class GradePointImpl extends GradePoint{

  public DataStream<Tuple3<String, Integer, Float>> calculate(DataStream<String> text){
    DataStream<Tuple3<String, Integer, Float>> grades = text.map(
        new MapFunction<String, Tuple3<String, Integer, Float>>() {
          @Override
          public Tuple3<String, Integer, Float> map(String value) throws Exception{
            String[] strs = value.split(" ");
            int point = Integer.parseInt(strs[1]);
            float gde = Float.parseFloat(strs[2]);
            if (gde == -1.0)
              gde = 0.0f;
            return new Tuple3<String, Integer, Float>(strs[0],point, gde);
          }
        }
    );
    DataStream<Tuple3<String, Integer, Float>> calans = grades.keyBy(0).reduce(
        new ReduceFunction<Tuple3<String, Integer, Float>>() {
          @Override
          public Tuple3<String, Integer, Float> reduce(
              Tuple3<String, Integer, Float> t0,
              Tuple3<String, Integer, Float> t1) throws Exception {
            float ave = (t0.f2 * t0.f1 + t1.f2 * t1.f1) * 1.0f / (t0.f1 + t1.f1);
            return Tuple3.of(t0.f0, t0.f1 + t1.f1, ave);
          }
        }
    );
    DataStream<Tuple3<String, Integer, Float>> temp = calans.keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5))).reduce(
            new ReduceFunction<Tuple3<String, Integer, Float>>() {
              @Override
              public Tuple3<String, Integer, Float> reduce(
                  Tuple3<String, Integer, Float> stringIntegerFloatTuple3,
                  Tuple3<String, Integer, Float> t1) throws Exception {
                return null;
              }
            }
        );
    DataStream<Tuple3<String, Integer, Float>> ans = calans.filter(
        new FilterFunction<Tuple3<String, Integer, Float>>() {
          @Override
          public boolean filter(Tuple3<String, Integer, Float> stringIntegerFloatTuple3)
              throws Exception {
            if (stringIntegerFloatTuple3.f1 == 10)
              return true;
            return false;
          }
        }
    ).map(
        new MapFunction<Tuple3<String, Integer, Float>, Tuple3<String, Integer, Float>>() {
          @Override
          public Tuple3<String, Integer, Float> map(
              Tuple3<String, Integer, Float> t) throws Exception {
            return new Tuple3<String, Integer, Float>(t.f0, t.f1, (float)Math.round(t.f2 * 100)/100.0f);
          }
        }
    );
    temp.print();
    return ans;
  }

}
