package DSPPCode.flink.speed_measurement.impl;

import DSPPCode.flink.speed_measurement.question.SpeedMeasurement;
import DSPPCode.flink.speed_measurement.question.SpeedKeyedProcessFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class SpeedKeyedProcessFunctionImpl extends SpeedKeyedProcessFunction{


  @Override
  public void open(Configuration parameters) throws Exception {

  }


  private String[] avt = null;
  private String car = null;

  @Override
  public void processElement(Tuple2<String, String> t, Context context, Collector<String> collector)
      throws Exception {
    if(car == context.getCurrentKey()) return;
    if(t.f1.contains(":")) {
      if(avt == null) {
        avt = new String[2];
        avt[0] = t.f1;
      } else {
        avt[1] = t.f1;
        if(averageSpeed(avt) > 60.0f) {
          car = context.getCurrentKey();
          collector.collect(car);
        }
      }
    } else {
      float speed = Float.valueOf(t.f1);
      if(speed > 60.0f) {
        car = context.getCurrentKey();
        collector.collect(car);
      }
    }
  }

  @Override
  public float averageSpeed(String[] times) {
    String[] t1 = times[0].split(":");
    String[] t2 = times[1].split(":");
    int h1 = Integer.valueOf(t1[0]), m1 = Integer.valueOf(t1[1]), s1 = Integer.valueOf(t1[2]);
    int h2 = Integer.valueOf(t2[0]), m2 = Integer.valueOf(t2[1]), s2 = Integer.valueOf(t2[2]);
    return 36000.0f / (((h2 - h1) * 60 + m2 - m1) * 60 + s2 - s1);
  }
}
