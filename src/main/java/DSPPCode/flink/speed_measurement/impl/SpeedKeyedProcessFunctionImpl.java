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

  @Override
  public void processElement(Tuple2<String, String> t, Context context, Collector<String> collector)
      throws Exception {

  }

  @Override
  public float averageSpeed(String[] times) {
    return 0;
  }
}
