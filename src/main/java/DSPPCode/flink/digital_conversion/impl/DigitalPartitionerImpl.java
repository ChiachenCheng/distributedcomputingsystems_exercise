package DSPPCode.flink.digital_conversion.impl;

import DSPPCode.flink.digital_conversion.question.DigitalPartitioner;

public class DigitalPartitionerImpl extends DigitalPartitioner{

  public abstract int partition(T key, int numPartitions){

  }

}