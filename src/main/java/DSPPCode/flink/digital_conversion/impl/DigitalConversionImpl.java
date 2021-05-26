package DSPPCode.flink.digital_conversion.impl;

import DSPPCode.flink.digital_conversion.question.DigitalConversion;
import DSPPCode.flink.digital_conversion.question.DigitalWord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;

public class DigitalConversionImpl extends DigitalConversion{

  public DataStream<String> digitalConversion(DataStream<Tuple1<String>> digitals){
    DataStream<String> ans = digitals.map(
        new MapFunction<Tuple1<String>, String>() {
          @Override
          public String map(Tuple1<String> tuple1) throws Exception {
            double d = Double.parseDouble(tuple1.f0);
            switch(d){
              case 0.0:
                return DigitalWord.ZERO.getWord();
                // break;
              case 1.0:
                return DigitalWord.ONE.getWord();
              // break;
              case 2.0:
                return DigitalWord.TWO.getWord();
              // break;
              case 3.0:
                return DigitalWord.THREE.getWord();
              // break;
              case 4.0:
                return DigitalWord.FOUR.getWord();
              // break;
              case 5.0:
                return DigitalWord.FIVE.getWord();
              // break;
              case 6.0:
                return DigitalWord.SIX.getWord();
              // break;
              case 7.0:
                return DigitalWord.SEVEN.getWord();
              // break;
              case '8':
                return DigitalWord.EIGHT.getWord();
              // break;
              case '9':
                return DigitalWord.NINE.getWord();
              // break;
              default:
                return "";
            }
          }
        });
    return ans;
  }

}