package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DenormalizingReducer extends Reducer<Text, Text, NullWritable, Text> {

  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";
  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";
  public static final String VEHICLE_DATE_FORMAT = "VEHICLE_DATE_FORMAT";

  private Configuration configuration;

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<String> vehicleList = new ArrayList<>();
      List<String> repairList = new ArrayList<>();

      for (Text value : values) {
          String[] fields = value.toString().split(Tag.KEY_SEPARATOR);

          if (fields[0].equals(Tag.VEHICLE.toString())) {
              vehicleList.add(key.toString() + Tag.KEY_SEPARATOR + fields[1]);
          } else {
              repairList.add(key.toString() + Tag.KEY_SEPARATOR + fields[1] + Tag.KEY_SEPARATOR + fields[2]);
          }
      }

      for (String vehicle : vehicleList) {
          String[] vehicleFields = vehicle.split(Tag.KEY_SEPARATOR);
          for (String repair : repairList) {
              String[] repairFields = repair.split(Tag.KEY_SEPARATOR);
              if (vehicleFields[0].equals(repairFields[0])) {
                  context.write(NullWritable.get(), new Text(vehicleFields[0] + Tag.KEY_SEPARATOR + vehicleFields[1] + Tag.KEY_SEPARATOR + repairFields[1] + Tag.KEY_SEPARATOR + repairFields[2]));
              }
          }
      }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
  }


  protected String get(String key) {
    return Preconditions.checkNotNull(configuration.get(key),
      "Expected %s to be present, but was not", key);
  }

}

