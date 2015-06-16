package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaggedVehicleMapper extends Mapper<Object, Text, Text, Text> {

  public static final String VEHICLE_COLUMN_SEPARATOR = "VEHICLE_COLUMN_SEPARATOR";
  public static final String VEHICLE_DATE_FORMAT = "VEHICLE_DATE_FORMAT";

  private Configuration configuration;

  @Override
  protected void map(Object key, Text row, Context context) throws IOException, InterruptedException {
      String line = row.toString();
      String[] fields = line.split(get(VEHICLE_COLUMN_SEPARATOR));
      String vehicleType = fields[0].toLowerCase();
      String vehicleRegistrationNumber = fields[1];
      context.write(new Text(vehicleType), new Text(Tag.VEHICLE.toString() + Tag.KEY_SEPARATOR + vehicleRegistrationNumber));
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    configuration = context.getConfiguration();
  }

    protected String get(String key){
        return Preconditions.checkNotNull(configuration.get(key),
                "Expected %s to be present, but was not", key);
    }
}
