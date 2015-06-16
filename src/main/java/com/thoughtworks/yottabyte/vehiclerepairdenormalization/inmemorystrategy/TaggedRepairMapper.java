package com.thoughtworks.yottabyte.vehiclerepairdenormalization.inmemorystrategy;

import com.google.common.base.Preconditions;
import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.Tag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaggedRepairMapper extends Mapper<Object, Text, Text, Text> {
  public static final String REPAIR_COLUMN_SEPARATOR = "REPAIR_COLUMN_SEPARATOR";

  private Configuration configuration;

  @Override
  protected void map(Object key, Text row, Context context) throws IOException, InterruptedException {
      String line = row.toString();
      String[] fields = line.split(get(REPAIR_COLUMN_SEPARATOR));
      String vehicleType = fields[0].toLowerCase();
      String repairType = fields[1];
      String repairDescription = fields[2];
      context.write(new Text(vehicleType), new Text(Tag.REPAIR.toString() + Tag.KEY_SEPARATOR + repairType + Tag.KEY_SEPARATOR + repairDescription));
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

