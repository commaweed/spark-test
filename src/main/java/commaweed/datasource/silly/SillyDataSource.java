package commaweed.datasource.silly;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

public class SillyDataSource
   implements SchemaRelationProvider, CreatableRelationProvider, DataSourceRegister, RelationProvider
{

   @Override
   public BaseRelation createRelation(
      SQLContext sqlContext, SaveMode mode, Map<String, String> options, Dataset<Row> data
   ) {
      SillyRelation sillyRelation = (SillyRelation) this.createRelation(sqlContext, options);
      sillyRelation.setDataToInsert(mode, data);
      return sillyRelation;
   }

   @Override
   public String shortName() {
      return "silly-ds";
   }

   @Override
   public BaseRelation createRelation(
      SQLContext sqlContext, Map<String, String> options, StructType schema
   ) {
      SillyRelation sillyRelation = (SillyRelation) this.createRelation(sqlContext, options);
      sillyRelation.setSchema(schema);
      return sillyRelation;
   }

   @Override
   public BaseRelation createRelation(
      SQLContext sqlContext, Map<String, String> options
   ) {
      return new SillyRelation(sqlContext, options);
   }
}
