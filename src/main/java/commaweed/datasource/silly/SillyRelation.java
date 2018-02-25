package commaweed.datasource.silly;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.ArrayList;
import java.util.List;

/**
 * A sample, custom BaseRelation that will be returned from my custom DataSource that acts as a
 * RelationProvider.  It is responsible for delegating the work to do the data reading and writing.
 */
public class SillyRelation extends BaseRelation
   implements TableScan
//   , PrunedScan, PrunedFilteredScan, InsertableRelation (not doing these for now)
{

   private static final Logger LOGGER = LoggerFactory.getLogger(SillyRelation.class);

   private final SQLContext sqlContext;
   private final scala.collection.immutable.Map<String, String> options;

   private StructType schema;
   private Dataset<Row> dataToInsert;
   private String[] requiredColumns;

   private String inputPath;

   private Encoder sillyEncoder = Encoders.bean(SillyRecord.class);

   private static final List<String> FAKE_DATA = new ArrayList<String>();
   static {
      FAKE_DATA.add("a,1,1.1");
      FAKE_DATA.add("b,2,2.2");
      FAKE_DATA.add("c,3,3.3");
      FAKE_DATA.add("d,4,4.4");
      FAKE_DATA.add("e,5,5.5");
      FAKE_DATA.add("aa,11,11.11");
      FAKE_DATA.add("bb,22,22.22");
      FAKE_DATA.add("cc,33,33.33");
      FAKE_DATA.add("dd,44,44.44");
      FAKE_DATA.add("ee,55,55.55");
   }

   private void validateInputPath() {
      Option<String> path = this.options.get("path");
      if (path == null || path.isEmpty()) {
         throw new IllegalArgumentException("Invalid input path; it must be provided (e.g. load('path'))");
      }
      this.inputPath = path.toString();
   }

   /**
    * Initialize.
    * @param sqlContext A reference to the SQLContext.  It can be used internally to perform queries, etc.
    * @param options A reference to the user provided options, if any.  It will contain the input
    *        "path" as one of its keys.
    */
   public SillyRelation(
      SQLContext sqlContext,
      scala.collection.immutable.Map<String, String> options
   ) {
      this.sqlContext = sqlContext;
      this.options = options;

      validateInputPath();

      // possibly validate options (e.g. accumulo ones)
   }

   /**
    * A reference to the sql context.
    * @return A reference to the SQLContext.
    */
   @Override
   public SQLContext sqlContext() {
      return this.sqlContext;
   }

   /**
    * Returns the schema - either use schema inference by generically going to the data
    * source and scanning the result set based upon the input path, or have the user
    * pass one in, or hard-code a specific one.
    * @return The schema.
    */
   @Override
   public StructType schema() {
      StructType schemaToUse = this.schema;  // if not null, user provided the schema

      if (schemaToUse == null) {
         schemaToUse = inferSchema();
      }

      return schemaToUse;
   }

   private StructType inferSchema() {
      // hard-coded way
      StructType schema = new StructType()
         .add("type", DataTypes.StringType)
         .add("count", DataTypes.IntegerType)
         .add("amount", DataTypes.DoubleType);

      // encoder way (Not working because it reversed the order)
//      StructType schema = sillyEncoder.schema();

      return schema;
   }

   /**
    * InsertableRelation - idea is to have this method to the writing to the data source.
    * @param data The data that will be inserted.
    * @param overwrite Indicates whether or not to overwrite the existing content (if any)
    */
//   @Override
   public void insert(Dataset<Row> data, boolean overwrite) {

   }

   /**
    * PrunedFilteredScan - idea is to have it to full data retrieval and apply column pruning
    * and filtering on disk.
    * @param requiredColumns The columns that should be fetched.
    * @param filters The filters to apply.
    * @return an RDD or generic Rows.
    */
//   @Override
   public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {
      return null;
   }

   /**
    * PrunedScan - idea is to have it to full data retrieval and apply column pruning.
    * @param requiredColumns The columns that should be fetched.
    * @return an RDD or generic Rows.
    */
//   @Override
   public RDD<Row> buildScan(String[] requiredColumns) {
      return null;
   }

   // return the fake data as silly records
   private List<SillyRecord> performTableScan() {
      List<SillyRecord> records = new ArrayList<>();
      for (String row : FAKE_DATA) {
         String[] values = row.split(",");
         SillyRecord record = new SillyRecord();
         record.setType(values[0]);
         record.setCount(Integer.parseInt(values[1]));
         record.setAmount(Double.parseDouble(values[2]));
         records.add(record);
      }
      return records;
   }

   /**
    * TableScan - idea is to have it to full data retrieval for the specified
    * input path with no push down filtering or column prune.
    * @return an RDD or generic Rows.
    */
   @Override
   public RDD<Row> buildScan() {
      List<SillyRecord> records = performTableScan();

      // convert silly records to RDD<Row>
      JavaSparkContext sparkContext = new JavaSparkContext(sqlContext.sparkContext());
      JavaRDD<Row> javaRdd = sparkContext.parallelize(records).map(
//         record -> RowFactory.create(record.getType(), record.getCount(), record.getAmount())
         record -> RowFactory.create(record.getType(), record.getCount(), record.getAmount())
      );

      // return the generic non-java RDD
      return javaRdd.rdd();
   }

   public StructType getSchema() {
      return schema;
   }

   public void setSchema(StructType schema) {
      this.schema = schema;
   }

   public Dataset<Row> getDataToInsert() {
      return dataToInsert;
   }

   public void setDataToInsert(SaveMode mode, Dataset<Row> dataToInsert) {
      this.dataToInsert = dataToInsert;
   }

   public String[] getRequiredColumns() {
      return requiredColumns;
   }

   public void setRequiredColumns(String[] requiredColumns) {
      if (requiredColumns != null && requiredColumns.length > 0) {
         this.requiredColumns = requiredColumns;
      }
   }
}
