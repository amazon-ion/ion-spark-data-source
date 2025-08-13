# Ion Spark Data Source

This package provides an implementation of the Spark Data Sources V1 API, specficially the `FileFormat` interface, which understands how to read and write Ion data, serialized as both text and binary. See the [`IonFileFormat`] for the data source entry point.

## Usage

### From Scala

Using the datasource from Scala is easy. You simply use `com.amazon.spark.ion.datasource.IonFileFormat` as the format type when calling read on a `SQLContext`, configure as desired, and invoke `load`:

    val spark: SQLContext = ???
    val df: Dataset[Row] = spark
      .read
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .options(options)
      ...
      .load(paths: _*)

Similarly, for write, invoke `save` on a `DataFrame`:

    val df: DataFrame = ???
    df
      .write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .options(options)
      .mode(saveMode)
      ...
      .save(path)

### Helper Methods in IonOperations

There are also helper methods defined in [`IonOperations`] to invoke from other Scala code (e.g. other custom data sources).

## Notes about reading Ion from Glue Catalogs

If attempting to use Spark's Catalog interface to read Ion data using this library you may need to set 
additional settings in your Spark config for proper reads. You need to set/add to the `spark.hadoop.hive.serdes.using.metastore.for.schema` conf:  

 > ```com.amazon.ionhiveserde.IonHiveSerDe```

This will force Spark to use the schema as returned from the catalog instead of trying to interpret the schema from 
the files using the SerDe. 

## Configuration

The following options can be configured when using the Ion data source. Passed in via options map in the FileFormat or DataSource.

### Serialization Options


- **serialization** - Controls the Ion serialization format. Options are:
  - `binary` (default) - Use Ion binary format
  - `text` - Use Ion text format

### Compression Options

- **compression** - Controls the compression codec used for writing Ion data. Defaults to `gzip`. Supports all Spark's CompressionCodecs.
  - Can be set via the options parameter or through the Spark configuration property `spark.ion.write.compressionCodec`

### Parser Mode Options

- **parserMode** (or **mode**) - Controls how the parser handles malformed records. Options are:
  - `FAILFAST` (default) - Throws an exception when encountering malformed records
  - `DROPMALFORMED` - Silently drops malformed records
  - `PERMISSIVE` - Attempts to parse malformed records, placing unparseable data in a column named by `columnNameOfCorruptRecord`

- **columnNameOfCorruptRecord** - Specifies the column name for corrupt records when using `PERMISSIVE` mode. Defaults to `_corrupt_record`.
  - Also supports `columnNameOfMalformedRecord` for backward compatibility

### Blob Handling Options

- **recordAsByteArray** - When set to `true`, the Ion writer will assume the FIRST column given to DataFrameWriter is an Ion blob column and will extract it and write it to the output as an Ion record. Defaults to `false`.
  - Useful when you have columns other than the blob for cases when you need to bucketBy, sortBy, or partitionBy on non-blob columns but only want to write the blob column as an Ion record.

- **recordBlobFieldName** - When set, Ion reader will read the entire row content as Ion binary data (blob) and store it into the BinaryType column whose name is defined in this parameter. The column must be in the schema provided to the DataFrameReader.
  - Useful when you need to load bucket-sorted data because Spark requires that the bucket-sort columns used in the BucketSpec be read from the source to leverage bucket-sort optimizations.

### Coercion Options

- **allowCoercionToString** - When set to `true`, allows coercion of Ion values to strings. Defaults to `false`.
  - Can be set via the option parameter or through the Spark configuration property `spark.ion.allowCoercionToString`

- **allowCoercionToBlob** - When set to `true`, allows coercion of Ion values to binary blobs. Defaults to `false`.
  - Can be set via the option parameter or through the Spark configuration property `spark.ion.allowCoercionToBlob`

### Other Options

- **enforceNullabilityConstraints** - When set to `true`, enforces nullability constraints defined in the schema. Defaults to `false`.

- **outputBufferSize** - Controls the buffer size for output operations. By default, this option is set to `-1`, which means the flush feature is not enabled. It is only supported when using `recordAsByteArray` and a buffer size greater than `-1`.
  - Can only be set through the Spark configuration property `spark.ion.output.bufferSize`

- **ignoreIonConversionErrors** - Kept for backward compatibility. When set to `true`, it translates to `PERMISSIVE` parser mode.

### Optimizer Options

- **IonUnzipOptimizer** - An optimizer that optimizes Ion unzipping operations by pruning the top-level schema used by IonToStructs to only include the required fields. This helps optimize jobs because only the pruned fields will be converted from Ion to Spark DataTypes.
  - Note: The configuration property `spark.sql.extensions.enableIonUnzipOptimizer` is no longer used.
  - To enable this optimizer, users must manually install the `IonUnzipOptimizerRuleSqlExtension` via an `appendSqlExtension` call on their SparkConf object:
    ```scala
    val sparkConf = new SparkConf()
      .appendSqlExtension(classOf[com.amazon.spark.ion.optimizer.IonUnzipOptimizerRuleSqlExtension])
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    ```

## Parsing Rules for Read

The general rule when parsing input Ion data **is being to be as permissive as possible while preventing information loss**. AKA the Cat Principle: If I fits, I sits.

### Reading into a `BooleanType` Field

The parser accepts Ion [`bool`](https://amzn.github.io/ion-docs/docs/spec.html#bool) and [`string`](https://amzn.github.io/ion-docs/docs/spec.html#string) values.

### Reading into Numeric Type Fields

Let's first separate the numeric types into 2 categories:
1. Exact representations (**Ion:** [`INT`](https://amzn.github.io/ion-docs/docs/spec.html#int), [`DECIMAL`](https://amzn.github.io/ion-docs/docs/spec.html#real-numbers) - **JVM:** byte, short, int, long, BigDecimal)
    - For conversions into exact representations (int/byte/decimal/...): we try to parse from any Ion source type (INT/FLOAT/DECIMAL), but the conversion has to be to exact, or it fails (otherwise it's an overflow and thus a very incorrect value - which we should never allow anyway)
1. Approximate representation - IEEE-754 binary floating point numbers (**Ion**: [`FLOAT`](https://amzn.github.io/ion-docs/docs/spec.html#real-numbers) - **JVM:** float, double)
    - For conversions into FloatType where the StructField has metadata "byte_length" which specifies 4 bytes - signaling the user understands what a FloatType is: we allow conversion from INT/FLOAT/DECIMAL regardless of precision loss - **as long as value isn't out of range and becomes +/- inf**.
    - For conversions into DoubleType where the StructField has metadata "byte_length" which specifies 8 bytes- signaling the user understands what a DoubleType is: same as above


For example, it still allows converting an `IonFloat` into a `ShortType` or `IntegerType`, AS LONG AS the value fits (within bounds) and has no fraction (no rounding). Same thing for an `IonDecimal` into an `IntegerType`. The reason is that Ion is a pretty loose in terms of enforcing schema, so it's possible to have datasets where the same column is sometimes an `IonInt` and sometimes an `IonFloat`, even if the data itself is still representable as an actual integer.

## Building and Testing

This project uses [SBT (Scala Build Tool)](https://www.scala-sbt.org/) for building and testing.

### Prerequisites

- **Java 8 or higher** (Java 17 or higher is recommended)
- **SBT** (will use version 1.10.0 as specified in `project/build.properties`)

### Building the Project

```bash
# Compile the main source code
sbt compile

# Run all tests
sbt test

# Run a specific test suite
sbt "testOnly *IonDataSourceSpec"

# Clean build artifacts
sbt clean
```

### Code Quality and Coverage

```bash
# Check code formatting
sbt scalafmtCheck

# Format code
sbt scalafmt

# Generate code coverage report
sbt coverageOn test coverageReport

# Run the full release process (format check, test, coverage, publish local)
sbt releaseLocal
```

### Publishing

```bash
# Publish to local Ivy repository
sbt publishLocal
```

### Java Version Compatibility

If you encounter Java compatibility issues (particularly with newer Java versions), you can specify Java 17:

```bash
# On macOS
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

# On Linux (adjust path as needed)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Then run SBT commands as normal
sbt compile
```

### IDE Integration

Most modern IDEs (IntelliJ IDEA, VS Code, Eclipse) have excellent SBT support and will automatically import the project configuration. Simply open the project directory in your IDE and it should recognize the SBT build configuration.

## Security

See [CONTRIBUTING](CONTRIBUTING.md) for more information.

## License

This project is licensed under the Apache-2.0 License.
