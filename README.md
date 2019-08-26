# concat-columns
Concat CSV columns along with timestamp in Spark


### Case_study.csv file

#### Load CSV into Spark Dataframe

```bash

df = spark.read.format("csv").option("header", "true").load("file:///tmp/case_study.csv")

df.registerTempTable("df")

df.show()

```

#### Import Pyspark SQL functions - col(), when(), lit() 
```bash

from pyspark.sql import functions as f
```

### Concatinate Phone Numbers Columns and email columns and current_timestamp
```bash

df = df.withColumn('phone_number', f.concat(
    f.when(f.col('phone_number_1').isNull(), f.lit('')).otherwise(f.col('phone_number_1')),
    f.when(f.col('phone_number_2').isNull(), f.lit('')).otherwise(f.col('phone_number_2')),
    f.when(f.col('phone_number_3').isNull(), f.lit('')).otherwise(f.col('phone_number_3'))
    ))

df = df.withColumn('email_id', f.concat(
    f.when(f.col('email_1').isNull(), f.lit('')).otherwise(f.col('email_1')),
    f.when(f.col('email_2').isNull(), f.lit('')).otherwise(f.col('email_2'))
    ))
    
df = df.withColumn('currenttimestamp', f.current_timestamp())

df.select('id', 'first_name', 'last_name', 'phone_number', 'email_id', 'created_at', 'currenttimestamp').show()

df.select('id', 'first_name', 'last_name', 'phone_number', 'email_id', 'created_at', 'currenttimestamp').write.parquet('/tmp/update_case_study__in_parquet')

hdfs dfs -ls /tmp/update_case_study__in_parquet

```
