# concat-columns
Concat CSV columns along with timestamp in Spark


#### Create Sample CSV file

```bash
cd /tmp

echo "c1, c2, c3, c4, c5, c6, c7, c8, c9" >> myfile.csv
echo "r11, r12, r13, r14, r15, r16, r17, r18, r19" >> myfile.csv
echo "r21, r22, r23, r24, r25, r26, r27, r28, r29" >> myfile.csv
echo "r31, r32, r33, r34, r35, r36, r37, r38, r39" >> myfile.csv

cat /tmp/myfile.csv

c1, c2, c3, c4, c5, c6, c7, c8, c9
r11, r12, r13, r14, r15, r16, r17, r18, r19
r21, r22, r23, r24, r25, r26, r27, r28, r29
r31, r32, r33, r34, r35, r36, r37, r38, r39
```

#### Open pySpark

```bash
[root@demo harsha]# pyspark

SparkSession available as 'spark'.
>>>

```

### Load CSV file (myfile.csv) into RDD

rdd = sc.textFile("file:///tmp/myfile.csv")

rdd.collect()




