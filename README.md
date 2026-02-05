Py4JJavaError                             Traceback (most recent call last)
<ipython-input-95-f87ed5dd8633> in <cell line: 4>()
      2 Output_df = xx
      3 Output = dataiku.Dataset("Output")
----> 4 dkuspark.write_with_schema(Output, Output_df)

/data1/dataiku/dataiku-dss-12.5.1/python/dataiku/spark/__init__.py in write_with_schema(dataset, dataframe, delete_first)
    196 
    197     write_schema_from_dataframe(dataset, dataframe)
--> 198     write_dataframe(dataset, dataframe, delete_first)
    199 
    200 

/data1/dataiku/dataiku-dss-12.5.1/python/dataiku/spark/__init__.py in write_dataframe(dataset, dataframe, delete_first)
    186 
    187     dsc = __dataikuSparkContext(dataframe._sc._jvm)
--> 188     dsc.savePyDataFrame(dataset.full_name, dataframe._jdf, dataset.writePartition, delete_first and "OVERWRITE" or "APPEND", False)
    189 
    190 

/opt/cloudera/parcels/SPARK3/lib/spark3/python/lib/py4j-0.10.9.5-src.zip/py4j/java_gateway.py in __call__(self, *args)
   1319 
   1320         answer = self.gateway_client.send_command(command)
-> 1321         return_value = get_return_value(
   1322             answer, self.gateway_client, self.target_id, self.name)
   1323 

/opt/cloudera/parcels/SPARK3/lib/spark3/python/pyspark/sql/utils.py in deco(*a, **kw)
    188     def deco(*a: Any, **kw: Any) -> Any:
    189         try:
--> 190             return f(*a, **kw)
    191         except Py4JJavaError as e:
    192             converted = convert_exception(e.java_exception)

/opt/cloudera/parcels/SPARK3/lib/spark3/python/lib/py4j-0.10.9.5-src.zip/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
    324             value = OUTPUT_CONVERTER[type](answer[2:], gateway_client)
    325             if answer[1] == REFERENCE_TYPE:
--> 326                 raise Py4JJavaError(
    327                     "An error occurred while calling {0}{1}{2}.\n".
    328                     format(target_id, ".", name), value)

Py4JJavaError: An error occurred while calling o859.savePyDataFrame.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 2 in stage 90.0 failed 4 times, most recent failure: Lost task 2.3 in stage 90.0 (TID 6982) (hki9vk7e5idi907.hk.standardchartered.com executor 39): org.apache.spark.SparkUpgradeException: You may get a different result due to the upgrading to Spark >= 3.0: Fail to parse '2026-01-06T09:11:02.236' in the new parser. You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string.
	at org.apache.spark.sql.errors.QueryExecutionErrors$.failToParseDateTimeInNewParserError(QueryExecutionErrors.scala:1083)
	at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:148)
	at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:141)
	at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
	at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:176)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.writeFields_0_1$(Unknown Source)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.apply(Unknown Source)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificUnsafeProjection.apply(Unknown Source)
	at scala.collection.Iterator$$anon$10.next(Iterator.scala:461)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage3.sort_addToSorter_0$(Unknown Source)
	at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage3.processNext(Unknown Source)
	at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
	at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:760)
	at org.apache.spark.sql.execution.aggregate.SortAggregateExec.$anonfun$doExecute$1(SortAggregateExec.scala:62)
	at org.apache.spark.sql.execution.aggregate.SortAggregateExec.$anonfun$doExecute$1$adapted(SortAggregateExec.scala:59)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndexInternal$2(RDD.scala:877)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndexInternal$2$adapted(RDD.scala:877)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
	at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
	at org.apache.spark.scheduler.Task.run(Task.scala:136)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:551)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1505)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:554)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:750)
Caused by: java.time.format.DateTimeParseException: Text '2026-01-06T09:11:02.236' could not be parsed, unparsed text found at index 10
	at java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:1952)
	at java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1777)
	at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:168)
	... 28 more

Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2672)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2608)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2607)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2607)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1182)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1182)
	at scala.Option.foreach(Option.scala:407)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1182)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2868)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2810)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2799)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
Caused by: org.apache.spark.SparkUpgradeException: You may get a different result due to the upgrading to Spark >= 3.0: Fail to parse '2026-01-06T09:11:02.236' in the new parser. You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string.
	at org.apache.spark.sql.errors.QueryExecutionErrors$.failToParseDateTimeInNewParserError(QueryExecutionErrors.scala:1083)
	at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:148)
	at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:141)
	at scala.runtime.AbstractPartialFunction.apply(Abstract
