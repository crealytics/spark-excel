Spark-excel Source Code Structure
=================================

Spark-excel, under the hood, there are two implementations. From spark-excel 0.14.0 we added spark-excel V2, which uses Spark Data Source API V2.

These two implementations are compatible with each other in terms of options and behavior. However, there are features from spark-excel V2 that are not available in original spark-excel implementation, or example: loading multiple Excel files, corrupted record handling eg.

Spark DataSource API V2 is still under development, since spark 2.3. And to keep spark-excel V2 code to minimum, spark-excel V2 heavily relies on utilities and improvements of each upstream spark version.

Spark-excel V2 introduces spark-version specific code folder, like:
`2.4/.../spark/v2/excel` for Spark 2.4 Data Source API V2
`3.x/.../spark/v2/excel` for all Spark 3.* Data Source API V2
`3.1_3.2/.../spark/v2/excel` for shared code between Spark 3.1 and Spark 3.2 Data Source API V2

These structures are also configured into [build.sc](https://github.com/crealytics/spark-excel/blob/main/build.sc#L13), so it can compile for each Spark version.
