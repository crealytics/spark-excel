Your issue may already be reported!
Please search on the [issue track](../) before creating one.
Moreover, please read the [`CHANGELOG.md`](../../blob/master/CHANGELOG.md) file for any changes you might have missed.

## Expected Behavior
> If you're describing a bug, tell us what should happen
> If you're suggesting a change/improvement, tell us how it should work

## Current Behavior
> If describing a bug, tell us what happens instead of the expected behavior
> If suggesting a change/improvement, explain the difference from current behavior.
> If you have a stack trace or any helpful information from the console, paste it in its entirety.
> If the problem happens with a certain file, upload it somewhere and paste a link.

## Possible Solution
> Not obligatory, but suggest a fix/reason for the bug,
> or ideas how to implement the addition or change

## Steps to Reproduce (for bugs)
> Provide a link to a live example, or an unambiguous set of steps to
> reproduce this bug. Include code to reproduce, if relevant.
> Example:
1. Download the example file uploaded [here](http://example.com/)
2. Start Spark from command line as `spark-shell --packages com.crealytics:spark-excel_2.12:x.y.z --foo=bar`
3. Read the downloaded example file
    ```
    val df = spark.read
    .format("com.crealytics.spark.excel")
    .option("dataAddress", "'My Sheet'!B3:C35")
    .load("example_file_exhibiting_bug.xlsx")
    ```

## Context
> How has this issue affected you? What are you trying to accomplish?
> Providing context helps us come up with a solution that is most useful in the real world

## Your Environment
> Include as many relevant details about the environment you experienced the bug in
* Spark version and language (Scala, Java, Python, R, ...):
* Spark-Excel version:
* Operating System and version, cluster environment, ...:
