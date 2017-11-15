0.9.7
=====
- Feature: `excerptSize` option determines how many rows to read when inferring schema

0.9.6
=====
- Feature: Read file using streaming by specifying `maxRowsInMemory`

0.9.5
=====
- Feature: Serialize BigDecimals

0.9.4
=====
- Feature: Detect date formatted columns when inferring schema #28
- Internal: Use scalafmt for code formatting

0.9.2 & 0.9.3
=============
- Feature: Reading and writing timestamps

0.9.1
=====
- Bugfix: `null` values in DataFrames are serialized as empty Strings. Thanks to @slehan

0.9.0
=====
- Feature: Writing Excel files
- Change: Providing the path to the file is now either done via `.option("path", thePath)` or `.load(thePath)`

0.8.6
=====
- Change: Some previously required parameters are now optional and have a default

0.8.5
=====
- Feature: Respecting user-provided schema
- Bugfix: Several fixes to parsing
