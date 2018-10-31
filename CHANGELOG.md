0.11.0
======
- Change: `dataAddress` as uniform way to specify where to read data from / write data to.
  Remove now obsolete `sheetName`, `startColumn`, `endColumn`, `skipFirstRows`.
- Feature: Append to existing files.
  Only the range implicitly or explicitly specified via `dataAddress` will be overwritten.
- Change: Remove `preHeaderLines`.
  This is superseded by writing into a file that contains all the required pre-headers and footers.

0.10.1
======
- Bugfix: Shade commons-compress. Fixes https://github.com/crealytics/spark-excel/issues/93

0.10.0
======
- Change: Update to Apache POI 4.0.0

0.9.18
======
- Feature: Add `workbookPassword` option for reading encrypted excel spreadsheets

0.9.17
======
- Bugfix: Handle multi-line column headers properly

0.9.16
======
- Bugfix: Improve handling of columns where the first row doesn't contain any data
- Feature: Add `preHeader` option for writing rows/cells before the column headers
- Feature: Add `skipFirstRows` option for skipping rows before the column headers

0.9.15
=====
- Feature: Add handling of floats

0.9.12
=====
- Feature: Improve performance by upgrading dependencies and instantiating date formatting lazily

0.9.11
=====
- Bugfix: Infer schema for FORMULA cells

0.9.10
=====
- Bugfix: Make sure files are closed after reading

0.9.9
=====
- Feature: Added support for FORMULA cells when typing to String or Numeric types

0.9.8
=====
- Bugfix: Shade Jackson dependency to avoid version conflicts

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
