Next
====


## [v0.18.4] - 2022-11-07
### :sparkles: New Features
- [`4ceca4f`](https://github.com/crealytics/spark-excel/commit/4ceca4f18434652a9ecaab076ea381ca927588d6) - V2 streaming read *(PR [#653](https://github.com/crealytics/spark-excel/pull/653) by [@pjfanning](https://github.com/pjfanning))*

### :wrench: Chores
- [`b86ce12`](https://github.com/crealytics/spark-excel/commit/b86ce1267c4831e64766f7172a63d136457a2a42) - Update scalafmt-core from 3.5.9 to 3.6.1 *(PR [#678](https://github.com/crealytics/spark-excel/pull/678) by [@scala-steward](https://github.com/scala-steward))*
- [`d09d232`](https://github.com/crealytics/spark-excel/commit/d09d23294ecfcb9894a8e3d7c584c011f9c042e8) - Update scalatest from 3.2.13 to 3.2.14 *(PR [#661](https://github.com/crealytics/spark-excel/pull/661) by [@scala-steward](https://github.com/scala-steward))*
- [`2344853`](https://github.com/crealytics/spark-excel/commit/2344853b1f7e14e29118a66985c546f547dddc0e) - Update poi-shared-strings from 2.5.4 to 2.5.5 *(PR [#659](https://github.com/crealytics/spark-excel/pull/659) by [@scala-steward](https://github.com/scala-steward))*
- [`1cd676e`](https://github.com/crealytics/spark-excel/commit/1cd676e6a14fb7e6ca0c9d0f7534526e4f7b57ae) - Update sbt-assembly from 1.2.0 to 2.0.0 *(PR [#665](https://github.com/crealytics/spark-excel/pull/665) by [@scala-steward](https://github.com/scala-steward))*
- [`ed97118`](https://github.com/crealytics/spark-excel/commit/ed97118a1ba7df720c18971d5c2e9635d0a8e0bb) - Update excel-streaming-reader from 4.0.2 to 4.0.4 *(PR [#670](https://github.com/crealytics/spark-excel/pull/670) by [@scala-steward](https://github.com/scala-steward))*
- [`7c96184`](https://github.com/crealytics/spark-excel/commit/7c96184398cf4e190208a61c187e06353d739f2d) - Update commons-compress from 1.21 to 1.22 *(PR [#676](https://github.com/crealytics/spark-excel/pull/676) by [@scala-steward](https://github.com/scala-steward))*


## [v0.18.0] - 2022-08-29
### :wrench: Chores
- [`64521bb`](https://github.com/crealytics/spark-excel/commit/64521bb6f4a9c763d9ed7d4ff8689dfc7c44bbf8) - Update base version *(commit by [@nightscape](https://github.com/nightscape))*

0.17.0
======
- Feature: Add PlainNumberFormat that does not round or use scientific notations for long numbers.
           Can be enabled by setting `usePlainNumberFormat=true` when reading the excel file. 
- Bugfix: Fixed SaveMode.Overwrite and SaveMode.Append for V2 API and spark >=3.0.1 
- Feature: Writing partitioned file structure for V2 API and spark >=3.0.1

0.13.2
======
- Change: Switch to the better maintained https://github.com/pjfanning/excel-streaming-reader
- Bugfix: https://github.com/crealytics/spark-excel/pull/229

0.13.1
======
- Bugfix: https://github.com/crealytics/spark-excel/pull/215

0.13.0
======
- Change: Rename `useHeader` option to `header` in order to better align with Spark's CSV reader.

0.12.6
======
- Bugfix: Properly handle empty spreadsheets

0.12.5
======
- Feature: Add `sheetNames' in shaded Workbook

0.12.4
======
- Bugfix: Shade xlsx-streamer. Should fix https://github.com/crealytics/spark-excel/issues/135

0.12.2
======
- Bugfix: Properly handle empty header cells (they get names like `_c1`)
- Bugfix: Properly read non-String cells in a sheet without headers

0.12.1
======
- Change: Update POI to 4.1.0 and several other dependencies
- Bugfix: The Scala 2.12 version now actually fixes https://github.com/crealytics/spark-excel/issues/93

0.12.0
======
- Change: Cross-build for Scala 2.11 and 2.12
- Bugfix: Bundle and shade commons-compress to prevent exceptions at runtime (fixes https://github.com/crealytics/spark-excel/issues/93)

0.11.1
======
- Bugfix: Make `dataAddress` actually optional.

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



[v0.18.0]: https://github.com/crealytics/spark-excel/compare/v0.18.0-beta2...v0.18.0

[v0.18.4]: https://github.com/crealytics/spark-excel/compare/v0.18.3-beta1...v0.18.4