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
