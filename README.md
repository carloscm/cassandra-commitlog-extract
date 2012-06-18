# About

This is a rough, work-in-progress Java tool for parsing the Apache Cassandra commit log for a given period of time in the past, reading the affected rows from Cassandra, and then outputting them in other formats or databases. It includes output drivers for CSV files, PostgreSQL hstore and Oracle 11g.


# Usage

Build with Apache Ant, this project is not yet Mavenized.

Define your own input-output flows in the cont/exporter.conf file and invoke it from the command line parameters. Look into the provided configurations and shell files for examples. Right now the output mapping is quite rigid for each format and mostly a 1-to-1 mapping from the Cassandra rows.


# To do

- Mavenization
- Documentation
- Allow more than one output instance to be enabled
- Flexible, configurable input filters to deal better with fat rows
- Flexible output filters to deal with arbitrary SQL schemas
- Make it mostly scripting-driven instead of configuration-driven to solve previous problems, look into possible Java scripting engines


# License

Copyright (C) 2012 by Carlos Carrasco

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
