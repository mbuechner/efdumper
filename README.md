# efdump - A download/dumping tool for [Entity Facts](http://www.dnb.de/DE/Service/DigitaleDienste/EntityFacts/entityfacts_node.html) authority data service 

Version: 2.0 (2019-07-02)
Maven documentation: https://mbuechner.github.io/efdump/

## Compile
```sh
> mvn clean package
```
### Maven documentation
Folder: ´´´´docs/
```sh
> mvn site
```

## Run
Print help text:
```sh
> java -jar efdump.jar -h
```
```
usage: java -jar efdump.jar [-i <file> | -f <folder>] [-l <language>] [-o {TIMESTAMP}-EFDump-{LANG}.json]
 -f <arg>   Folder with CSV file(s) containing one GND-ID in each line (file name pattern is '*.csv'). Default: input/
 -i <arg>   CSV file containing one GND-ID in each line.
 -l <arg>   Language(s) to dump (comma for separation, e.g. de-DE,en-US). Default: de-DE
 -o <arg>   File name of output file. Default: {TIMESTAMP}-EFDump-{LANG}.json
 -v         Print version
```
Example for simplest use:
```sh
> java -jar efdump.jar -i gndidns.csv -o authorities_entityfacts_20190702.jsonld
```
Content of file `gndidns.csv`:
```
10002142-6
10005411-0
10007410-8
10013721-0
10024106-2
10028341-X
10030138-1
...
```

## Requirements
- **CSV file(s)**: CSV file(s) containing one GND-ID in each line. The file extension must be *.csv.
- **Entity Facts:** Internet connection and access to the [Entity Facts](http://www.dnb.de/DE/Service/DigitaleDienste/EntityFacts/entityfacts_node.html) data service

## Configuration
```
	MAXTHREADS = 16
	MAXTHREADRERUN = 3
	THREADSLEEP = 1500
```