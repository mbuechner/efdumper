# efdump
A dumping tool for Entity Facts.

Compile: ``mvn clean package``
Run: ``java -jar EFDumper.jar`` OR ``java -jar EFDumper.jar -i GND.ttl.gz -o myEFDump.json``

- Source: [GND dump](https://data.dnb.de/opendata/)
- Working data: http://hub.culturegraph.org/entityfacts/{ID}
```
	MAXTHREADS = 16
	MAXTHREADRERUN = 3
	THREADSLEEP = 1500
```