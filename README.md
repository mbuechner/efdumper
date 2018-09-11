# efdump - A download/dumping tool for [Entity Facts](http://www.dnb.de/DE/Service/DigitaleDienste/EntityFacts/entityfacts_node.html) authority data service 

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
Start immediately with default configuration:
```sh
> java -jar efdump.jar
```

Print help text:
```sh
> java -jar efdump.jar -h
```
```
usage: java -jar efdump.jar [-i <folder>] [-l <language>] [-m dump|beacon]
            [-o {TIMESTAMP}-EFDump-{LANG}.json]
 -i <arg>   Folder with GND Turtle Dump(s) as GZipped File(s) (file name
            pattern is '*.ttl.gz'). Default: dumps/
 -l <arg>   Language(s) to dump (comma for separation, e.g. de-DE,en-US).
            Default: de-DE
 -m <arg>   Mode. Can be 'beacon' (create BEACON file) or 'dump' (dump
            Entity Facts data from service). Default: dump
 -o <arg>   File name of output file. Default:
            {TIMESTAMP}-EFDump-{LANG}.json
```

## Requirements
- **Gemeinsame Normdatei (GND) Dump(s)**: [GND Turtle Dump(s) as GZipped File(s)](https://data.dnb.de/opendata/) need to be stored locally in a folder. File extension must be *.ttl.gz.
- **Entity Facts:** Internet connection and access to the [Entity Facts](http://www.dnb.de/DE/Service/DigitaleDienste/EntityFacts/entityfacts_node.html) data service

## Configuration
```
	MAXTHREADS = 16
	MAXTHREADRERUN = 3
	THREADSLEEP = 1500
```

### GND entity types
```
http://d-nb.info/standards/elementset/gnd#DifferentiatedPerson
// sub-classes
http://d-nb.info/standards/elementset/gnd#CollectivePseudonym
http://d-nb.info/standards/elementset/gnd#Gods
http://d-nb.info/standards/elementset/gnd#LiteraryOrLegendaryCharacter
http://d-nb.info/standards/elementset/gnd#Pseudonym
http://d-nb.info/standards/elementset/gnd#RoyalOrMemberOfARoyalHouse
http://d-nb.info/standards/elementset/gnd#Spirits

http://d-nb.info/standards/elementset/gnd#CorporateBody
// sub-classes
http://d-nb.info/standards/elementset/gnd#Company
http://d-nb.info/standards/elementset/gnd#FictiveCorporateBody
http://d-nb.info/standards/elementset/gnd#MusicalCorporateBody
http://d-nb.info/standards/elementset/gnd#OrganOfCorporateBody
http://d-nb.info/standards/elementset/gnd#ProjectOrProgram
http://d-nb.info/standards/elementset/gnd#ReligiousAdministrativeUnit
http://d-nb.info/standards/elementset/gnd#ReligiousCorporateBody

http://d-nb.info/standards/elementset/gnd#Family

http://d-nb.info/standards/elementset/gnd#PlaceOrGeographicName
// sub-classes
http://d-nb.info/standards/elementset/gnd#Company
http://d-nb.info/standards/elementset/gnd#AdministrativeUnit
http://d-nb.info/standards/elementset/gnd#BuildingOrMemorial
http://d-nb.info/standards/elementset/gnd#Country
http://d-nb.info/standards/elementset/gnd#ExtraterrestrialTerritory
http://d-nb.info/standards/elementset/gnd#FictivePlace
http://d-nb.info/standards/elementset/gnd#MemberState
http://d-nb.info/standards/elementset/gnd#NameOfSmallGeographicUnitLyingWithinAnotherGeographicUnit
http://d-nb.info/standards/elementset/gnd#NaturalGeographicUnit
http://d-nb.info/standards/elementset/gnd#ReligiousTerritory
http://d-nb.info/standards/elementset/gnd#TerritorialCorporateBodyOrAdministrativeUnit
http://d-nb.info/standards/elementset/gnd#WayBorderOrLine
```

## Output
...
