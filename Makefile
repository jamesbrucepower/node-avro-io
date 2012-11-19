PATH := ${PATH}:/usr/local/bin:./node_modules/.bin/
AVRO_TOOLS_JAR = tools/avro-tools-1.7.2.jar
REPORTER = spec
AVRO_SRCS := $(wildcard test/data/*.json)
AVRO_FILES := $(AVRO_SRCS:.json=.avro)

avro-tools: 
	-@[ -d "tools" ] || mkdir tools
	-@[ -e $(AVRO_TOOLS_JAR) ] || (cd tools && curl -sO http://mirror.lividpenguin.com/pub/apache/avro/avro-1.7.2/java/avro-tools-1.7.2.jar)

test:
	@NODE_ENV=test ./node_modules/.bin/mocha -R $(REPORTER)
	
lib-cov:
	@jscoverage --no-highlight lib lib-cov
	
coverage: lib-cov
	-@MOCHA_COV=1 $(MAKE) test REPORTER=html-cov > coverage.html
	-@open coverage.html
        
avro: avro-tools $(AVRO_FILES)
	
test/data/%.avro: test/data/%.json test/data/%.schema
	java -jar $(AVRO_TOOLS_JAR) fromjson --schema-file $(word 2,$^) $< > $@
	
debug:
	@NODE_ENV=test ./node_modules/.bin/mocha debug -R $(REPORTER)
	
clean:
	-@rm coverage.html
	-@rm -rf lib-cov
	-@rm test/data/*.avro
	-@rm -rf tools
	
all: avro coverage
	
.PHONY: test
