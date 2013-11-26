PATH := ${PATH}:/usr/local/bin:./node_modules/.bin/
AVRO_TOOLS_JAR = avro-tools-1.7.5.jar
AVRO_TOOLS = tools/$(AVRO_TOOLS_JAR)
TEST_DATA = test/data
REPORTER = spec
AVRO_SRCS := $(wildcard $(TEST_DATA)/*.json)
AVRO_FILES := $(AVRO_SRCS:.json=.avro)

$(AVRO_TOOLS): 
	-@[ -d "tools" ] || mkdir tools
	-@[ -e $(AVRO_TOOLS) ] || (cd tools && curl -sO http://www.mirrorservice.org/sites/ftp.apache.org/avro/avro-1.7.5/java/$(AVRO_TOOLS_JAR))
	
test: $(AVRO_FILES)
	@NODE_ENV=test ./node_modules/.bin/mocha -R $(REPORTER)
	
lib-cov:
	@jscoverage --no-highlight lib lib-cov
	
coverage: lib-cov
	-@MOCHA_COV=1 $(MAKE) test REPORTER=html-cov > coverage.html
	-@open coverage.html
        	
$(TEST_DATA)/%.avro: $(AVRO_TOOLS) $(TEST_DATA)/%.json $(TEST_DATA)/%.schema
	java -jar $(AVRO_TOOLS) fromjson --schema-file $(word 3,$^) $(word 2,$^) > $@
	java -jar $(AVRO_TOOLS) random --codec deflate --count 4096 --schema-file $(TEST_DATA)/log.schema $(TEST_DATA)/log.deflate.avro
	java -jar $(AVRO_TOOLS) random --codec snappy --count 4096 --schema-file $(TEST_DATA)/log.schema $(TEST_DATA)/log.snappy.avro
	
debug:
	@NODE_ENV=test ./node_modules/.bin/mocha debug -R $(REPORTER)
	
clean:
	-@[ -f coverage.html ] && rm coverage.html || exit 0
	-@[ -d lib-cov ] && rm -rf lib-cov || exit 0
	-@rm $(TEST_DATA)/*.avro || exit 0
	-@rm $(TEST_DATA)/.*avro.crc || exit 0
	-@rm -rf tools || exit 0
	
all: test coverage
	
.PHONY: test lib-cov coverage avro debug clean all 
