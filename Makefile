PATH := ${PATH}:/usr/local/bin:./node_modules/.bin/
AVRO_TOOLS_JAR = tools/avro-tools-1.7.2.jar
REPORTER = spec
AVRO_SRCS=$(wildcard test/data/*.avro)

avro-tools: 
	-@[ -d "tools" ] || mkdir tools
	-@[ -e $(AVRO_TOOLS_JAR) ] || (cd tools && curl -sO http://apache.mirror.rbftpnetworks.com/avro/avro-1.7.2/java/avro-tools-1.7.2.jar)

lib-cov:
	@jscoverage lib lib-cov
	
test:
	@NODE_ENV=test ./node_modules/.bin/mocha --reporter $(REPORTER)
        
test-cov: lib-cov
	@EXPRESS_COV=1 $(MAKE) test REPORTER=html-cov > coverage.html
	
avro: $(AVRO_SRCS) avro-tools
	java -jar $(AVRO_TOOLS_JAR) fromjson --schema-file $(word 2,$^) $< > $@
	
clean:
	-@[ -e "coverage.html" ] && rm coverage.html
	-@[ -d "lib-cov" ] && rm -rf lib-cov
	-@[ -e "test/data/test.avro" ] && rm test/data/*.avro
	-@[ -d "tools" ] && rm -r tools
	
.PHONY: test
