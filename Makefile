PATH := ${PATH}:/usr/local/bin:./node_modules/.bin/
AVRO_TOOLS_JAR = tools/avro-tools-1.7.2.jar
REPORTER = dot

avro-tools:
	mkdir tools
	cd tools && curl -O http://apache.mirror.rbftpnetworks.com/avro/avro-1.7.2/java/avro-tools-1.7.2.jar

lib-cov:
	@jscoverage lib lib-cov
	
test:
	@NODE_ENV=test ./node_modules/.bin/mocha --reporter $(REPORTER)
        
test-cov: lib-cov
	@EXPRESS_COV=1 $(MAKE) test REPORTER=html-cov > coverage.html
	
test/data/%.avro: test/data/%.json test/data/%.schema avro-tools
	java -jar $(AVRO_TOOLS_JAR) fromjson --schema-file $(word 2,$^) $< > $@
	
clean:
	-@[ -e "coverage.html" ] && rm coverage.html
	-@[ -d "lib" ] && rm -rf lib-cov
	-@[ -e "test/data/*.avro" ] && rm test/data/*.avro
	-@[ -d "tools" ] && rm -r tools
	
.PHONY: test
