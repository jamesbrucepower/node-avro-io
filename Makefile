PATH := ${PATH}:/usr/local/bin:./node_modules/.bin/
AVRO_TOOLS_JAR = ~/Downloads/avro-tools-1.7.2.jar
REPORTER = dot

lib-cov:
	@jscoverage lib lib-cov
	
test:
	@NODE_ENV=test ./node_modules/.bin/mocha --reporter $(REPORTER)
        
test-cov: lib-cov
	@EXPRESS_COV=1 $(MAKE) test REPORTER=html-cov > coverage.html
	
test/data/%.avro: test/data/%.json test/data/%.schema
	java -jar $(AVRO_TOOLS_JAR) fromjson --schema-file $(word 2,$^) $< > $@
	
clean:
	-@[ -e "coverage.html" ] && rm coverage.html
	-@[ -d "lib" ] && rm -rf lib-cov
	-@[ -e "test/data/*.avro" ] && rm test/data/*.avro
	
.PHONY: test
