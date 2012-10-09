PATH := ${PATH}:/usr/local/bin:./node_modules/.bin/

test:
	node_modules/.bin/mocha --reporter spec
        
.PHONY: test