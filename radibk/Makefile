

# Extracting current package version
VERSION := $(shell grep '^Version:' DESCRIPTION | awk '{print $$2}')

.PHONY: document
document:
	Rscript -e "devtools::document()"

clean:


.PHONY: install build
build: clean document
	@echo Building current version: $(VERSION)
	(cd ../ && R CMD build radibk)
install: build
	@echo Installing current version: $(VERSION)
	(cd ../ && R CMD INSTALL radibk_$(VERSION).tar.gz)
check: build
	@echo Checking current version: $(VERSION)
	(cd ../ && R CMD check --as-cran radibk_$(VERSION).tar.gz)

