

# Extracting current package version
VERSION := $(shell grep '^Version:' DESCRIPTION | awk '{print $$2}')

.PHONY: document
document:
	Rscript -e "devtools::document()"

.PHONY: clean
clean:
	-rm src/*.so
	-rm src/*.o
	-rm vignettes/*.html
	-rm -r vignettes/*_files/

.PHONY: install build
build: clean document
	@echo Building current version: $(VERSION)
	(cd ../ && R CMD build stadtrad)
install: build
	@echo Installing current version: $(VERSION)
	(cd ../ && R CMD INSTALL stadtrad_$(VERSION).tar.gz)
check: build
	@echo Checking current version: $(VERSION)
	(cd ../ && R CMD check --as-cran stadtrad_$(VERSION).tar.gz)

##.PHONY: readme docs
##readme:
##	quarto render README.qmd --to gfm
##
##docs: readme
##	Rscript -e "altdoc::render_docs(verbose = FALSE, parallel = TRUE)"
##	##Rscript -e "altdoc::render_docs(verbose = TRUE)"
