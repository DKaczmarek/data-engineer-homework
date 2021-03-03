run:
	poetry run homework \
		-i data/data_sample.log \
		-o data/data_output.csv \
		--local

test:
	poetry run pytest

test-verbose:
	poetry run pytest -rP


JOBDIR = ./job
build-job:
	@echo "> create build directory"
	if [ -d $(JOBDIR) ]; then rm -r $(JOBDIR); fi;
	mkdir $(JOBDIR)
	@echo "zip job source code"
	zip -r $(JOBDIR)/homework.zip homework \
		-x /*__pycache__/* \
		-x homework/main.py;
	@echo "copy executable file"
	cp homework/main.py $(JOBDIR)/main.py
	@echo "build complete"