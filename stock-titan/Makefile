dirs:
	@mkdir -p data archive logs
main: 
	@python3 main.py
ingest: 
	@python3 ingest.py
all:
	make dirs
	make main
	make ingest
