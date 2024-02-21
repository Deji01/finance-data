dirs:
	@mkdir -p data archive logs
main: 
	@python3 main.py
next: 
	@python3 next.py
ingest: 
	@python3 ingest.py
all:
	make dirs
	make main
	make next
	make ingest
