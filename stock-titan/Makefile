dirs:
	@mkdir -p data archive logs
extract: 
	@python3 extract.py
load_db: 
	@python3 load_db.py
load_vs: 
	@python3 load_vs.py
all:
	make dirs
	make extract
	make load_db
	make load_vs
