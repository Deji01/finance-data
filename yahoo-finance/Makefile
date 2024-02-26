dirs:
	@mkdir -p data archive logs
extract_urls: 
	@python3 extract_urls.py
extract_articles: 
	@python3 extract_articles.py
load_db: 
	@python3 load_db.py
load_vs: 
	@python3 load_vs.py
all:
	make dirs
	make extract_urls
	make extract_articles
	make load_db
	make load_vs
