CSV_DIR := rawcsv

all:
	python3 create_dataset.py --dir $(CSV_DIR)

.PHONY: venv clean
venv:
	source venv/bin/activate

clean:
	rm -f *.csv
