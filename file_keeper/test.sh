rm -f _test.db
PYTHONPATH=.. python3 file_db.py --db-file _test.db --path .
PYTHONPATH=.. python3 file_db.py --db-file _test.db --path .
PYTHONPATH=.. python3 file_db.py --db-file _test.db --list-files
PYTHONPATH=.. python3 file_db.py --db-file _test.db --list-dupes
rm _test.db
