# Migrate old paradox tables to Sqlite

Last night's new project was the rewriting of an ancient Delphi program with a paradox database.
As usual, I looked for solutions online, but since I couldn't find a simple and working free solution, I wrote one instead.

The solution is a quite simple Python code (not as clean as should ;)

required dependencies: ( pip install <lib> )

- pypxlab
- sqlite3

# Usage

```
python gv_converter.py -c cp852 -i "F:\many\many\folder\db\MYPARADOX1.DB" "F:\many\many\folder\db\MYPARADOX2.DB"  -o "F:\where\I\want\my\migrated_sqlite.db"
```

or by specifying the path to the input tables:

```
python gv_converter.py -c cp852 -i "MYPARADOX1" "MYPARADOX2" -o "F:\where\I\want\my\migrated_sqlite.db" -p "F:\many\many\folder\db"
```

Note that the .db suffix may be omitted from the input table names.

Feel free to write me on errors and request (if U cleanup the code just create a pull request :)

More articles, code examples:
https://gecsevar.hu
