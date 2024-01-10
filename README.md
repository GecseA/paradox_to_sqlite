# Migrate old paradox tables to Sqlite

Last night's new project was the rewriting of an ancient Delphi program with a paradoxical database.
As usual, I looked for solutions online, but since I couldn't find a simple and working free solution, I wrote one instead.

The solution is a quite simple Python code (not as clean as should ;)

required dependencies: ( pip intall <lib> )
* pypxlab
* sqlite3

# Usage
```
python gv_converter.py -c cp852 -i "F:\many\many\folder\db\MYPARADOX1.DB" "F:\many\many\folder\db\MYPARADOX2.DB"  -o "F:\where\I\want\my\migrated_sqlite.db"
```

https://gecsevar.hu
