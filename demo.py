import json,duckdb
from pathlib import Path
from pipeline import load,export
if __name__=='__main__':
    for file in ['data/readings.csv','data/corrections.csv']:
        print(json.dumps(load(file)))
    with duckdb.connect('meters.duckdb') as con:
        export(con,Path('results'))
        print(con.execute('SELECT * FROM gold ORDER BY meter_id,day').fetchall())
