"""Local medallion reference: raw lineage, validated upsert, daily energy mart."""
import csv,hashlib,io,json,math
from pathlib import Path
from datetime import datetime,timezone
import duckdb

SCHEMA="""
CREATE TABLE IF NOT EXISTS batches(batch_id VARCHAR PRIMARY KEY, source VARCHAR, loaded_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS bronze(batch_id VARCHAR, line INTEGER, raw_json VARCHAR);
CREATE TABLE IF NOT EXISTS quarantine(batch_id VARCHAR, line INTEGER, reason VARCHAR);
CREATE TABLE IF NOT EXISTS silver(meter_id VARCHAR, event_time TIMESTAMPTZ, kwh DOUBLE,
    revision INTEGER, batch_id VARCHAR, PRIMARY KEY(meter_id,event_time));
"""
def load(path,database='meters.duckdb',export_dir='results'):
    path=Path(path); payload=path.read_bytes(); batch=hashlib.sha256(payload).hexdigest()
    rows=list(csv.DictReader(io.StringIO(payload.decode('utf-8-sig'))))
    required={'meter_id','event_time','kwh','revision'}
    if not rows or not required.issubset(rows[0]): raise ValueError('Invalid meter CSV schema')
    con=duckdb.connect(str(database)); con.execute(SCHEMA)
    try:
        if con.execute('SELECT 1 FROM batches WHERE batch_id=?',[batch]).fetchone():
            return {'batch_id':batch,'replayed':True,'accepted':0,'rejected':0}
        con.execute('BEGIN')
        con.execute('INSERT INTO batches VALUES (?,?,?)',[batch,path.name,datetime.now(timezone.utc)])
        accepted=0; rejected=0
        for line,row in enumerate(rows,2):
            con.execute('INSERT INTO bronze VALUES (?,?,?)',[batch,line,json.dumps(row)])
            try:
                ts=datetime.fromisoformat(row['event_time'].replace('Z','+00:00'))
                kwh=float(row['kwh']); revision=int(row['revision'])
                if not row['meter_id'] or ts.tzinfo is None or not math.isfinite(kwh) or kwh<0 or revision<1:
                    raise ValueError('invalid_value')
            except (ValueError,TypeError):
                rejected+=1
                con.execute('INSERT INTO quarantine VALUES (?,?,?)',[batch,line,'invalid_value'])
                continue
            con.execute("""INSERT INTO silver VALUES (?,?,?,?,?)
                ON CONFLICT(meter_id,event_time) DO UPDATE SET
                kwh=excluded.kwh,revision=excluded.revision,batch_id=excluded.batch_id
                WHERE excluded.revision>silver.revision""",[row['meter_id'],ts,kwh,revision,batch])
            accepted+=1
        con.execute("""CREATE OR REPLACE TABLE gold AS
            SELECT meter_id,CAST(event_time AT TIME ZONE 'UTC' AS DATE) AS day,
                   sum(kwh) AS total_kwh,count(*) AS reading_count
            FROM silver GROUP BY meter_id,day""")
        con.execute('COMMIT')
        target=Path(export_dir); target.mkdir(parents=True,exist_ok=True)
        # Export after commit. If export fails, database remains correct; rerun demo exports.
        export(con,target)
        return {'batch_id':batch,'replayed':False,'accepted':accepted,'rejected':rejected}
    except Exception:
        try: con.execute('ROLLBACK')
        except duckdb.TransactionException: pass
        raise
    finally: con.close()

def export(con,target):
    for table in ['bronze','silver','gold','quarantine']:
        safe=str((target/(table+'.parquet')).resolve()).replace("'","''")
        con.execute(f"COPY {table} TO '{safe}' (FORMAT PARQUET)")
    safe=str((target/'daily_usage.csv').resolve()).replace("'","''")
    con.execute(f"COPY (SELECT * FROM gold ORDER BY meter_id,day) TO '{safe}' (HEADER,DELIMITER ',')")
