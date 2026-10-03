import tempfile,unittest
from pathlib import Path
import duckdb
from pipeline import load
class MeterTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.db=Path(self.temp.name)/'meter.duckdb'; self.out=Path(self.temp.name)/'out'
    def run_load(self,file): return load(file,self.db,self.out)
    def query(self,q):
        with duckdb.connect(str(self.db)) as c: return c.execute(q).fetchall()
    def test_invalid_rows_quarantined(self):
        result=self.run_load('data/readings.csv'); self.assertEqual(result['rejected'],2)
        self.assertEqual(self.query('select count(*) from bronze'),[(6,)])
        self.assertEqual(self.query('select count(*) from silver'),[(3,)])
    def test_replay_idempotent(self):
        self.run_load('data/readings.csv')
        self.assertTrue(self.run_load('data/readings.csv')['replayed'])
        self.assertEqual(self.query('select count(*) from bronze'),[(6,)])
    def test_late_correction_and_stale_revision(self):
        self.run_load('data/readings.csv'); self.run_load('data/corrections.csv')
        self.assertEqual(self.query("select total_kwh from gold where meter_id='m001'"),[(4.0,)])
    def test_parquet_roundtrip(self):
        self.run_load('data/readings.csv')
        with duckdb.connect() as c:
            count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(self.out/'silver.parquet')]).fetchone()[0]
        self.assertEqual(count,3)
    def test_schema_failure_does_not_load(self):
        p=Path(self.temp.name)/'bad.csv'; p.write_text('a,b\n1,2')
        with self.assertRaises(ValueError): self.run_load(p)
