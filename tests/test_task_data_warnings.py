import ast
import unittest
from pathlib import Path
import pandas as pd

SOURCE=Path(__file__).parents[1]/'monitoring_dashboard/encore_monitoring.py'

def health(row):
    tree=ast.parse(SOURCE.read_text())
    loader=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='load_task_status')
    func=next(n for n in loader.body if isinstance(n,ast.FunctionDef) and n.name=='health')
    ns={'pd':pd,'now':pd.Timestamp('2026-10-20T12:00:00Z')}
    exec(compile(ast.Module(body=[func],type_ignores=[]),str(SOURCE),'exec'),ns)
    return ns['health'](row)

class WarningDisplay(unittest.TestCase):
    def row(self,**changes):
        row=dict(enabled=True,last_task_result=0,status='SUCCESS_WITH_WARNINGS',run_start=pd.Timestamp('2026-10-20T11:00:00Z'),last_run_time=pd.NaT,next_run_time=pd.Timestamp('2026-10-21T11:00:00Z'))
        row.update(changes);return row
    def test_warning_is_visible(self):self.assertEqual(health(self.row()),'🟠 COMPLETED WITH DATA WARNINGS')
    def test_failure_not_hidden(self):self.assertEqual(health(self.row(last_task_result=1)),'🔴 WINDOWS FAILED')
    def test_script_failure_not_hidden(self):self.assertEqual(health(self.row(status='FAILED')),'🔴 SCRIPT FAILED')
    def test_missed_schedule_not_hidden(self):self.assertEqual(health(self.row(next_run_time=pd.Timestamp('2026-10-20T11:30:00Z'))),'🟠 MISSED SCHEDULE')
    def test_disabled_precedes_warning(self):self.assertEqual(health(self.row(enabled=False)),'⚪ DISABLED')
    def test_clean_success(self):self.assertEqual(health(self.row(status='SUCCESS')),'🟢 HEALTHY')

if __name__=='__main__':unittest.main()
