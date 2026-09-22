import copy
import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch

from minute_prediction import MinutePredictions, feature_at, parse_tencent, session, TZ


def history():
    days = {}
    for j in range(5):
        day = '2026-09-' + str(14+j)
        points = []
        for k in range(121):
            stamp = datetime(2026, 9, 14, 9, 30) + timedelta(minutes=k)
            points.append({'time':stamp.strftime('%H:%M'), 'price':10+j+k*.001,
                           'volume':(k+1)*100*(j+1), 'amount':(k+1)*100*(j+1)*(10+j)})
        points.append({'time':'15:00', 'price':10+j+.15, 'volume':50000*(j+1), 'amount':500000*(j+1)})
        days[day] = points
    return days


class PredictionTests(unittest.TestCase):
    def test_cumulative_volume_is_not_summed_twice(self):
        days = history()
        f = feature_at(days, '2026-09-18', '10:10')
        self.assertEqual(f['windowVolumeRatio'], 1)
        self.assertEqual(f['volumeState'], '量能平稳')
        self.assertEqual(f['sameTimeVolumeRatio'], 1.458)
        self.assertEqual(f['sameTimeVolumeState'], '较昨日/前日同期放量')
        self.assertEqual(f['comparisons'][0]['cumulativeVolumeRatio'], 1.25)
        self.assertEqual(f['comparisons'][0]['windowVolumeRatio'], 1.25)
        self.assertAlmostEqual(f['averagePrice'], 14)

    def test_future_prices_do_not_change_prediction_features(self):
        days = history()
        before = feature_at(days, '2026-09-18', '10:10')
        for p in days['2026-09-18']:
            if p['time']>'10:10':
                p.update(price=9999, volume=99999999, amount=99999999)
        self.assertEqual(before, feature_at(days, '2026-09-18', '10:10'))

    def test_incomplete_window_and_lunch_are_not_zero_volume(self):
        days = history()
        days['2026-09-18'] = [p for p in days['2026-09-18'] if p['time']!='10:00']
        self.assertIsNone(feature_at(days,'2026-09-18','10:10'))
        self.assertIsNone(feature_at(days,'2026-09-18','13:10'))

    def test_missing_amount_does_not_fabricate_vwap(self):
        days=history()
        for p in days['2026-09-18']: p['amount']=None
        f=feature_at(days,'2026-09-18','10:10')
        self.assertIsNone(f['averagePrice'])
        self.assertIsNone(f['comparisons'][0]['cumulativeAmountRatio'])

    def test_parser_keeps_dates_and_missing_bse_amount(self):
        result=parse_tencent({'data':{'bj920438':{'data':[{'date':'20260918','data':['0930 10 100','0931 11 120']} ]}}},'bj920438')
        self.assertEqual(result['2026-09-18'][1]['volume'],120)
        self.assertIsNone(result['2026-09-18'][1]['amount'])

    def test_session_respects_weekend_and_lunch(self):
        self.assertFalse(session(datetime(2026,9,20,10,tzinfo=TZ)))
        self.assertFalse(session(datetime(2026,9,18,12,tzinfo=TZ)))
        self.assertTrue(session(datetime(2026,9,18,10,tzinfo=TZ)))

    def test_replay_and_sample_gating_and_persistence(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'test.sqlite'
            service=MinutePredictions(path, lambda:{'stocks':[]}, lambda *args: (_ for _ in ()).throw(ValueError()))
            days=history()
            with service.db() as db:
                db.executemany('INSERT INTO prediction_minutes VALUES (?,?,?,?,?,?)',
                    [('688361',d,p['time'],p['price'],p['volume'],p['amount']) for d,ps in days.items() for p in ps])
            with patch('minute_prediction.datetime') as clock:
                clock.now.return_value=datetime(2026,9,20,10,tzinfo=TZ)
                p=service.predict('688361',{'name':'Test'})
            self.assertEqual(p['mode'],'review')
            self.assertEqual(p['date'],'2026-09-18')
            self.assertEqual(p['minute'],'11:30')
            self.assertIsNone(p['closeUpProbability'])
            self.assertIsNone(p['furtherUpProbability'])
            with service.db() as db:
                self.assertGreater(db.execute('SELECT count(*) FROM prediction_samples').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT count(*) FROM prediction_outputs').fetchone()[0],1)
            # A historical replay excludes all outcomes of the replay date.
            self.assertFalse(service.model('2026-09-16')['valid'])
            self.assertEqual(service.model('2026-09-16')['samples'],0)

    def test_reactivated_blacklist_hidden_without_rescan(self):
        with tempfile.TemporaryDirectory() as directory:
            service=MinutePredictions(Path(directory)/'test.sqlite', lambda:{'stocks':[]}, None)
            with service.db() as db:
                db.execute('CREATE TABLE custom_blacklist(code TEXT,active INTEGER)')
                db.execute("INSERT INTO custom_blacklist VALUES ('688361',1)")
            service.members={'688361':{'name':'A'},'688114':{'name':'B'}}
            self.assertEqual([p['code'] for p in service.view()['stocks']], ['688114'])

    def test_preliminary_probability_uses_prior_dates_and_shrinkage(self):
        with tempfile.TemporaryDirectory() as directory:
            service=MinutePredictions(Path(directory)/'test.sqlite', lambda:{'stocks':[]}, None)
            key='star|10|0|3'
            rows=[]
            for date in ('2026-09-16','2026-09-17'):
                for index in range(40):
                    rows.append((f'68{index:04d}',date,'10:10',key,'{}',1 if index < 30 else 0,
                                 1 if index < 20 else 0,1))
            with service.db() as db:
                db.executemany('INSERT INTO prediction_samples VALUES (?,?,?,?,?,?,?,?)', rows)
            model=service.model('2026-09-18')
            self.assertFalse(model['valid'])
            self.assertTrue(model['provisional'])
            self.assertEqual(model['days'],2)
            self.assertIn(key,model['cells'])
            self.assertGreater(model['cells'][key][0],.5)
            self.assertLess(model['cells'][key][0],1)

    def test_search_initials_present_for_loading_and_cached_results(self):
        with tempfile.TemporaryDirectory() as directory:
            service=MinutePredictions(Path(directory)/'test.sqlite', lambda:{'stocks':[]}, None)
            with service.db() as db:
                db.execute('CREATE TABLE custom_blacklist(code TEXT,active INTEGER)')
            service.members={'688361':{'name':'中科飞测','nameInitials':'zkfc'},
                             '688114':{'name':'华大智造','nameInitials':'hdzz'}}
            service.results={'688361':{'code':'688361','name':'中科飞测','status':'ready'}}
            rows={s['code']:s for s in service.view()['stocks']}
            self.assertEqual(rows['688361']['nameInitials'],'zkfc')
            self.assertEqual(rows['688114']['nameInitials'],'hdzz')
            self.assertEqual(rows['688114']['status'],'loading')

if __name__=='__main__':
    unittest.main()
