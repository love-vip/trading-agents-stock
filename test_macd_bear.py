"""Isolated regression checks: never write the production stock database."""
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import market_server as market
from macd_bear import MacdBearService, RULES


class BearTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.mirrored = []
        api = dict(vars(market), BACKTEST_DB_PATH=Path(self.temp.name)/'test.sqlite',
                   record_market_alert=self.mirrored.append)
        self.service = MacdBearService(api)
        with self.service.connect() as db:
            db.execute('CREATE TABLE daily_close_quotes (trade_date TEXT, code TEXT, quote_json TEXT)')
        self.now = datetime(2026, 10, 19, 10, 0, tzinfo=market.CHINA_TZ)
        self.stock = dict(code='300432', name='测试', price=10, changePct=3,
                          macdSelectionRules=[RULES[0]], industry='测试概念')

    def tearDown(self):
        self.temp.cleanup()

    def emit(self, previous=None, **changes):
        stock = dict(self.stock, **changes)
        with self.service.connect() as db:
            self.service.record_alerts(db, '2026-10-19', self.now, previous or {}, {'300432': stock})
            return db.execute('SELECT COUNT(*) FROM macd_bear_alert_history').fetchone()[0]

    def test_alert_lifecycle_and_unavailable(self):
        self.assertEqual(self.emit(quoteUnavailable=True), 0)
        self.assertEqual(self.emit(), 1)
        self.assertEqual(self.emit({'300432': self.stock}, changePct=3.9), 1)
        self.assertEqual(self.emit({'300432': self.stock}, changePct=4.1), 2)
        self.assertEqual(self.emit({}, changePct=4.1), 3)
        self.assertEqual(self.emit({'300432': self.stock}, changePct=20), 3)
        with self.service.connect() as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='market_alert_history'").fetchone())

    def test_rules_require_bearish_and_min_price(self):
        with patch.object(self.service.api, 'macd_is_bearish_arrangement', return_value=False):
            self.assertEqual(self.service.rules([10]*40, 10, 3)[0], [])
        with patch.object(self.service.api, 'macd_is_bearish_arrangement', return_value=True), \
             patch.object(self.service.api, 'macd_flip_strength_passes', side_effect=lambda _s, _f, _sd, _fd, change: (False, change >= 0, False)):
            self.assertEqual(self.service.rules([10]*40, 10, 3)[0], [RULES[0]])
            self.assertEqual(self.service.rules([10]*40, 4.99, 3)[0], [])
            self.assertEqual(self.service.rules([10]*40, 10, -.1)[0], [])

    def test_rules_include_normal_dual_red_conditions_inside_bear_pool(self):
        with patch.object(self.service.api, 'macd_is_bearish_arrangement', return_value=True), \
             patch.object(self.service.api, 'macd_flip_strength_passes', return_value=(False,False,False)), \
             patch.object(self.service.api, 'macd_dual_red_rule', return_value='双线连红3日+涨5%'):
            rules, _, _ = self.service.rules([10] * 40, 10.5, 2)
        self.assertEqual(rules, ['双线连红3日+涨5%'])

    def test_history_filter_count_dedupe(self):
        self.emit()
        self.emit({}, changePct=4.5)
        with self.service.connect() as db:
            db.execute('INSERT INTO macd_bear_members VALUES (?,?,?,?)', ('2026-10-19','300432',json.dumps(self.stock),self.now.isoformat()))
        q={'date':['2026-10-19'],'includeDrawdowns':['1'],'direction':['all'],'signalType':['all']}
        self.assertEqual(self.service.history(q)['total'],2)
        result=self.service.history(dict(q,dedupe=['earliest']))
        self.assertEqual(result['total'],1)
        self.assertEqual(result['rows'][0]['changePct'],3)
        self.assertEqual(result['signalCounts']['双线同步翻红'],1)

    def test_only_m_signal_can_reach_shared_alert_stream(self):
        self.emit(macdSelectionRules=[RULES[0]])
        self.service.mirror_pending_m_alerts('2026-10-19')
        self.assertEqual(self.mirrored, [])

        self.emit({}, macdSelectionRules=[RULES[2]], changePct=4.5)
        self.service.mirror_pending_m_alerts('2026-10-19')
        self.assertEqual(len(self.mirrored), 1)
        event = self.mirrored[0]
        self.assertEqual(event['pool'], 'MACD空头')
        self.assertEqual(event['signal'], '快线连红 ≥2 日 + 慢线翻红')
        self.assertTrue(event['id'].startswith('macd-bear-'))

        # Acknowledged events cannot produce a second top-frame notification.
        self.service.mirror_pending_m_alerts('2026-10-19')
        self.assertEqual(len(self.mirrored), 1)


if __name__ == '__main__':
    unittest.main()
