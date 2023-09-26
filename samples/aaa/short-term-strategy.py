# coding: utf-8

import argparse
import datetime

import pandas as pd

import backtrader as bt
import backtrader.feeds as btfeeds


class ShortTermTradingStrategy(bt.Strategy):
    params = (
        ('period', 20),
    )

    def log0(self, txt, dt=None):
        dt = dt or self.data0.datetime[0]
        dt = bt.num2date(dt)
        print('data0 - %s - %s' % (dt.isoformat(), txt))

    def log1(self, txt, dt=None):
        dt = dt or self.data1.datetime[0]
        dt = bt.num2date(dt)
        print('data1 - %s - %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def __init__(self):
        columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest']
        self.data0_valid_bars = pd.DataFrame(columns=columns)
        self.data0_upper_tp1 = pd.DataFrame(columns=columns)
        self.data0_lower_tp1 = pd.DataFrame(columns=columns)
        self.data0_upper_tp2 = pd.DataFrame(columns=columns)
        self.data0_lower_tp2 = pd.DataFrame(columns=columns)
        self.data0_upper_tp3 = pd.DataFrame(columns=columns)
        self.data0_lower_tp3 = pd.DataFrame(columns=columns)

        self.data1_valid_bars = pd.DataFrame(columns=columns)
        self.data1_upper_tp1 = pd.DataFrame(columns=columns)
        self.data1_lower_tp1 = pd.DataFrame(columns=columns)
        self.data1_upper_tp2 = pd.DataFrame(columns=columns)
        self.data1_lower_tp2 = pd.DataFrame(columns=columns)
        self.data1_upper_tp3 = pd.DataFrame(columns=columns)
        self.data1_lower_tp3 = pd.DataFrame(columns=columns)

        self.boll0 = bt.indicators.BollingerBands(self.data0)
        self.boll1 = bt.indicators.BollingerBands(self.data1)

        self.data1_last_datetime = None
        self.data0_last_datetime = None

        # To control operation entries
        self.orderid = None
        self.buyprice = None
        self.buycomm = None

    def data0_next(self):
        curr_bar = dict(datetime=bt.num2date(self.data0.datetime[0]),
                        open=self.data0.open[0],
                        high=self.data0.high[0],
                        low=self.data0.low[0],
                        close=self.data0.close[0],
                        volume=self.data0.volume[0],
                        openinterest=self.data0.openinterest[0])

        if self.data0_valid_bars.shape[0] == 0:
            self.data0_valid_bars.loc[len(self.data0_valid_bars)] = curr_bar
            return

        # -------------------------------------------------
        # 剔除内包K线
        # -------------------------------------------------
        prev_bar = self.data0_valid_bars.iloc[-1]
        if curr_bar['high'] <= prev_bar['high'] and curr_bar['low'] >= prev_bar['low']:
            self.log0('内包，当前K线无效')
            return

        # -------------------------------------------------
        # 剔除外包K线
        # -------------------------------------------------
        for _ in range(12):
            if ((prev_bar['high'] < curr_bar['high'] and prev_bar['low'] >= curr_bar['low']) or
                    (prev_bar['high'] <= curr_bar['high'] and prev_bar['low'] > curr_bar['low'])):

                # -------------------------------------------------
                # 剔除已添加K线前，清理短中长期拐点
                # -------------------------------------------------
                if self.data0_upper_tp1.shape[0] >= 1:
                    last_upper_tp1 = self.data0_upper_tp1.iloc[-1]
                    if last_upper_tp1['datetime'] == prev_bar['datetime']:
                        self.data0_upper_tp1 = self.data0_upper_tp1[:-1]
                        self.log0('剔除被外包的短期上拐点K线')

                if self.data0_upper_tp2.shape[0] >= 1:
                    last_upper_tp2 = self.data0_upper_tp2.iloc[-1]
                    if last_upper_tp2['datetime'] == prev_bar['datetime']:
                        self.data0_upper_tp2 = self.data0_upper_tp2[:-1]
                        self.log0('剔除被外包的中期上拐点K线')

                if self.data0_upper_tp3.shape[0] >= 1:
                    last_upper_tp3 = self.data0_upper_tp3.iloc[-1]
                    if last_upper_tp3['datetime'] == prev_bar['datetime']:
                        self.data0_upper_tp3 = self.data0_upper_tp3[:-1]
                        self.log0('剔除被外包的长期上拐点K线')

                if self.data0_lower_tp1.shape[0] >= 1:
                    last_lower_tp1 = self.data0_lower_tp1.iloc[-1]
                    if last_lower_tp1['datetime'] == prev_bar['datetime']:
                        self.data0_lower_tp1 = self.data0_lower_tp1[:-1]
                        self.log0('剔除被外包的短期下拐点K线')

                if self.data0_lower_tp2.shape[0] >= 1:
                    last_lower_tp2 = self.data0_lower_tp2.iloc[-1]
                    if last_lower_tp2['datetime'] == prev_bar['datetime']:
                        self.data0_lower_tp2 = self.data0_lower_tp2[:-1]
                        self.log0('剔除被外包的中期下拐点K线')

                if self.data0_lower_tp3.shape[0] >= 1:
                    last_lower_tp3 = self.data0_lower_tp3.iloc[-1]
                    if last_lower_tp3['datetime'] == prev_bar['datetime']:
                        self.data0_lower_tp3 = self.data0_lower_tp3[:-1]
                        self.log0('剔除被外包的长期下拐点K线')

                self.data0_valid_bars = self.data0_valid_bars.iloc[:-1]
                self.log0('外包，弹出前一K线')

            else:
                break

            prev_bar = self.data0_valid_bars.iloc[-1]

        self.data0_valid_bars.loc[len(self.data0_valid_bars)] = curr_bar

        # -------------------------------------------------
        # 标注短、中、长期拐点
        # -------------------------------------------------
        if self.data0_valid_bars.shape[0] < 3:
            return

        curr_bar = self.data0_valid_bars.iloc[-1]
        prev_bar = self.data0_valid_bars.iloc[-2]
        pprev_bar = self.data0_valid_bars.iloc[-3]

        curr_high = curr_bar['high']
        curr_low = curr_bar['low']
        prev_high = prev_bar['high']
        prev_low = prev_bar['low']
        pprev_high = pprev_bar['high']
        pprev_low = pprev_bar['low']

        # -------------------------------------------------
        # 判断是否有短期上拐点形成
        # -------------------------------------------------
        if ((pprev_high <= prev_high and pprev_low < prev_low)
                and (curr_high <= prev_high and curr_low < prev_low)):
            # 避免已确定为拐点的K线，因后续的出现内包外包而重复添加
            if self.data0_upper_tp1.shape[0] < 1:
                self.data0_upper_tp1.loc[len(self.data0_upper_tp1)] = prev_bar
            else:
                last_upper_tp1 = self.data0_upper_tp1.iloc[-1]
                if last_upper_tp1['datetime'] != prev_bar['datetime']:
                    self.data0_upper_tp1.loc[len(self.data0_upper_tp1)] = prev_bar

            # -------------------------------------------------
            # 判断是否有中期上拐点形成
            # -------------------------------------------------
            if self.data0_upper_tp1.shape[0] < 3:
                return

            curr_upper_tp1 = self.data0_upper_tp1.iloc[-1]
            prev_upper_tp1 = self.data0_upper_tp1.iloc[-2]
            pprev_upper_tp1 = self.data0_upper_tp1.iloc[-3]

            if (prev_upper_tp1['high'] >= pprev_upper_tp1['high']
                    and prev_upper_tp1['high'] >= curr_upper_tp1['high']):

                if self.data0_upper_tp2.shape[0] < 1:
                    self.data0_upper_tp2.loc[len(self.data0_upper_tp2)] = prev_upper_tp1
                else:
                    last_upper_tp2 = self.data0_upper_tp2.iloc[-1]
                    if last_upper_tp2['datetime'] != prev_upper_tp1['datetime']:
                        self.data0_upper_tp2.loc[len(self.data0_upper_tp1)] = prev_upper_tp1

                # -------------------------------------------------
                # 判断是否有长期上拐点形成
                # -------------------------------------------------
                if self.data0_upper_tp2.shape[0] < 3:
                    return

                curr_upper_tp2 = self.data0_upper_tp2.iloc[-1]
                prev_upper_tp2 = self.data0_upper_tp2.iloc[-2]
                pprev_upper_tp2 = self.data0_upper_tp2.iloc[-3]

                if (prev_upper_tp2['high'] >= pprev_upper_tp2['high']
                        and prev_upper_tp2['high'] >= curr_upper_tp2['high']):
                    if self.data0_upper_tp3.shape[0] < 1:
                        self.data0_upper_tp3.loc[len(self.data0_upper_tp3)] = prev_upper_tp2
                    else:
                        last_upper_tp3 = self.data0_upper_tp3.iloc[-1]
                        if last_upper_tp3['datetime'] != prev_upper_tp2['datetime']:
                            self.data0_upper_tp3.loc[len(self.data0_upper_tp3)] = prev_upper_tp2

        # -------------------------------------------------
        # 判断是否有短期下拐点形成
        # -------------------------------------------------
        if ((pprev_high > prev_high and pprev_low >= prev_low)
                and (curr_high > prev_high and curr_low >= prev_low)):

            if self.data0_lower_tp1.shape[0] < 1:
                self.data0_lower_tp1.loc[len(self.data0_lower_tp1)] = prev_bar
            else:
                last_lower_tp1 = self.data0_lower_tp1.iloc[-1]
                if last_lower_tp1['datetime'] != prev_bar['datetime']:
                    self.data0_lower_tp1.loc[len(self.data0_lower_tp1)] = prev_bar

            # -------------------------------------------------
            # 判断是否有中期下拐点形成
            # -------------------------------------------------
            if self.data0_lower_tp1.shape[0] < 3:
                return

            curr_lower_tp1 = self.data0_lower_tp1.iloc[-1]
            prev_lower_tp1 = self.data0_lower_tp1.iloc[-2]
            pprev_lower_tp1 = self.data0_lower_tp1.iloc[-3]

            if ((prev_lower_tp1['low'] <= pprev_lower_tp1['low'])
                    and (prev_lower_tp1['low'] <= curr_lower_tp1['low'])):

                if self.data0_lower_tp2.shape[0] < 1:
                    self.data0_lower_tp2.loc[len(self.data0_lower_tp2)] = prev_lower_tp1
                else:
                    last_lower_tp2 = self.data0_lower_tp2.iloc[-1]
                    if last_lower_tp2['datetime'] != prev_lower_tp1['datetime']:
                        self.data0_lower_tp2.loc[len(self.data0_lower_tp2)] = prev_lower_tp1
                # -------------------------------------------------
                # 判断是否有长期下拐点形成
                # -------------------------------------------------
                if self.data0_lower_tp2.shape[0] < 3:
                    return

                curr_lower_tp2 = self.data0_lower_tp2.iloc[-1]
                prev_lower_tp2 = self.data0_lower_tp2.iloc[-2]
                pprev_lower_tp2 = self.data0_lower_tp2.iloc[-3]

                if ((prev_lower_tp2['low'] <= pprev_lower_tp2['low'])
                        and (prev_lower_tp2['low'] <= curr_lower_tp2['low'])):

                    if self.data0_lower_tp3.shape[0] < 1:
                        self.data0_lower_tp3.loc[len(self.data0_lower_tp3)] = prev_lower_tp2
                    else:
                        last_lower_tp3 = self.data0_lower_tp3.iloc[-1]
                        if last_lower_tp3['datetime'] != prev_lower_tp2['datetime']:
                            self.data0_lower_tp3.loc[len(self.data0_lower_tp3)] = prev_lower_tp2

    def data1_next(self):
        curr_bar = dict(datetime=bt.num2date(self.data1.datetime[0]),
                        open=self.data1.open[0],
                        high=self.data1.high[0],
                        low=self.data1.low[0],
                        close=self.data1.close[0],
                        volume=self.data1.volume[0],
                        openinterest=self.data1.openinterest[0])

        if self.data1_valid_bars.shape[0] == 0:
            self.data1_valid_bars.loc[len(self.data1_valid_bars)] = curr_bar
            return

        # -------------------------------------------------
        # 剔除内包K线
        # -------------------------------------------------
        prev_bar = self.data1_valid_bars.iloc[-1]
        if curr_bar['high'] <= prev_bar['high'] and curr_bar['low'] >= prev_bar['low']:
            self.log1('内包，当前K线无效')
            return

        # -------------------------------------------------
        # 剔除外包K线
        # -------------------------------------------------
        for _ in range(12):
            if ((prev_bar['high'] < curr_bar['high'] and prev_bar['low'] >= curr_bar['low']) or
                    (prev_bar['high'] <= curr_bar['high'] and prev_bar['low'] > curr_bar['low'])):

                # -------------------------------------------------
                # 剔除已添加K线前，清理短中长期拐点
                # -------------------------------------------------
                if self.data1_upper_tp1.shape[0] >= 1:
                    last_upper_tp1 = self.data1_upper_tp1.iloc[-1]
                    if last_upper_tp1['datetime'] == prev_bar['datetime']:
                        self.data1_upper_tp1 = self.data1_upper_tp1[:-1]
                        self.log1('剔除被外包的短期上拐点K线')

                if self.data1_upper_tp2.shape[0] >= 1:
                    last_upper_tp2 = self.data1_upper_tp2.iloc[-1]
                    if last_upper_tp2['datetime'] == prev_bar['datetime']:
                        self.data1_upper_tp2 = self.data1_upper_tp2[:-1]
                        self.log1('剔除被外包的中期上拐点K线')

                if self.data1_upper_tp3.shape[0] >= 1:
                    last_upper_tp3 = self.data1_upper_tp3.iloc[-1]
                    if last_upper_tp3['datetime'] == prev_bar['datetime']:
                        self.data1_upper_tp3 = self.data1_upper_tp3[:-1]
                        self.log1('剔除被外包的长期上拐点K线')

                if self.data1_lower_tp1.shape[0] >= 1:
                    last_lower_tp1 = self.data1_lower_tp1.iloc[-1]
                    if last_lower_tp1['datetime'] == prev_bar['datetime']:
                        self.data1_lower_tp1 = self.data1_lower_tp1[:-1]
                        self.log1('剔除被外包的短期下拐点K线')

                if self.data1_lower_tp2.shape[0] >= 1:
                    last_lower_tp2 = self.data1_lower_tp2.iloc[-1]
                    if last_lower_tp2['datetime'] == prev_bar['datetime']:
                        self.data1_lower_tp2 = self.data1_lower_tp2[:-1]
                        self.log1('剔除被外包的中期下拐点K线')

                if self.data1_lower_tp3.shape[0] >= 1:
                    last_lower_tp3 = self.data1_lower_tp3.iloc[-1]
                    if last_lower_tp3['datetime'] == prev_bar['datetime']:
                        self.data1_lower_tp3 = self.data1_lower_tp3[:-1]
                        self.log1('剔除被外包的长期下拐点K线')

                self.data1_valid_bars = self.data1_valid_bars.iloc[:-1]
                self.log1('外包，弹出前一K线')

            else:
                break

            prev_bar = self.data1_valid_bars.iloc[-1]

        self.data1_valid_bars.loc[len(self.data1_valid_bars)] = curr_bar

        # -------------------------------------------------
        # 标注短、中、长期拐点
        # -------------------------------------------------
        if self.data1_valid_bars.shape[0] < 3:
            return

        curr_bar = self.data1_valid_bars.iloc[-1]
        prev_bar = self.data1_valid_bars.iloc[-2]
        pprev_bar = self.data1_valid_bars.iloc[-3]

        curr_high = curr_bar['high']
        curr_low = curr_bar['low']
        prev_high = prev_bar['high']
        prev_low = prev_bar['low']
        pprev_high = pprev_bar['high']
        pprev_low = pprev_bar['low']

        # -------------------------------------------------
        # 判断是否有短期上拐点形成
        # -------------------------------------------------
        if ((pprev_high <= prev_high and pprev_low < prev_low)
                and (curr_high <= prev_high and curr_low < prev_low)):
            # 避免已确定为拐点的K线，因后续的出现内包外包而重复添加
            if self.data1_upper_tp1.shape[0] < 1:
                self.data1_upper_tp1.loc[len(self.data1_upper_tp1)] = prev_bar
            else:
                last_upper_tp1 = self.data1_upper_tp1.iloc[-1]
                if last_upper_tp1['datetime'] != prev_bar['datetime']:
                    self.data1_upper_tp1.loc[len(self.data1_upper_tp1)] = prev_bar

            # -------------------------------------------------
            # 判断是否有中期上拐点形成
            # -------------------------------------------------
            if self.data1_upper_tp1.shape[0] < 3:
                return

            curr_upper_tp1 = self.data1_upper_tp1.iloc[-1]
            prev_upper_tp1 = self.data1_upper_tp1.iloc[-2]
            pprev_upper_tp1 = self.data1_upper_tp1.iloc[-3]

            if (prev_upper_tp1['high'] >= pprev_upper_tp1['high']
                    and prev_upper_tp1['high'] >= curr_upper_tp1['high']):

                if self.data1_upper_tp2.shape[0] < 1:
                    self.data1_upper_tp2.loc[len(self.data1_upper_tp2)] = prev_upper_tp1
                else:
                    last_upper_tp2 = self.data1_upper_tp2.iloc[-1]
                    if last_upper_tp2['datetime'] != prev_upper_tp1['datetime']:
                        self.data1_upper_tp2.loc[len(self.data1_upper_tp1)] = prev_upper_tp1

                # -------------------------------------------------
                # 判断是否有长期上拐点形成
                # -------------------------------------------------
                if self.data1_upper_tp2.shape[0] < 3:
                    return

                curr_upper_tp2 = self.data1_upper_tp2.iloc[-1]
                prev_upper_tp2 = self.data1_upper_tp2.iloc[-2]
                pprev_upper_tp2 = self.data1_upper_tp2.iloc[-3]

                if (prev_upper_tp2['high'] >= pprev_upper_tp2['high']
                        and prev_upper_tp2['high'] >= curr_upper_tp2['high']):
                    if self.data1_upper_tp3.shape[0] < 1:
                        self.data1_upper_tp3.loc[len(self.data1_upper_tp3)] = prev_upper_tp2
                    else:
                        last_upper_tp3 = self.data1_upper_tp3.iloc[-1]
                        if last_upper_tp3['datetime'] != prev_upper_tp2['datetime']:
                            self.data1_upper_tp3.loc[len(self.data1_upper_tp3)] = prev_upper_tp2

        # -------------------------------------------------
        # 判断是否有短期下拐点形成
        # -------------------------------------------------
        if ((pprev_high > prev_high and pprev_low >= prev_low)
                and (curr_high > prev_high and curr_low >= prev_low)):

            if self.data1_lower_tp1.shape[0] < 1:
                self.data1_lower_tp1.loc[len(self.data1_lower_tp1)] = prev_bar
            else:
                last_lower_tp1 = self.data1_lower_tp1.iloc[-1]
                if last_lower_tp1['datetime'] != prev_bar['datetime']:
                    self.data1_lower_tp1.loc[len(self.data1_lower_tp1)] = prev_bar

            # -------------------------------------------------
            # 判断是否有中期下拐点形成
            # -------------------------------------------------
            if self.data1_lower_tp1.shape[0] < 3:
                return

            curr_lower_tp1 = self.data1_lower_tp1.iloc[-1]
            prev_lower_tp1 = self.data1_lower_tp1.iloc[-2]
            pprev_lower_tp1 = self.data1_lower_tp1.iloc[-3]

            if ((prev_lower_tp1['low'] <= pprev_lower_tp1['low'])
                    and (prev_lower_tp1['low'] <= curr_lower_tp1['low'])):

                if self.data1_lower_tp2.shape[0] < 1:
                    self.data1_lower_tp2.loc[len(self.data1_lower_tp2)] = prev_lower_tp1
                else:
                    last_lower_tp2 = self.data1_lower_tp2.iloc[-1]
                    if last_lower_tp2['datetime'] != prev_lower_tp1['datetime']:
                        self.data1_lower_tp2.loc[len(self.data1_lower_tp2)] = prev_lower_tp1
                # -------------------------------------------------
                # 判断是否有长期下拐点形成
                # -------------------------------------------------
                if self.data1_lower_tp2.shape[0] < 3:
                    return

                curr_lower_tp2 = self.data1_lower_tp2.iloc[-1]
                prev_lower_tp2 = self.data1_lower_tp2.iloc[-2]
                pprev_lower_tp2 = self.data1_lower_tp2.iloc[-3]

                if ((prev_lower_tp2['low'] <= pprev_lower_tp2['low'])
                        and (prev_lower_tp2['low'] <= curr_lower_tp2['low'])):

                    if self.data1_lower_tp3.shape[0] < 1:
                        self.data1_lower_tp3.loc[len(self.data1_lower_tp3)] = prev_lower_tp2
                    else:
                        last_lower_tp3 = self.data1_lower_tp3.iloc[-1]
                        if last_lower_tp3['datetime'] != prev_lower_tp2['datetime']:
                            self.data1_lower_tp3.loc[len(self.data1_lower_tp3)] = prev_lower_tp2

    def next(self):
        print('next::current period:', len(self.data0))
        print('next::current period:', len(self.data1))

        # data0数据作为小周期数据，每次执行next都要做拐点判断
        if not self.data0_last_datetime or bt.num2date(self.data0.datetime[0]) != bt.num2date(self.data0_last_datetime):
            self.data0_last_datetime = self.data0.datetime[0]
            self.data0_next()

        # data1数据作为大周期数据，只有执行next时，日期发生改变才做拐点判断
        if not self.data1_last_datetime or int(self.data1.datetime[0]) != int(self.data1_last_datetime):
            self.data1_last_datetime = self.data1.datetime[0]
            self.data1_next()

        # if self.orderid:
        #     return

    def stop(self):
        print('==================================================')
        print('Starting Value - %.2f' % self.broker.startingcash)
        print('Ending   Value - %.2f' % self.broker.getvalue())
        print('==================================================')


def runstrat():
    args = parse_args()

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(100000.0)
    cerebro.addstrategy(
        ShortTermTradingStrategy,
        # args for the strategy
        period=args.period,
    )

    # Load the Data
    datapath = 'SA88_20191201_20230908_15m.csv'
    data0 = btfeeds.GenericCSVData(dataname=datapath,
                                   fromdate=datetime.datetime(2019, 12, 1),
                                   todate=datetime.datetime(2023, 9, 30),
                                   timeframe=bt.TimeFrame.Minutes,
                                   compression=1)
    cerebro.adddata(data0, name='15m')

    datapath = 'SA88_20191201_20230908_1d.csv'
    data1 = btfeeds.GenericCSVData(dataname=datapath,
                                   dtformat='%Y-%m-%d',
                                   fromdate=datetime.datetime(2019, 12, 1),
                                   todate=datetime.datetime(2023, 9, 30),
                                   timeframe=bt.TimeFrame.Days,
                                   compression=1)
    cerebro.adddata(data1, name='1d')

    cerebro.run()
    # cerebro.plot(style='bar')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Pandas test script')

    parser.add_argument('--period', default=20, required=False, type=int,
                        help='Period to apply to indicator')

    return parser.parse_args()


if __name__ == '__main__':
    runstrat()
