# coding: utf-8

import argparse
import datetime

import pandas as pd

import backtrader as bt
import backtrader.feeds as btfeeds


class ShortTermTradingStrategy(bt.Strategy):
    params = (
        ('period', 5),
        ('stake', 1),
    )

    def log(self, txt, dt=None):
        dt = dt or self.data.datetime[0]
        dt = bt.num2date(dt)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # To control operation entries
        self.orderid = None
        self.data0_valid_bars = pd.DataFrame(
            columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'turningpoint'])
        self.data0_upper_tp1 = pd.DataFrame(
            columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'turningpoint'])
        self.data0_lower_tp1 = pd.DataFrame(
            columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'turningpoint'])
        self.data0_upper_tp2 = pd.DataFrame(
            columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'turningpoint'])
        self.data0_lower_tp2 = pd.DataFrame(
            columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'turningpoint'])
        self.data0_upper_tp3 = pd.DataFrame(
            columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'turningpoint'])
        self.data0_lower_tp3 = pd.DataFrame(
            columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'turningpoint'])

        self.data0_valid_bars.set_index('datetime')
        self.data0_upper_tp1.set_index('datetime')
        self.data0_lower_tp1.set_index('datetime')
        self.data0_upper_tp2.set_index('datetime')
        self.data0_lower_tp2.set_index('datetime')
        self.data0_upper_tp3.set_index('datetime')
        self.data0_lower_tp3.set_index('datetime')

    def next(self):
        # print('next::current period:', len(self.data0))

        if self.orderid:
            return

        # 信号：中期拐点生成时，且长期拐点方向一致
        bar = dict(datetime=bt.num2date(self.data0.datetime[0]),
                   open=self.data0.open[0],
                   high=self.data0.high[0],
                   low=self.data0.low[0],
                   close=self.data0.close[0],
                   volume=self.data0.volume[0],
                   openinterest=self.data0.openinterest[0],
                   turningpoint=0)

        if self.data0_valid_bars.shape[0] == 0:
            self.data0_valid_bars.loc[len(self.data0_valid_bars)] = bar
            return
        # -------------------------------------------------
        # 剔除内外包K线
        # -------------------------------------------------
        high = bar['high']
        low = bar['low']

        prev_bar = self.data0_valid_bars.iloc[-1]
        prev_high = prev_bar['high']
        prev_low = prev_bar['low']

        # 如果是内包K线
        if high <= prev_high and low >= prev_low:
            return

        # 如果是外包K线
        for _ in range(12):
            if ((prev_high < high and prev_low >= low) or
                    (prev_high <= high and prev_low > low)):
                self.data0_valid_bars.drop(self.data0_valid_bars.tail(1).index)
                # self.valid_bars = self.valid_bars.iloc[:-1]
            else:
                break

            prev_bar = self.data0_valid_bars.iloc[-1]
            prev_high = prev_bar['high']
            prev_low = prev_bar['low']

        self.data0_valid_bars.loc[len(self.data0_valid_bars)] = bar

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
        upper_left = (pprev_high <= prev_high and pprev_low < prev_low)
        upper_right = (curr_high <= prev_high and curr_low < prev_low)
        print(upper_left, upper_right)
        if upper_left and upper_right:
            print('00000')
            self.data0_upper_tp1.loc[len(self.data0_upper_tp1)] = prev_bar
            # -------------------------------------------------
            # 判断是否有中期上拐点形成
            # -------------------------------------------------
            print('11111')
            if self.data0_upper_tp1.shape[0] < 3:
                return

            upper_tp1_bar = self.data0_upper_tp1.iloc[-1]
            prev_upper_tp1_bar = self.data0_upper_tp1.iloc[-2]
            pprev_upper_tp1_bar = self.data0_upper_tp1.iloc[-3]

            if (prev_upper_tp1_bar['high'] >= pprev_upper_tp1_bar['high']
                    and prev_upper_tp1_bar['high'] >= upper_tp1_bar['high']):
                self.data0_upper_tp2.loc[len(self.data0_upper_tp2)] = prev_upper_tp1_bar
                # -------------------------------------------------
                # 判断是否有长期上拐点形成
                # -------------------------------------------------
                if self.data0_upper_tp2.shape[0] < 3:
                    return

                upper_tp2_bar = self.data0_upper_tp2.iloc[-1]
                prev_upper_tp2_bar = self.data0_upper_tp2.iloc[-2]
                pprev_upper_tp2_bar = self.data0_upper_tp2.iloc[-3]

                if (prev_upper_tp2_bar['high'] >= pprev_upper_tp2_bar['high']
                        and prev_upper_tp2_bar['high'] >= upper_tp2_bar['high']):
                    self.data0_upper_tp3.loc[len(self.data0_upper_tp3)] = prev_upper_tp2_bar

        # -------------------------------------------------
        # 判断是否有短期下拐点形成
        # -------------------------------------------------
        lower_left = (pprev_high > prev_high and pprev_low >= prev_low)
        lower_right = (high > prev_high and low >= prev_low)
        if lower_left and lower_right:
            self.data0_lower_tp1.loc[len(self.data0_lower_tp1)] = prev_bar
            # -------------------------------------------------
            # 判断是否有中期下拐点形成
            # -------------------------------------------------
            if self.data0_lower_tp1.shape[0] < 3:
                return

            lower_tp1_bar = self.data0_lower_tp1.iloc[-1]
            prev_lower_tp1_bar = self.data0_lower_tp1.iloc[-2]
            pprev_lower_tp1_bar = self.data0_lower_tp1.iloc[-3]
            if (prev_lower_tp1_bar['low'] <= pprev_lower_tp1_bar['low']
                    and prev_lower_tp1_bar['low'] <= lower_tp1_bar['low']):
                self.data0_lower_tp2.loc[len(self.data0_lower_tp2)] = prev_lower_tp1_bar
                # -------------------------------------------------
                # 判断是否有长期下拐点形成
                # -------------------------------------------------
                if self.data0_lower_tp2.shape[0] < 3:
                    return

                lower_tp2_bar = self.data0_lower_tp2.iloc[-1]
                prev_lower_tp2_bar = self.data0_lower_tp2.iloc[-2]
                pprev_lower_tp2_bar = self.data0_lower_tp2.iloc[-3]

                if (prev_lower_tp2_bar['low'] <= pprev_lower_tp2_bar['low']
                        and prev_lower_tp2_bar['low'] <= lower_tp2_bar['low']):
                    self.data0_lower_tp3.loc[len(self.data0_lower_tp3)] = prev_lower_tp2_bar

    def stop(self):
        print(f'valid bars: {self.data0_valid_bars.shape}')
        print(f'data0_upper_tp1: {self.data0_upper_tp1}')
        print(f'data0_lower_tp1: {self.data0_lower_tp1}')
        print(f'data0_upper_tp2: {self.data0_upper_tp2}')
        print(f'data0_lower_tp2: {self.data0_lower_tp2}')
        print(f'data0_upper_tp3: {self.data0_upper_tp3}')
        print(f'data0_lower_tp3: {self.data0_lower_tp3}')

        print('==================================================')
        print('Starting Value - %.2f' % self.broker.startingcash)
        print('Ending   Value - %.2f' % self.broker.getvalue())
        print('==================================================')


def runstrat():
    args = parse_args()

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(30000.0)
    cerebro.addstrategy(
        ShortTermTradingStrategy,

        # args for the strategy
        period=args.period,
    )

    # Load the Data
    datapath = 'SA88_20191201_20230908_1d.csv'
    data0 = btfeeds.GenericCSVData(dataname=datapath,
                                   dtformat='%Y-%m-%d',
                                   fromdate=datetime.datetime(2019, 12, 1),
                                   todate=datetime.datetime(2023, 9, 30),
                                   timeframe=bt.TimeFrame.Minutes,
                                   compression=1)
    cerebro.adddata(data0, name='15m')

    # datapath = 'SA88_20191201_20230908_1d.csv'
    # data1 = btfeeds.GenericCSVData(dataname=datapath,
    #                                dtformat='%Y-%m-%d',
    #                                fromdate=datetime.datetime(2019, 12, 1),
    #                                todate=datetime.datetime(2023, 9, 30),
    #                                timeframe=bt.TimeFrame.Days,
    #                                compression=1)
    # cerebro.adddata(data1, name='1d')

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
