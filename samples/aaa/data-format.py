# encoding: utf-8
import pandas
import rqdatac
from rqdatac.services.get_price import get_price

username = 'license'
password = 'HpLzgGU1zWSCxNLe7BOC9C32UiyxnVgXQLsry2BFPM2SNM_IzTTQ5CH0G5uBOqafQyqnNU5o_yw7tvWUxqmo3rnUoVnWd4ZK5iOKJuronyldtzUp_H9x0T46pwd_2uAD-mgO7L1S02E9HDTt8UbAmZ2qLpUa4ooYYLcvJMOT0G4=MbtfcvU2IY7IUcLp4FYnzpGnB-F4hp0Z0a6QCY_twwJJpikFwPu5Aklh0Br-3reQ0rg2xlCzaxbg-T6ECAV0ipTsONaxT61vOtnbnsVO1UH7QFPt8Mel46OxEcjN2O-2CIl694iX2_A-tf-cWaCeg5FRtabQWf6oG7KJj7bdqiY='


def format_data(df_in):
    # datetime,open,high,low,close,volume,openinterest
    df_in = df_in.reset_index()
    df_in.rename(columns={'open_interest': 'openinterest', 'date': 'datetime'}, inplace=True)
    df_in = df_in.loc[:, ['datetime', 'open', 'high', 'low', 'close', 'volume',
                          'openinterest']]
    df_in.set_index('datetime', inplace=True)
    print(f'共读取 {df_in.shape[0]} 根K线')

    return df_in


def mark_tp(df_in):
    # ----------------------------------------
    # 过滤内包K线和外包K线
    # ----------------------------------------
    valid_bars = []
    for index, row in df_in.iterrows():
        if len(valid_bars) == 0:
            valid_bars.append([index, row, 0])
            continue

        high = row['high']
        low = row['low']
        prev_high = valid_bars[-1][1]['high']
        prev_low = valid_bars[-1][1]['low']

        # 判断是不是内包: 如果是无须任何操作，continue
        if high <= prev_high and low >= prev_low:
            continue

        # 判断是不是外包
        for _ in range(12):
            if ((prev_high < high and prev_low >= low) or
                    (prev_high <= high and prev_low > low)):
                valid_bars.pop()
            else:
                break
            prev_high = valid_bars[-1][1]['high']
            prev_low = valid_bars[-1][1]['low']

        # 有效K线: append
        valid_bars.append([index, row])

    # ----------------------------------------
    # 标注拐点（高/低点）
    # ----------------------------------------
    # 将所有非内保的数据放到一个List当出现拐点的地方标注高低点
    short_upper_tp = []
    short_lower_tp = []

    medium_upper_tp = []
    medium_lower_tp = []

    long_upper_tp = []
    long_lower_tp = []

    for i in range(1, len(valid_bars) - 1):
        prev_row = valid_bars[i - 1][1]
        curr_row = valid_bars[i][1]
        next_row = valid_bars[i + 1][1]

        prev_high = prev_row['high']
        prev_low = prev_row['low']

        curr_high = curr_row['high']
        curr_low = curr_row['low']

        next_high = next_row['high']
        next_low = next_row['low']

        # 判断上拐点
        up_left = (prev_high <= curr_high and prev_low < curr_low)
        up_right = (next_high <= curr_high and next_low < curr_low)
        if up_left and up_right:
            # 短期上拐点
            # 信号1
            valid_bars[i][2] = 1
            short_upper_tp.append(valid_bars[i][2])

            # 中期上拐点
            if len(short_upper_tp) >= 3:
                left_point = short_upper_tp[-3]
                mid_point = short_upper_tp[-2]
                right_point = short_upper_tp[-1]
                if (mid_point[1]['high'] >= left_point[1]['high']
                        and mid_point[1]['high'] >= right_point[1]['high']):
                    # 信号2
                    medium_upper_tp.append(mid_point)
                    if () and ():
                        pass

            # 长期上拐点
            if len(medium_upper_tp) >= 3:
                pass

        # 判断下拐点
        down_left = (prev_high > curr_high and prev_low >= prev_low)
        down_right = (next_high > curr_high and next_low >= curr_low)
        if down_left and down_right:
            valid_bars[i][2] = -1
            short_lower_tp.append(valid_bars[i][2])

    return df_in


if __name__ == '__main__':
    instrument = 'IF88'
    from_date = '20000308'
    to_date = '20230908'
    period = '1d'

    rqdatac.init(username, password)
    df = get_price(instrument, from_date, to_date, period)
    df = format_data(df)
    df.to_csv(f"{instrument}_{from_date}_{to_date}_{period}.csv")

    # 验证：周期越小，无效(内包/外包)K线越多
