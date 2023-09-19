# encoding: utf-8
import pandas
import rqdatac
from rqdatac.services.get_price import get_price

username = 'license'
password = 'HpLzgGU1zWSCxNLe7BOC9C32UiyxnVgXQLsry2BFPM2SNM_IzTTQ5CH0G5uBOqafQyqnNU5o_yw7tvWUxqmo3rnUoVnWd4ZK5iOKJuronyldtzUp_H9x0T46pwd_2uAD-mgO7L1S02E9HDTt8UbAmZ2qLpUa4ooYYLcvJMOT0G4=MbtfcvU2IY7IUcLp4FYnzpGnB-F4hp0Z0a6QCY_twwJJpikFwPu5Aklh0Br-3reQ0rg2xlCzaxbg-T6ECAV0ipTsONaxT61vOtnbnsVO1UH7QFPt8Mel46OxEcjN2O-2CIl694iX2_A-tf-cWaCeg5FRtabQWf6oG7KJj7bdqiY='


def rqdata_download(order_book_id, start_date, end_date):
    rqdatac.init(username, password)

    #df = get_price(order_book_id, start_date, end_date, '1m')
    #df.to_csv(f'{order_book_id}_{start_date}_{end_date}_1m.csv')

    #df = get_price(order_book_id, start_date, end_date, '3m')
    #df.to_csv(f'{order_book_id}_{start_date}_{end_date}_3m.csv')

    df = get_price(order_book_id, start_date, end_date, '15m')
    df.to_csv(f'{order_book_id}_{start_date}_{end_date}_15m.csv')

    # df = get_price(order_book_id, start_date, end_date, '60m')
    # df.to_csv(f'{order_book_id}_{start_date}_{end_date}_1h.csv')

    #df = get_price(order_book_id, start_date, end_date, '1d')
    #df.to_csv(f'{order_book_id}_{start_date}_{end_date}_1d.csv')

    #df = get_price(order_book_id, start_date, end_date, '1w')
    #df.to_csv(f'{order_book_id}_{start_date}_{end_date}_1w.csv')

def mark_turning_point(filepath):
    df = pandas.read_csv(filepath)
    df.rename(columns={'open_interest': 'openinterest', 'date': 'datetime',
                       'dominant_id': 'dominant'}, inplace=True)
    # 选出7列
    # datetime,open,high,low,close,volume,openinterest
    df = df.loc[:, ['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest']]
    df['dominant']='SA88'
    # 增加三列：inside bar & turning point 1 2 3
    df.loc[:, 'inside'] = 0
    df.loc[:, 'outside'] = 0
    df.loc[:, 'turningpoint'] = 0

    total = df.shape[0]
    print(f'共读取 {total} 根K线')

    # ----------------------------------------
    # 第一步：判断被内部包含的K线并剔除
    # ----------------------------------------
    # 非内保K线
    valid_bars = []
    for index, row in df.iterrows():
        valid_bars.append([index, row, 0, 0, 0])

    for _ in range(10):
        temp_bars = []
        prev_high = valid_bars[0][1]['high']
        prev_low = valid_bars[0][1]['low']
        for i in range(1, len(valid_bars)):
            row = valid_bars[i][1]
            high = row['high']
            low = row['low']
            if high <= prev_high and low >= prev_low:
                # 标记被内包K线
                valid_bars[i][2] = 1
                df.iloc[valid_bars[i][0], 8] = 1

            prev_high = row['high']
            prev_low = row['low']

        for j in range(len(valid_bars)):
            if valid_bars[j][2] == 0:
                temp_bars.append(valid_bars[j])

        valid_bars = temp_bars

    be_inside = total - len(valid_bars)
    print(f'内包K线 {be_inside} 根，占比: {be_inside / total}')

    # ----------------------------------------
    # 第二步：判断"被"外部包含的K线并剔除
    # ----------------------------------------
    for _ in range(12):
        temp_bars = []
        for i in range(0, len(valid_bars) - 1):
            row = valid_bars[i][1]
            high = row['high']
            low = row['low']

            next_high = valid_bars[i + 1][1]['high']
            next_low = valid_bars[i + 1][1]['low']

            if (high < next_high and low >= next_low) or (high <= next_high and low > next_low):
                valid_bars[i][3] = 1
                df.iloc[valid_bars[i][0], 9] = 1

        for j in range(len(valid_bars)):
            if valid_bars[j][3] == 0:
                temp_bars.append(valid_bars[j])

        valid_bars = temp_bars

    print(f'非内包和非外包K线 {len(valid_bars)} 根，占比: {len(valid_bars) / total}')

    # ----------------------------------------
    # 第三步：标注拐点（高/低点）
    # ----------------------------------------
    # 将所有非内保的数据放到一个List当出现拐点的地方标注高低点
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
            valid_bars[i][4] = 1
            df.iloc[valid_bars[i][0], 10] = 1

        # 判断下拐点
        down_left = (prev_high > curr_high and prev_low >= prev_low)
        down_right = (next_high > curr_high and next_low >= curr_low)
        if down_left and down_right:
            valid_bars[i][4] = -1
            df.iloc[valid_bars[i][0], 10] = -1

    # ----------------------------------------
    # 找出两个同时为上拐点/下拐点的区间，遍历此区间找到最高或者最低的拐点
    # ----------------------------------------
    turning_points = []
    for i in range(0, len(valid_bars)):
        if valid_bars[i][4] != 0:
            turning_points.append(valid_bars[i])

    print(f'拐点个数: {len(turning_points)}')

    # ----------------------------------------
    # 第四步：标注中期拐点
    # ----------------------------------------
    # 遍历所有拐点，如果左右拐点都高于/低于当前拐点，当前拐点为中期拐点
    upper_turning_points2 = []
    lower_turning_points2 = []
    for index, row in df.iterrows():
        if row['turningpoint'] != 0:
            if row['turningpoint'] == 1:
                upper_turning_points2.append((index, row))
            if row['turningpoint'] == -1:
                lower_turning_points2.append((index, row))

    for i in range(1, len(upper_turning_points2) - 1):
        prev_point = upper_turning_points2[i - 1]
        curr_point = upper_turning_points2[i]
        next_point = upper_turning_points2[i + 1]

        if (curr_point[1]['high'] >= prev_point[1]['high']
                and curr_point[1]['high'] >= next_point[1]['high']):
            df.iloc[curr_point[0], 10] = 2

    for i in range(1, len(lower_turning_points2) - 1):
        prev_point = lower_turning_points2[i - 1]
        curr_point = lower_turning_points2[i]
        next_point = lower_turning_points2[i + 1]

        if (curr_point[1]['low'] <= prev_point[1]['low']
                and curr_point[1]['low'] <= next_point[1]['low']):
            df.iloc[curr_point[0], 10] = -2

    # ----------------------------------------
    # 第五步：标注长期高低点
    # ----------------------------------------
    # 遍历所有中期拐点，如果左右中期拐点都低于/高于当前中期拐点，则当前中期拐点为长期拐点
    upper_turning_points3 = []
    lower_turning_points3 = []
    for index, row in df.iterrows():
        if row['turningpoint'] != 0:
            if row['turningpoint'] == 2:
                upper_turning_points3.append((index, row))
            if row['turningpoint'] == -2:
                lower_turning_points3.append((index, row))

    for i in range(1, len(upper_turning_points3) - 1):
        prev_point = upper_turning_points3[i - 1]
        curr_point = upper_turning_points3[i]
        next_point = upper_turning_points3[i + 1]

        if (curr_point[1]['high'] >= prev_point[1]['high']
                and curr_point[1]['high'] >= next_point[1]['high']):
            df.iloc[curr_point[0], 10] = 3

    for i in range(1, len(lower_turning_points3) - 1):
        prev_point = lower_turning_points3[i - 1]
        curr_point = lower_turning_points3[i]
        next_point = lower_turning_points3[i + 1]

        if (curr_point[1]['low'] <= prev_point[1]['low']
                and curr_point[1]['low'] <= next_point[1]['low']):
            df.iloc[curr_point[0], 10] = -3

    df = df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'turningpoint', 'dominant']]
    df.set_index('datetime', inplace=True)
    df.to_csv('SA88_1d.csv')


def get_all_instruments(type='Future'):
    rqdatac.init(username, password)
    df = rqdatac.all_instruments(type=type)
    df.to_csv('all_instruments.csv')


if __name__ == '__main__':
    # get_all_instruments()

    # rqdata_download('I88', '20131018', '20230913')
    # short_term_trading_data(filepath='I88_20131018_20230913_1d.csv')
    # short_term_trading_data(filepath='I88_20131018_20230913_15m.csv')

    # rqdata_download('SA88', '20191201', '20230908')
    # mark_turning_point(filepath='SA88_20191201_20230908_1d.csv')
    mark_turning_point(filepath='SA88_20191201_20230908_1d.csv')
