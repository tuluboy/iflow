#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用TqSdk获取中国期货市场所有期货主力合约的日、周、月K线数据，
计算MACD指标值，并根据MACD指标值进行分类打标签，
最后将结果保存到CSV文件中。

注意：使用本脚本需要快期账户，如果没有账户，请访问 https://account.shinnytech.com/ 注册。
"""

from math import nan
import pandas as pd
from tqsdk import TqApi, TqSim
from tqsdk.ta import MACD
import datetime
import numpy as np
import os
import time

# 读取.env文件中的环境变量
def load_env():
    env_vars = {}
    env_file = '.env'
    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    # 移除值中的引号
                    value = value.strip().strip('"').strip("'")
                    env_vars[key.strip()] = value.strip()
    return env_vars

# 加载环境变量
env_vars = load_env()

# 全局变量
KLINE_COUNT = 150  # 获取最近100条K线数据
WEEKLY_COUNT_FOR_MONTHLY = 120  # 获取120条周K线用于合成月线

# 计算MACD指标
def calculate_macd(df, fast_period=12, slow_period=26, signal_period=9):
    """
    计算MACD指标
    :param df: 包含close列的DataFrame
    :param fast_period: 快速EMA周期
    :param slow_period: 慢速EMA周期
    :param signal_period: 信号线EMA周期
    :return: diff, dea, macd Series
    """
    macd_result = MACD(df, fast_period, slow_period, signal_period)
    return macd_result['diff'], macd_result['dea'], macd_result['bar']

# 根据MACD指标值进行分类打标签
def get_guaxiang(diff, dea):
    """
    根据MACD指标值进行分类打标签
    :param diff: DIFF值
    :param dea: DEA值
    :return: 卦象标签
    """
    if dea < 0 and diff > dea:
        return '1'
    elif dea > 0 and diff > dea:
        return '2'
    elif dea > 0 and diff < dea:
        return '3'
    elif dea < 0 and diff < dea:
        return '4'
    else:
        return '0'

# 计算涨幅并进行分类标记
def get_increase_classification(today_close, yesterday_close):
    """
    计算涨幅并进行分类标记
    :param today_close: 当天收盘价
    :param yesterday_close: 昨天收盘价
    :return: 涨幅分类标记
    """
    if yesterday_close == 0:
        return 0
    
    a = (today_close - yesterday_close) / yesterday_close
    
    if a > 0.01:
        return 2
    elif a > 0:
        return 1
    elif a >= -0.01:
        return 3
    else:
        return 4

# 从周线数据合成月线数据
def synthesize_monthly_from_weekly(weekly_df):
    """
    从周线数据合成月线数据
    :param weekly_df: 周线数据DataFrame
    :return: 月线数据DataFrame
    """
    df = weekly_df.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['year_month'] = df['datetime'].dt.to_period('M')
    
    monthly_data = []
    for year_month, group in df.groupby('year_month'):
        last_row = group.iloc[-1]
        monthly_data.append({
            'datetime': last_row['datetime'],
            'close': last_row['close'],
            'year_month': year_month
        })
    
    monthly_df = pd.DataFrame(monthly_data)
    monthly_df = monthly_df.sort_values('datetime').reset_index(drop=True)
    
    monthly_df['diff'], monthly_df['dea'], monthly_df['macd'] = calculate_macd(monthly_df)
    monthly_df['guaxiang'] = monthly_df.apply(lambda row: get_guaxiang(row['diff'], row['dea']), axis=1)
    
    monthly_df = monthly_df.drop(columns=['year_month'])
    
    return monthly_df

# 获取合约的K线数据
def get_kline_data(api, symbol, duration, count=None):
    """
    获取合约的K线数据
    :param api: TqApi实例
    :param symbol: 合约代码
    :param duration: K线周期，如'1d', '1w'
    :param count: 获取的K线数量，默认使用KLINE_COUNT
    :return: K线数据DataFrame
    """
    if count is None:
        count = KLINE_COUNT
    
    duration_map = {
        '1h': 3600,
        '1d': 86400,
        '1w': 604800
    }
    
    if duration not in duration_map:
        raise ValueError(f"不支持的K线周期: {duration}")
    
    try:
        kline = api.get_kline_serial(symbol, duration_map[duration], count)
        
        if len(kline['close']) == 0:
            raise Exception(f"获取{symbol}的{duration}数据为空")
        
        df = pd.DataFrame({
            'datetime': kline['datetime'],
            'close': kline['close']
        })
        
        df['diff'], df['dea'], df['macd'] = calculate_macd(df)
        
        df['guaxiang'] = df.apply(lambda row: get_guaxiang(row['diff'], row['dea']), axis=1)
        
        return df
    except Exception as e:
        print(f"获取{symbol}的{duration}数据时出错: {e}")
        raise

def get_tick_data(api, symbol):
    """
    获取合约的最新1条Tick数据
    :param api: TqApi实例
    :param symbol: 合约代码
    :return: (datetime, close) 或 None
    """
    try:
        tick = api.get_quote(symbol)
        #print(tick)
        if pd.isna(tick.close):
            print(f"  {symbol} Tick.close数据为空")
            tick.close = tick.pre_close if pd.isna(tick.last_price) else tick.last_price
        return pd.to_datetime(tick.datetime), tick.close
    except Exception as e:
        print(f"  获取{symbol}的Tick数据时出错: {e}")
        return None

def get_hourly_kline(api, symbol):
    """
    查询合约的最新1条1小时K线数据
    :param api: TqApi实例
    :param symbol: 合约代码
    :return: (datetime, close) 或 None
    """
    try:
        return get_tick_data(api, symbol)
        kline = api.get_kline_serial(symbol, 60, 1)
        
        if len(kline['close']) == 0:
            print(f"  {symbol} 1小时K线数据为空")
            return None
        
        dt = pd.to_datetime(kline['datetime'].iloc[0])
        close = kline['close'].iloc[0]
        return dt, close
    except Exception as e:
        print(f"  获取{symbol}的1小时K线数据时出错: {e}")
        return None

def update_daily_with_hourly(daily_df, hourly_dt, hourly_close):
    """
    用小时K线更新日线数据
    :param daily_df: 日线DataFrame
    :param hourly_dt: 小时K线日期时间
    :param hourly_close: 小时K线收盘价
    :return: 更新后的日线DataFrame
    """
    hourly_date = hourly_dt.date()
    last_daily_date = pd.to_datetime(daily_df['datetime'].iloc[-1]).date()
    
    if hourly_date > last_daily_date:
        # 新增日期，添加新行
        datetime_dtype = daily_df['datetime'].dtype
        if datetime_dtype == 'float64':
            new_datetime = hourly_dt.timestamp() * 1e9
        else:
            new_datetime = hourly_dt
        
        new_row = pd.DataFrame({
            'datetime': [new_datetime],
            'close': [hourly_close]
        })
        daily_df = pd.concat([daily_df, new_row], ignore_index=True)
        daily_df['diff'], daily_df['dea'], daily_df['macd'] = calculate_macd(daily_df)
        daily_df['guaxiang'] = daily_df.apply(lambda row: get_guaxiang(row['diff'], row['dea']), axis=1)
        print(f"  日线已更新: 新增日期 {hourly_date}")
    elif hourly_date == last_daily_date:
        # 同一天，更新收盘价
        daily_df.loc[daily_df.index[-1], 'close'] = hourly_close
        daily_df['diff'], daily_df['dea'], daily_df['macd'] = calculate_macd(daily_df)
        daily_df['guaxiang'] = daily_df.apply(lambda row: get_guaxiang(row['diff'], row['dea']), axis=1)
        print(f"  日线已更新: 更新当天收盘价 {hourly_date}")
    
    return daily_df

def update_weekly_with_hourly(weekly_df, hourly_dt, hourly_close):
    """
    用小时K线更新周线数据
    :param weekly_df: 周线DataFrame
    :param hourly_dt: 小时K线日期时间
    :param hourly_close: 小时K线收盘价
    :return: 更新后的周线DataFrame
    """
    is_monday = hourly_dt.weekday() == 0
    
    datetime_dtype = weekly_df['datetime'].dtype
    if datetime_dtype == 'float64':
        new_datetime = hourly_dt.timestamp() * 1e9
    else:
        new_datetime = hourly_dt
    
    if is_monday:
        new_row = pd.DataFrame({
            'datetime': [new_datetime],
            'close': [hourly_close]
        })
        weekly_df = pd.concat([weekly_df, new_row], ignore_index=True)
        print(f"  周线已更新: 新增周一 {hourly_dt.date()}")
    else:
        weekly_df.loc[weekly_df.index[-1], 'close'] = hourly_close
        print(f"  周线已更新: 更新收盘价为 {hourly_close}")
    
    weekly_df['diff'], weekly_df['dea'], weekly_df['macd'] = calculate_macd(weekly_df)
    weekly_df['guaxiang'] = weekly_df.apply(lambda row: get_guaxiang(row['diff'], row['dea']), axis=1)
    
    return weekly_df

# 主函数
def main():
    # 从环境变量中读取账户信息
    username = env_vars.get('TQSDK_USERNAME')
    password = env_vars.get('TQSDK_PASSWORD')
    
    # 调试信息
    print(f"读取到的账户信息: 用户名='{username}', 密码=******")
    
    if not username or not password:
        print("未找到快期账户信息，请在.env文件中设置TQSDK_USERNAME和TQSDK_PASSWORD")
        return
    
    try:
        # 初始化TqApi
        print(f"正在使用账户 {username} 连接TqSdk...")
        # 使用TqSim作为交易接口
        from tqsdk import TqSim, TqAuth
        sim = TqSim()
        auth = TqAuth(username, password)
        api = TqApi(sim, auth=auth)
        
        # 使用KQ.m@格式生成主连合约
        print("生成主连合约列表...")
        
        # 定义常见的交易所和合约品种
        contracts = {
            'CFFEX': ['IF', 'IC', 'IH', 'IM', 'TF', 'T', 'TS', 'TL'],
            'SHFE': ['cu', 'al', 'zn', 'pb', 'ni', 'sn', 'au', 'ag', 'rb', 'hc', 'sp', 'bu', 'ru', 'fu', 'ss'],
            'INE': ['sc', 'nr', 'lu', 'bc'],
            'DCE': ['a', 'b', 'm', 'y', 'p', 'c', 'cs', 'l', 'v', 'pp', 'j', 'jm', 'i', 'eg', 'eb', 'pg', 'jd', 'lh', 'rr', 'fb', 'bb'],
            'CZCE': ['TA', 'MA', 'ZC', 'SF', 'SM', 'RS', 'WH', 'RI', 'AP', 'OI', 'RM', 'CF', 'CY', 'SR', 'CJ', 'UR', 'PF', 'SA', 'FG', 'JR', 'LR', 'PM', 'PX'],
            'GFEX': ['si', 'lc', 'ps']
        }
        
        # 生成主连合约列表
        main_contracts = []
        for exchange, symbols in contracts.items():
            for symbol in symbols:
                main_contracts.append(f'KQ.m@{exchange}.{symbol}')
        
        print(f"共生成 {len(main_contracts)} 个主连合约")
        
        # 存储所有合约的结果
        all_results = []
        
        # 遍历每个主力合约
        for symbol in main_contracts:
            print(f"处理合约: {symbol}")
            
            try:
                hourly_result = get_hourly_kline(api, symbol)
                
                daily_df = get_kline_data(api, symbol, '1d')
                
                weekly_df_for_monthly = get_kline_data(api, symbol, '1w', count=WEEKLY_COUNT_FOR_MONTHLY)
                
                weekly_df = weekly_df_for_monthly.tail(KLINE_COUNT).reset_index(drop=True)
                
                if hourly_result:
                    hourly_dt, hourly_close = hourly_result
                    print(f"  小时K线: {hourly_dt} 收盘价: {hourly_close}")
                    
                    daily_df = update_daily_with_hourly(daily_df, hourly_dt, hourly_close)
                    
                    weekly_df_for_monthly = update_weekly_with_hourly(weekly_df_for_monthly, hourly_dt, hourly_close)
                    weekly_df = weekly_df_for_monthly.tail(KLINE_COUNT).reset_index(drop=True)
                
                monthly_df = synthesize_monthly_from_weekly(weekly_df_for_monthly)
                
                if not daily_df.empty and not weekly_df.empty and not monthly_df.empty:
                    # 获取最近的日期
                    latest_date = daily_df['datetime'].iloc[-1]
                    latest_date_str = pd.to_datetime(latest_date).strftime('%Y%m%d')
                    
                    # 获取最近的K线数据
                    latest_daily = daily_df.iloc[-1]
                    latest_weekly = weekly_df.iloc[-1]
                    latest_monthly = monthly_df.iloc[-1]
                    
                    # 计算涨幅分类标记
                    increase_classification = 0
                    if len(daily_df) >= 2:
                        yesterday_close = daily_df.iloc[-2]['close']
                        today_close = latest_daily['close']
                        increase_classification = get_increase_classification(today_close, yesterday_close)
                    
                    # 构造结果字典
                    result = {
                        '日期': pd.to_datetime(latest_date).strftime('%Y-%m-%d'),
                        '合约': symbol,
                        '日收盘价': latest_daily['close'],
                        '日diff': latest_daily['diff'],
                        '日dea': latest_daily['dea'],
                        '日macd': latest_daily['macd'],
                        '日卦象': latest_daily['guaxiang'],
                        '周收盘价': latest_weekly['close'],
                        '周diff': latest_weekly['diff'],
                        '周dea': latest_weekly['dea'],
                        '周macd': latest_weekly['macd'],
                        '周卦象': latest_weekly['guaxiang'],
                        '月收盘价': latest_monthly['close'],
                        '月diff': latest_monthly['diff'],
                        '月dea': latest_monthly['dea'],
                        '月macd': latest_monthly['macd'],
                        '月卦象': latest_monthly['guaxiang'],
                        '涨幅分类': increase_classification
                    }
                    
                    all_results.append(result)
                    
            except Exception as e:
                print(f"处理合约 {symbol} 时出错: {e}")
                continue
        
        # 如果有结果，保存到CSV文件
        if all_results:
            # 创建结果DataFrame
            result_df = pd.DataFrame(all_results)
            
            # 获取最近的日期作为文件名的一部分
            latest_date_str = result_df['日期'].iloc[-1].replace('-', '')
            csv_filename = f"guaxiang_{latest_date_str}.csv"
            
            # 保存到CSV文件（使用绝对路径）
            csv_path = os.path.join(os.getcwd(), csv_filename)
            result_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"结果已保存到 {csv_path}")
            print(result_df)
        else:
            print("没有获取到数据，无法生成CSV文件")
            
    except Exception as e:
        print(f"初始化API时出错: {e}")
        return
    finally:
        # 关闭API（如果已初始化）
        try:
            api.close()
        except:
            pass

if __name__ == "__main__":
    main()
