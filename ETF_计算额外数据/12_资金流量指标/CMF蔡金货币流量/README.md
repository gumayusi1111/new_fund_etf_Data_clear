# CMF蔡金货币流量指标 (Chaikin Money Flow)

## 📊 指标概述

蔡金货币流量指标(CMF)是由马克·蔡金(Marc Chaikin)开发的成交量技术指标，用于衡量特定期间内资金流入或流出的强度。CMF通过结合价格位置和成交量来判断市场的买卖压力，是判断趋势强度和资金流向的重要工具。

## 🧮 计算公式

### 核心计算步骤

#### 1. 资金流量乘数 (Money Flow Multiplier)
```python
def money_flow_multiplier(high, low, close):
    """
    计算资金流量乘数
    MFM = [(Close - Low) - (High - Close)] / (High - Low)
    """
    mfm = ((close - low) - (high - close)) / (high - low)
    # 处理分母为0的情况
    mfm = mfm.fillna(0)
    return mfm
```

#### 2. 资金流量 (Money Flow Volume)
```python
def money_flow_volume(mfm, volume):
    """
    计算资金流量
    MFV = MFM × Volume
    """
    return mfm * volume
```

#### 3. CMF指标
```python
def chaikin_money_flow(high, low, close, volume, period=20):
    """
    计算蔡金货币流量指标
    CMF = Sum(MFV, period) / Sum(Volume, period)
    """
    mfm = money_flow_multiplier(high, low, close)
    mfv = money_flow_volume(mfm, volume)
    
    # 计算指定期间的CMF
    cmf = mfv.rolling(period).sum() / volume.rolling(period).sum()
    return cmf
```

### 完整实现示例
```python
import pandas as pd
import numpy as np

def calculate_cmf(df, period=20):
    """
    完整的CMF计算函数
    
    参数:
    df: 包含OHLCV数据的DataFrame
    period: 计算周期，默认20天
    
    返回:
    CMF序列
    """
    # 计算资金流量乘数
    mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    mfm = mfm.fillna(0)  # 处理除零错误
    
    # 计算资金流量
    mfv = mfm * df['volume']
    
    # 计算CMF
    cmf = mfv.rolling(period).sum() / df['volume'].rolling(period).sum()
    
    return cmf

# 使用示例
# df['CMF_20'] = calculate_cmf(df, period=20)
# df['CMF_10'] = calculate_cmf(df, period=10)
```

## 📈 信号解读

### 数值范围和含义
- **CMF > 0.1**: 强烈的资金流入信号，买压明显
- **0 < CMF < 0.1**: 轻微资金流入，买压温和
- **-0.1 < CMF < 0**: 轻微资金流出，卖压温和  
- **CMF < -0.1**: 强烈的资金流出信号，卖压明显

### 信号强度分级
```python
def cmf_signal_strength(cmf):
    """
    CMF信号强度分级
    """
    conditions = [
        cmf >= 0.2,
        (cmf >= 0.1) & (cmf < 0.2),
        (cmf > 0) & (cmf < 0.1),
        (cmf >= -0.1) & (cmf <= 0),
        (cmf >= -0.2) & (cmf < -0.1),
        cmf < -0.2
    ]
    choices = ['强烈买入', '买入', '弱买入', '弱卖出', '卖出', '强烈卖出']
    return pd.Series(np.select(conditions, choices, default='中性'), index=cmf.index)
```

## 🎯 交易策略

### 1. 基础趋势确认策略
```python
def cmf_trend_strategy(close, cmf, threshold=0.1):
    """
    CMF趋势确认策略
    """
    # 价格趋势
    price_trend = close > close.rolling(20).mean()
    
    # CMF确认
    cmf_bullish = cmf > threshold
    cmf_bearish = cmf < -threshold
    
    # 生成信号
    buy_signal = price_trend & cmf_bullish
    sell_signal = (~price_trend) & cmf_bearish
    
    return buy_signal, sell_signal
```

### 2. CMF背离策略
```python
def cmf_divergence_strategy(close, cmf, window=20):
    """
    CMF与价格背离策略
    """
    # 价格趋势
    price_high = close.rolling(window).max()
    price_low = close.rolling(window).min()
    
    # CMF趋势
    cmf_high = cmf.rolling(window).max()
    cmf_low = cmf.rolling(window).min()
    
    # 顶背离：价格新高，CMF下降
    bearish_divergence = (close == price_high) & (cmf < cmf.shift(window))
    
    # 底背离：价格新低，CMF上升
    bullish_divergence = (close == price_low) & (cmf > cmf.shift(window))
    
    return bullish_divergence, bearish_divergence
```

### 3. CMF区间突破策略
```python
def cmf_breakout_strategy(cmf, upper_threshold=0.15, lower_threshold=-0.15):
    """
    CMF区间突破策略
    """
    # 突破上轨
    breakout_buy = (cmf > upper_threshold) & (cmf.shift(1) <= upper_threshold)
    
    # 突破下轨
    breakout_sell = (cmf < lower_threshold) & (cmf.shift(1) >= lower_threshold)
    
    # 回归中性
    back_to_neutral = (abs(cmf) < 0.05) & (abs(cmf.shift(1)) >= 0.1)
    
    return breakout_buy, breakout_sell, back_to_neutral
```

## 📊 实际应用案例

### ETF资金流分析
```python
def etf_money_flow_analysis(df, etf_name):
    """
    ETF资金流量分析
    """
    # 计算不同周期的CMF
    cmf_10 = calculate_cmf(df, period=10)
    cmf_20 = calculate_cmf(df, period=20)
    cmf_50 = calculate_cmf(df, period=50)
    
    # 分析结果
    analysis = {
        'ETF名称': etf_name,
        '短期资金流向': ' 流入' if cmf_10.iloc[-1] > 0 else '流出',
        '中期资金流向': '流入' if cmf_20.iloc[-1] > 0 else '流出',
        '长期资金流向': '流入' if cmf_50.iloc[-1] > 0 else '流出',
        '短期CMF': round(cmf_10.iloc[-1], 3),
        '中期CMF': round(cmf_20.iloc[-1], 3),
        '长期CMF': round(cmf_50.iloc[-1], 3)
    }
    
    return analysis
```

### 资金流量强度排名
```python
def money_flow_ranking(etf_data_dict):
    """
    多个ETF的资金流量强度排名
    """
    rankings = []
    
    for etf_code, df in etf_data_dict.items():
        cmf = calculate_cmf(df)
        current_cmf = cmf.iloc[-1]
        
        rankings.append({
            'ETF代码': etf_code,
            'CMF值': current_cmf,
            '资金流向': '流入' if current_cmf > 0 else '流出',
            '强度等级': abs(current_cmf)
        })
    
    # 按强度排序
    rankings_df = pd.DataFrame(rankings)
    rankings_df = rankings_df.sort_values('强度等级', ascending=False)
    
    return rankings_df
```

## ⚠️ 使用注意事项

### 指标局限性
1. **滞后性**: CMF是基于历史数据的指标，存在一定滞后
2. **假信号**: 在震荡市场中可能产生较多假信号
3. **成交量依赖**: 成交量数据质量直接影响指标有效性

### 优化建议
```python
def cmf_optimization(df):
    """
    CMF指标优化建议
    """
    cmf = calculate_cmf(df)
    
    # 使用EMA平滑
    cmf_smoothed = cmf.ewm(span=5).mean()
    
    # 添加信号过滤
    volume_avg = df['volume'].rolling(20).mean()
    high_volume_filter = df['volume'] > volume_avg * 1.2
    
    # 只在高成交量时考虑CMF信号
    filtered_signals = cmf_smoothed * high_volume_filter
    
    return filtered_signals
```

### 参数调优
- **短期周期**: 5-10天，适合短线交易
- **中期周期**: 15-25天，适合中线投资
- **长期周期**: 30-50天，适合长线布局

## 🔄 与其他指标结合

### CMF + RSI组合
```python
def cmf_rsi_combo(close, high, low, volume):
    """
    CMF与RSI组合策略
    """
    cmf = calculate_cmf(pd.DataFrame({'high': high, 'low': low, 'close': close, 'volume': volume}))
    rsi = calculate_rsi(close)  # 假设已有RSI函数
    
    # 强势信号：CMF > 0 且 RSI < 70
    strong_buy = (cmf > 0.1) & (rsi < 70)
    
    # 弱势信号：CMF < 0 且 RSI > 30
    strong_sell = (cmf < -0.1) & (rsi > 30)
    
    return strong_buy, strong_sell
```

### CMF + MACD组合
```python
def cmf_macd_combo(close, high, low, volume):
    """
    CMF与MACD组合分析
    """
    cmf = calculate_cmf(pd.DataFrame({'high': high, 'low': low, 'close': close, 'volume': volume}))
    # MACD计算（假设已有）
    # macd_line, signal_line, histogram = calculate_macd(close)
    
    # 三重确认：价格、MACD、CMF同向
    # buy_signal = (macd_line > signal_line) & (cmf > 0.05)
    # sell_signal = (macd_line < signal_line) & (cmf < -0.05)
    
    return cmf  # 返回CMF用于后续分析
```

CMF蔡金货币流量指标是判断资金流向的重要工具，结合价格和成交量信息，能够有效识别市场的买卖压力，是ETF投资中不可或缺的技术分析工具！ 