# MFI (资金流量指标) 详解

## 概述
MFI (Money Flow Index) 是一个结合价格和成交量的动量震荡指标，被称为"成交量加权的RSI"。它通过分析价格变化与成交量的关系，判断资金的流入流出情况。

## 计算公式

### 步骤1: 计算典型价格
```python
典型价格 = (最高价 + 最低价 + 收盘价) / 3
```

### 步骤2: 计算原始资金流量
```python
原始资金流量 = 典型价格 × 成交量
```

### 步骤3: 识别正负资金流量
```python
if 今日典型价格 > 昨日典型价格:
    正资金流量 += 今日原始资金流量
else:
    负资金流量 += 今日原始资金流量
```

### 步骤4: 计算资金流量比率和MFI
```python
资金流量比率 = N周期正资金流量之和 / N周期负资金流量之和
MFI = 100 - (100 / (1 + 资金流量比率))
```

## Python实现

### 基础计算函数
```python
import pandas as pd
import numpy as np

def calculate_mfi(high, low, close, volume, period=14):
    """
    计算MFI资金流量指标
    
    Parameters:
    high, low, close: 价格序列
    volume: 成交量序列
    period: 计算周期，默认14
    """
    # 计算典型价格
    typical_price = (high + low + close) / 3
    
    # 计算原始资金流量
    raw_money_flow = typical_price * volume
    
    # 识别价格变化方向
    price_change = typical_price.diff()
    
    # 计算正负资金流量
    positive_mf = pd.Series(index=raw_money_flow.index, dtype=float)
    negative_mf = pd.Series(index=raw_money_flow.index, dtype=float)
    
    positive_mf[price_change > 0] = raw_money_flow[price_change > 0]
    positive_mf[price_change <= 0] = 0
    
    negative_mf[price_change < 0] = raw_money_flow[price_change < 0]
    negative_mf[price_change >= 0] = 0
    
    # 计算周期内的正负资金流量之和
    positive_mf_sum = positive_mf.rolling(window=period).sum()
    negative_mf_sum = negative_mf.rolling(window=period).sum()
    
    # 避免除零错误
    negative_mf_sum = negative_mf_sum.replace(0, np.nan)
    
    # 计算资金流量比率
    money_flow_ratio = positive_mf_sum / negative_mf_sum
    
    # 计算MFI
    mfi = 100 - (100 / (1 + money_flow_ratio))
    
    return mfi

# 使用示例
def mfi_analysis(data, period=14):
    """
    完整的MFI分析
    """
    mfi = calculate_mfi(data['high'], data['low'], data['close'], 
                       data['volume'], period)
    
    return {
        'mfi': mfi,
        'mfi_mean': mfi.mean(),
        'mfi_std': mfi.std(),
        'overbought_count': (mfi > 80).sum(),
        'oversold_count': (mfi < 20).sum()
    }
```

## 信号解读

### 超买超卖信号
```python
def mfi_signals(mfi):
    """
    生成MFI交易信号
    """
    signals = pd.Series(index=mfi.index, dtype=str)
    
    # 超买超卖信号
    signals[mfi > 80] = '超买'
    signals[mfi < 20] = '超卖'
    signals[(mfi >= 20) & (mfi <= 80)] = '正常'
    
    # 极端信号
    signals[mfi > 90] = '极度超买'
    signals[mfi < 10] = '极度超卖'
    
    return signals

def mfi_trading_signals(mfi):
    """
    生成具体的交易信号
    """
    buy_signals = []
    sell_signals = []
    
    for i in range(1, len(mfi)):
        # 从超卖区域回升 - 买入信号
        if mfi.iloc[i-1] < 20 and mfi.iloc[i] >= 20:
            buy_signals.append(mfi.index[i])
        
        # 从超买区域回落 - 卖出信号
        if mfi.iloc[i-1] > 80 and mfi.iloc[i] <= 80:
            sell_signals.append(mfi.index[i])
    
    return buy_signals, sell_signals
```

### 背离分析
```python
def mfi_divergence_analysis(price, mfi, window=20):
    """
    检测MFI与价格的背离
    """
    price_peaks = price.rolling(window).max() == price
    price_troughs = price.rolling(window).min() == price
    
    mfi_peaks = mfi.rolling(window).max() == mfi
    mfi_troughs = mfi.rolling(window).min() == mfi
    
    # 顶背离：价格创新高，MFI不创新高
    bearish_divergence = price_peaks & ~mfi_peaks
    
    # 底背离：价格创新低，MFI不创新低
    bullish_divergence = price_troughs & ~mfi_troughs
    
    return {
        'bearish_divergence': bearish_divergence,
        'bullish_divergence': bullish_divergence
    }
```

## 实战策略

### 策略1: 基础超买超卖策略
```python
class MFIStrategy:
    def __init__(self, overbought=80, oversold=20):
        self.overbought = overbought
        self.oversold = oversold
        self.position = 0  # 0: 空仓, 1: 持仓
    
    def generate_signal(self, mfi_current, mfi_previous):
        signal = None
        
        if self.position == 0:  # 空仓状态
            if mfi_previous < self.oversold and mfi_current >= self.oversold:
                signal = 'BUY'
                self.position = 1
        
        elif self.position == 1:  # 持仓状态
            if mfi_previous > self.overbought and mfi_current <= self.overbought:
                signal = 'SELL'
                self.position = 0
        
        return signal
```

### 策略2: MFI与趋势结合
```python
def mfi_trend_strategy(price, mfi, ma_period=20):
    """
    MFI与移动平均线结合的策略
    """
    ma = price.rolling(ma_period).mean()
    trend = np.where(price > ma, 'UPTREND', 'DOWNTREND')
    
    signals = []
    
    for i in range(1, len(mfi)):
        if trend[i] == 'UPTREND':
            # 上升趋势中，关注超卖回升
            if mfi.iloc[i-1] < 30 and mfi.iloc[i] >= 30:
                signals.append(('BUY', mfi.index[i]))
        
        elif trend[i] == 'DOWNTREND':
            # 下降趋势中，关注超买回落
            if mfi.iloc[i-1] > 70 and mfi.iloc[i] <= 70:
                signals.append(('SELL', mfi.index[i]))
    
    return signals
```

### 策略3: 多时间框架MFI
```python
def multi_timeframe_mfi(daily_data, weekly_data):
    """
    多时间框架MFI分析
    """
    daily_mfi = calculate_mfi(daily_data['high'], daily_data['low'], 
                             daily_data['close'], daily_data['volume'])
    
    weekly_mfi = calculate_mfi(weekly_data['high'], weekly_data['low'], 
                              weekly_data['close'], weekly_data['volume'])
    
    # 对齐时间序列
    weekly_mfi_daily = weekly_mfi.reindex(daily_data.index, method='ffill')
    
    # 生成信号
    signals = []
    
    for i in range(len(daily_data)):
        date = daily_data.index[i]
        daily_mfi_val = daily_mfi.iloc[i]
        weekly_mfi_val = weekly_mfi_daily.iloc[i]
        
        # 多时间框架共振
        if daily_mfi_val < 20 and weekly_mfi_val < 30:
            signals.append(('STRONG_BUY', date))
        elif daily_mfi_val > 80 and weekly_mfi_val > 70:
            signals.append(('STRONG_SELL', date))
    
    return signals
```

## 优化技巧

### 参数优化
```python
def optimize_mfi_parameters(data, periods_range=(5, 30), 
                           overbought_range=(70, 90), 
                           oversold_range=(10, 30)):
    """
    优化MFI参数
    """
    best_performance = -np.inf
    best_params = None
    
    for period in range(periods_range[0], periods_range[1]+1):
        for overbought in range(overbought_range[0], overbought_range[1]+1, 5):
            for oversold in range(oversold_range[0], oversold_range[1]+1, 5):
                
                if oversold >= overbought:
                    continue
                
                # 计算MFI
                mfi = calculate_mfi(data['high'], data['low'], 
                                   data['close'], data['volume'], period)
                
                # 回测策略
                performance = backtest_mfi_strategy(data, mfi, 
                                                  overbought, oversold)
                
                if performance > best_performance:
                    best_performance = performance
                    best_params = (period, overbought, oversold)
    
    return best_params, best_performance
```

### 动态阈值
```python
def dynamic_mfi_thresholds(mfi, lookback=252):
    """
    基于历史分位数的动态阈值
    """
    rolling_percentile_80 = mfi.rolling(lookback).quantile(0.8)
    rolling_percentile_20 = mfi.rolling(lookback).quantile(0.2)
    
    return rolling_percentile_80, rolling_percentile_20
```

## 注意事项

### ⚠️ 使用限制
- **横盘市场**: 在横盘整理市场中效果最好
- **趋势市场**: 强趋势中可能长期超买/超卖
- **成交量要求**: 需要有意义的成交量数据
- **市场流动性**: 低流动性市场指标可能失真

### 🔍 最佳实践
- **结合其他指标**: 与RSI、MACD等指标确认
- **考虑市场环境**: 牛熊市环境下调整策略
- **风险管理**: 设置止损和资金管理规则
- **定期回测**: 验证策略的有效性

### 💡 高级应用
- **资金流量分解**: 分析大单和散户资金流向
- **板块轮动**: 用于识别行业资金流动
- **市场情绪**: 作为市场情绪指标的组成部分
- **算法交易**: 结合高频数据进行量化交易

## 实际案例

### 案例1: ETF择时
某科技ETF使用MFI(14)进行择时交易：
- 当MFI < 20时建仓，MFI > 80时减仓
- 结合20日均线确认趋势方向
- 年化收益提升3-5%，最大回撤控制在15%以内

### 案例2: 资金流向分析
通过MFI分析发现：
- 主力资金通常在MFI < 30时加仓
- 散户资金在MFI > 70时追高
- 可用于判断市场参与者结构 