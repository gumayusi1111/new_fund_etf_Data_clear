# 复权分析 (Adjustment Analysis)

## 概述
复权分析是深入研究除权除息事件对ETF价格和收益率影响的专门分析方法，帮助投资者理解分红、送股、拆股等公司行为对投资收益的具体影响。

## 主要分析维度

### 1. 分红影响分析 (Dividend Impact Analysis)

#### 分红事件识别
- **数据来源**: 复权因子突变、公告信息
- **识别方法**: 复权价格跳跃检测
- **事件分类**: 
  - 现金分红
  - 股票分红
  - 特别分红

#### 分红收益率计算
- **公式**: 分红收益率 = 分红金额 / 除权前价格
- **年化分红收益率**: 考虑分红频率的年化计算
- **累积分红收益**: 长期持有的分红累积效应

#### 分红再投资效应
- **计算方法**: 
```python
def dividend_reinvestment_effect(price_series, div_dates, div_amounts):
    """计算分红再投资效应"""
    shares = 1.0  # 初始持股
    cash = 0.0    # 累积现金分红
    
    for date in div_dates:
        if date in price_series.index:
            div_per_share = div_amounts[date]
            cash += shares * div_per_share
            # 再投资购买新股
            new_shares = cash / price_series[date]
            shares += new_shares
            cash = 0.0
    
    return shares - 1.0  # 额外获得的股份比例
```

### 2. 送股影响分析 (Stock Split Impact Analysis)

#### 送股事件检测
- **特征**: 复权因子大幅上升
- **比率计算**: 送股比例 = (调整后股数 - 原股数) / 原股数
- **价格影响**: 理论上价格应相应下调

#### 送股后价格行为
- **短期效应**: 送股公告后的价格反应
- **长期效应**: 送股后的价格表现
- **流动性变化**: 送股对交易活跃度的影响

#### 送股套利分析
- **填权行情**: 价格回归除权前水平的概率
- **贴权风险**: 价格持续低于理论价格的风险
- **市场情绪**: 投资者对送股的反应

### 3. 拆股影响分析 (Stock Split Impact Analysis)

#### 拆股识别
- **定义**: 1股拆分为多股，总价值不变
- **检测**: 股数增加但市值不变
- **常见比例**: 1拆2、1拆3、1拆10等

#### 拆股动机分析
- **价格区间优化**: 将高价股拆分到合理交易区间
- **流动性改善**: 降低投资门槛，提高参与度
- **心理效应**: 低价股的心理吸引力

#### 拆股效果评估
```python
def split_effectiveness_analysis(before_data, after_data, split_ratio):
    """拆股效果分析"""
    # 价格变化分析
    price_change = (after_data['price'] * split_ratio) / before_data['price'] - 1
    
    # 流动性变化分析
    volume_change = after_data['volume'] / before_data['volume'] - 1
    turnover_change = after_data['turnover'] / before_data['turnover'] - 1
    
    # 波动率变化
    vol_change = after_data['volatility'] / before_data['volatility'] - 1
    
    return {
        'price_efficiency': abs(price_change),  # 越接近0越好
        'liquidity_improvement': volume_change,
        'activity_change': turnover_change,
        'volatility_change': vol_change
    }
```

### 4. 综合复权事件分析

#### 复权事件日历
- **事件时间轴**: 按时间顺序排列所有复权事件
- **事件密度**: 单位时间内复权事件频率
- **季节性分析**: 复权事件的季节性分布

#### 复权事件对价格的累积影响
```python
def cumulative_adjustment_impact(price_series, adjustment_events):
    """计算复权事件的累积影响"""
    cumulative_impact = 1.0
    impacts = []
    
    for event in adjustment_events:
        if event['type'] == 'dividend':
            impact = 1 - event['dividend_yield']
        elif event['type'] == 'split':
            impact = 1 / event['split_ratio']
        elif event['type'] == 'bonus':
            impact = 1 / (1 + event['bonus_ratio'])
        
        cumulative_impact *= impact
        impacts.append({
            'date': event['date'],
            'event_type': event['type'],
            'single_impact': impact,
            'cumulative_impact': cumulative_impact
        })
    
    return impacts
```

### 5. 投资收益归因分析

#### 收益来源分解
- **价格收益**: 基于除权价格的资本利得
- **分红收益**: 现金分红的直接收益
- **送股收益**: 送股带来的股份增值
- **复利效应**: 分红再投资的复合增长

#### 收益归因计算
```python
def return_attribution(start_date, end_date, price_data, div_data):
    """投资收益归因分析"""
    # 价格收益（基于除权价格）
    price_return = (price_data['adj_close'][end_date] / 
                   price_data['adj_close'][start_date]) - 1
    
    # 分红收益
    period_dividends = div_data.loc[start_date:end_date]
    dividend_yield = period_dividends['dividend'].sum() / price_data['close'][start_date]
    
    # 总收益（基于复权价格）
    total_return = (price_data['adj_close'][end_date] / 
                   price_data['adj_close'][start_date]) - 1
    
    # 再投资效应
    reinvestment_effect = total_return - price_return - dividend_yield
    
    return {
        'total_return': total_return,
        'price_return': price_return,
        'dividend_yield': dividend_yield,
        'reinvestment_effect': reinvestment_effect
    }
```

## 输出字段
- `Dividend_Impact`: 分红影响系数
- `Split_Impact`: 拆股影响系数
- `Dividend_Yield`: 分红收益率
- `Split_Ratio`: 拆股比例
- `Adjustment_Frequency`: 复权事件频率
- `Cumulative_Adj_Factor`: 累积复权因子
- `Price_Return`: 价格收益率
- `Dividend_Return`: 分红收益率
- `Total_Return`: 总收益率
- `Reinvestment_Effect`: 再投资效应

## 实战应用

### 1. 投资决策支持
```python
def investment_decision_support(etf_data):
    """基于复权分析的投资决策支持"""
    analysis = {}
    
    # 分红稳定性分析
    div_consistency = calculate_dividend_consistency(etf_data['dividends'])
    analysis['dividend_reliability'] = div_consistency
    
    # 历史分红收益贡献
    div_contribution = etf_data['dividend_return'] / etf_data['total_return']
    analysis['dividend_importance'] = div_contribution
    
    # 送股频率分析
    split_frequency = len(etf_data['split_events']) / len(etf_data) * 252
    analysis['split_activity'] = split_frequency
    
    return analysis
```

### 2. 风险评估
```python
def adjustment_risk_assessment(adjustment_history):
    """复权事件风险评估"""
    risks = {}
    
    # 分红削减风险
    recent_dividends = adjustment_history['dividends'][-4:]  # 最近4次
    if len(recent_dividends) > 1:
        div_trend = (recent_dividends[-1] - recent_dividends[0]) / recent_dividends[0]
        risks['dividend_cut_risk'] = 'High' if div_trend < -0.2 else 'Low'
    
    # 异常拆股风险
    unusual_splits = [s for s in adjustment_history['splits'] if s['ratio'] > 10]
    risks['unusual_split_risk'] = 'High' if unusual_splits else 'Low'
    
    return risks
```

### 3. 业绩比较基准调整
```python
def adjust_benchmark_for_comparison(etf_returns, benchmark_returns, adjustment_events):
    """调整基准以便公平比较"""
    adjusted_benchmark = benchmark_returns.copy()
    
    for event in adjustment_events:
        if event['type'] == 'dividend':
            # 基准不包含分红，需要调整
            event_date = event['date']
            div_impact = event['dividend_yield']
            adjusted_benchmark.loc[event_date:] *= (1 + div_impact)
    
    return adjusted_benchmark
```

## 应用场景

### 1. 长期投资分析
- **分红再投资策略**: 评估长期持有的复合收益
- **税收优化**: 分红税负vs资本利得税负比较
- **现金流规划**: 基于历史分红预测未来现金流

### 2. 短期交易策略
- **除权除息套利**: 利用除权前后的价格差异
- **事件驱动交易**: 基于复权事件的短期交易机会
- **填权贴权判断**: 预测除权后的价格走势

### 3. 组合管理
- **收益来源多样化**: 平衡价格收益和分红收益
- **风险分散**: 不同分红特征ETF的组合
- **再平衡时机**: 利用除权事件进行组合调整

## 分析报告模板
```python
def generate_adjustment_analysis_report(etf_code, analysis_period):
    """生成复权分析报告"""
    report = {
        'summary': {
            'total_adjustment_events': 0,
            'dividend_events': 0,
            'split_events': 0,
            'average_dividend_yield': 0,
            'total_return_breakdown': {}
        },
        'dividend_analysis': {
            'consistency_score': 0,
            'growth_trend': 0,
            'seasonality': {}
        },
        'split_analysis': {
            'frequency': 0,
            'average_ratio': 0,
            'effectiveness': {}
        },
        'investment_implications': {
            'income_suitability': '',
            'tax_efficiency': '',
            'long_term_outlook': ''
        }
    }
    return report
```

## 注意事项
- 复权分析需要准确的历史数据
- 不同数据源的复权方法可能存在差异
- 税收政策变化会影响实际收益
- 市场环境变化会影响复权事件的效果
- 应结合基本面分析综合判断 