# 复权价格对比 (Adjusted Price Comparison)

## 概述
复权价格对比是分析不同复权方式下价格差异的重要工具，帮助投资者理解除权除息对价格的影响，以及如何正确进行技术分析和投资决策。

## 复权方式介绍

### 1. 前复权 (Forward Adjusted)
- **定义**: 以当前价格为基准，调整历史价格
- **特点**: 当前价格保持不变，历史价格被调整
- **优势**: 便于观察真实的价格走势
- **应用**: 技术分析的首选方式

### 2. 后复权 (Backward Adjusted)
- **定义**: 以最早价格为基准，调整后续价格
- **特点**: 历史价格保持不变，后续价格被调整
- **优势**: 保持历史成本的真实性
- **应用**: 成本分析和长期投资

### 3. 除权价格 (Non-Adjusted)
- **定义**: 原始交易价格，未做任何调整
- **特点**: 真实的市场交易价格
- **局限**: 不连续，影响技术分析
- **应用**: 实际交易参考

## 主要对比指标

### 1. 价格差异分析

#### 绝对价差
- **公式**: 差值 = 前复权价格 - 后复权价格
- **用途**: 观察复权调整的绝对影响
- **特点**: 随时间累积变化

#### 相对价差
- **公式**: 比率 = 前复权价格 / 后复权价格
- **用途**: 观察复权调整的相对影响
- **特点**: 反映累积分红除权影响

#### 价差变化率
- **公式**: 变化率 = (当前价差 - 前期价差) / 前期价差
- **用途**: 识别除权除息时点
- **应用**: 事件研究分析

### 2. 复权因子分析

#### 累积复权因子
- **定义**: 从基准时点到当前的累积调整倍数
- **计算**: 连续复权事件的乘积
- **意义**: 反映总的权益变化

#### 复权因子变化
- **识别**: 复权因子发生跳跃的时点
- **原因**: 分红、送股、拆股等事件
- **应用**: 公司行为事件识别

### 3. 收益率对比

#### 复权收益率 vs 原始收益率
- **复权收益率**: 基于复权价格计算的收益率
- **原始收益率**: 基于原始价格计算的收益率
- **差异**: 复权收益率包含分红再投资效应

#### 累积收益率差异
- **计算**: 长期累积收益率的差异
- **意义**: 分红再投资的长期价值
- **应用**: 投资业绩评估

## 计算方法

### 价格差异计算
```python
def price_difference_analysis(forward_adj, backward_adj, non_adj):
    """计算不同复权方式的价格差异"""
    results = {}
    
    # 绝对价差
    results['abs_diff_fb'] = forward_adj - backward_adj
    results['abs_diff_fn'] = forward_adj - non_adj
    results['abs_diff_bn'] = backward_adj - non_adj
    
    # 相对价差
    results['rel_diff_fb'] = forward_adj / backward_adj
    results['rel_diff_fn'] = forward_adj / non_adj
    results['rel_diff_bn'] = backward_adj / non_adj
    
    return results
```

### 复权因子计算
```python
def calculate_adjustment_factors(forward_adj, non_adj):
    """计算复权因子"""
    adj_factor = forward_adj / non_adj
    
    # 识别复权事件（因子突变）
    factor_change = adj_factor.pct_change()
    significant_changes = factor_change[abs(factor_change) > 0.01]
    
    return adj_factor, significant_changes
```

### 收益率对比
```python
def return_comparison(forward_adj, backward_adj, non_adj):
    """对比不同复权方式的收益率"""
    returns = {}
    
    # 计算各种收益率
    returns['forward_ret'] = forward_adj.pct_change()
    returns['backward_ret'] = backward_adj.pct_change()
    returns['non_adj_ret'] = non_adj.pct_change()
    
    # 累积收益率
    returns['forward_cum'] = (1 + returns['forward_ret']).cumprod() - 1
    returns['backward_cum'] = (1 + returns['backward_ret']).cumprod() - 1
    returns['non_adj_cum'] = (1 + returns['non_adj_ret']).cumprod() - 1
    
    return returns
```

## 输出字段
- `Forward_Backward_Diff`: 前复权与后复权价差
- `Forward_Raw_Diff`: 前复权与原始价格价差
- `Forward_Backward_Ratio`: 前复权与后复权价格比率
- `Adjustment_Factor`: 复权因子
- `Factor_Change`: 复权因子变化率
- `Dividend_Impact`: 分红影响估算
- `Cumulative_Adjustment`: 累积调整幅度
- `Return_Difference`: 收益率差异

## 实战应用

### 1. 技术分析选择
```python
def choose_price_type(analysis_purpose):
    """根据分析目的选择价格类型"""
    if analysis_purpose == "技术分析":
        return "前复权价格"
    elif analysis_purpose == "成本分析":
        return "后复权价格"
    elif analysis_purpose == "实时交易":
        return "原始价格"
    else:
        return "前复权价格"  # 默认
```

### 2. 除权事件检测
```python
def detect_dividend_events(adj_factor, threshold=0.02):
    """检测分红除权事件"""
    factor_changes = adj_factor.pct_change()
    events = factor_changes[abs(factor_changes) > threshold]
    
    dividend_events = []
    for date, change in events.items():
        event_type = "分红" if change < 0 else "送股"
        dividend_events.append({
            'date': date,
            'type': event_type,
            'magnitude': abs(change),
            'adjustment_factor': adj_factor[date]
        })
    
    return dividend_events
```

### 3. 投资收益评估
```python
def investment_return_analysis(start_date, end_date, prices):
    """投资收益分析"""
    start_prices = {k: v[start_date] for k, v in prices.items()}
    end_prices = {k: v[end_date] for k, v in prices.items()}
    
    returns = {}
    for price_type in ['forward', 'backward', 'non_adj']:
        returns[f'{price_type}_return'] = (
            end_prices[price_type] - start_prices[price_type]
        ) / start_prices[price_type]
    
    # 分红收益估算
    dividend_return = returns['forward_return'] - returns['non_adj_return']
    
    return {
        'price_returns': returns,
        'dividend_return': dividend_return,
        'total_return': returns['forward_return']
    }
```

## 应用场景

### 1. 技术分析
- **趋势分析**: 使用前复权价格确保连续性
- **支撑阻力**: 基于前复权价格识别关键位置
- **指标计算**: 所有技术指标应基于复权价格

### 2. 基本面分析
- **估值分析**: 注意除权对PE、PB等指标的影响
- **股息率计算**: 需要调整价格基准
- **业绩对比**: 考虑复权影响的公平比较

### 3. 量化策略
- **回测准确性**: 使用正确的复权价格
- **信号验证**: 确保信号不受除权扭曲
- **业绩归因**: 区分价格收益和分红收益

### 4. 风险管理
- **波动率计算**: 基于复权价格的准确测量
- **相关性分析**: 避免除权造成的虚假相关
- **回撤分析**: 真实反映投资组合表现

## 注意事项

### 1. 数据一致性
- 确保所有分析使用同一复权方式
- 注意不同数据源的复权标准差异
- 定期检查复权数据的准确性

### 2. 历史对比
- 长期比较需要考虑复权累积效应
- 不同时期的价格需要调整到同一基准
- 注意复权方式变更的影响

### 3. 实际交易
- 下单时使用实际交易价格
- 复权价格仅用于分析参考
- 注意复权价格与实际价格的转换

## 组合分析建议
- **与基本面结合**: 了解除权事件的基本面原因
- **与技术面结合**: 观察除权对技术形态的影响
- **与资金面结合**: 分析除权前后的资金流向
- **定期更新**: 随着新的除权事件更新分析框架 