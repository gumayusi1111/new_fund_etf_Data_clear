# 季节性分析指标

## 概述
季节性分析通过研究ETF在不同时间周期内的规律性表现，识别具有统计显著性的时间效应，为投资决策提供时间维度的参考。

## 主要分析类型

### 📅 月度效应分析
```python
def monthly_effect_analysis(price_data):
    """
    分析每月的平均收益率和波动特征
    """
    returns = price_data.pct_change()
    monthly_stats = {}
    
    for month in range(1, 13):
        month_returns = returns[returns.index.month == month]
        monthly_stats[month] = {
            'avg_return': month_returns.mean(),
            'std_return': month_returns.std(),
            'win_rate': (month_returns > 0).mean(),
            'max_return': month_returns.max(),
            'min_return': month_returns.min()
        }
    
    return monthly_stats
```

### 🗓️ 星期效应分析
```python
def weekday_effect_analysis(price_data):
    """
    分析一周内各交易日的表现特征
    """
    returns = price_data.pct_change()
    weekday_names = ['周一', '周二', '周三', '周四', '周五']
    weekday_stats = {}
    
    for i, day_name in enumerate(weekday_names):
        day_returns = returns[returns.index.weekday == i]
        weekday_stats[day_name] = {
            'avg_return': day_returns.mean(),
            'std_return': day_returns.std(),
            'win_rate': (day_returns > 0).mean(),
            'sample_size': len(day_returns)
        }
    
    return weekday_stats
```

### 🎆 节假日效应分析
```python
def holiday_effect_analysis(price_data, holidays_list):
    """
    分析节假日前后的价格行为
    """
    returns = price_data.pct_change()
    holiday_effects = {}
    
    for holiday in holidays_list:
        # 节前3天收益
        pre_holiday = returns[
            (returns.index >= holiday - pd.Timedelta(days=5)) & 
            (returns.index < holiday)
        ].mean()
        
        # 节后3天收益
        post_holiday = returns[
            (returns.index > holiday) & 
            (returns.index <= holiday + pd.Timedelta(days=5))
        ].mean()
        
        holiday_effects[holiday.strftime('%Y-%m-%d')] = {
            'pre_holiday_return': pre_holiday,
            'post_holiday_return': post_holiday
        }
    
    return holiday_effects
```

## 统计显著性检验

### t检验
```python
from scipy import stats

def seasonal_significance_test(returns_group1, returns_group2):
    """
    检验两个时间段收益率的差异是否显著
    """
    t_stat, p_value = stats.ttest_ind(returns_group1, returns_group2)
    
    return {
        't_statistic': t_stat,
        'p_value': p_value,
        'is_significant': p_value < 0.05,
        'effect_size': (returns_group1.mean() - returns_group2.mean()) / 
                      np.sqrt((returns_group1.var() + returns_group2.var()) / 2)
    }
```

## 投资策略应用

### 📈 季节性配置策略
```python
def seasonal_allocation_strategy(monthly_effects):
    """
    基于月度效应的动态配置策略
    """
    # 找出表现最好和最差的月份
    best_months = sorted(monthly_effects.items(), 
                        key=lambda x: x[1]['avg_return'], reverse=True)[:3]
    worst_months = sorted(monthly_effects.items(), 
                         key=lambda x: x[1]['avg_return'])[:3]
    
    strategy = {
        'overweight_months': [month for month, _ in best_months],
        'underweight_months': [month for month, _ in worst_months],
        'normal_months': [month for month in range(1,13) 
                         if month not in [m for m,_ in best_months + worst_months]]
    }
    
    return strategy
```

### 🎯 择时交易策略
```python
def seasonal_timing_strategy(current_date, seasonal_patterns):
    """
    基于季节性模式的择时建议
    """
    current_month = current_date.month
    current_weekday = current_date.weekday()
    
    # 月度建议
    monthly_score = seasonal_patterns['monthly'][current_month]['avg_return']
    
    # 星期建议
    weekly_score = seasonal_patterns['weekly'][current_weekday]['avg_return']
    
    # 综合评分
    total_score = monthly_score * 0.7 + weekly_score * 0.3
    
    if total_score > 0.005:  # 0.5%
        return "强烈看好", "建议加仓"
    elif total_score > 0.002:  # 0.2%
        return "适度看好", "可以买入"
    elif total_score > -0.002:
        return "中性", "维持现状"
    elif total_score > -0.005:
        return "适度看淡", "可以减仓"
    else:
        return "强烈看淡", "建议减仓"
```

## 高级分析方法

### 滚动季节性分析
```python
def rolling_seasonal_analysis(price_data, window_years=3):
    """
    滚动窗口的季节性分析，观察模式的稳定性
    """
    returns = price_data.pct_change()
    results = {}
    
    for year in range(window_years, len(price_data.index.year.unique())):
        end_year = price_data.index.year.max() - (len(price_data.index.year.unique()) - year - 1)
        start_year = end_year - window_years + 1
        
        period_data = returns[
            (returns.index.year >= start_year) & 
            (returns.index.year <= end_year)
        ]
        
        monthly_pattern = {}
        for month in range(1, 13):
            month_returns = period_data[period_data.index.month == month]
            monthly_pattern[month] = month_returns.mean()
        
        results[f"{start_year}-{end_year}"] = monthly_pattern
    
    return results
```

### 季节性强度评估
```python
def seasonal_strength_assessment(seasonal_patterns):
    """
    评估季节性效应的强度和稳定性
    """
    monthly_returns = [patterns['avg_return'] for patterns in seasonal_patterns['monthly'].values()]
    
    # 计算季节性强度指标
    seasonal_range = max(monthly_returns) - min(monthly_returns)
    seasonal_std = np.std(monthly_returns)
    consistency_score = 1 - (seasonal_std / np.mean(np.abs(monthly_returns)))
    
    return {
        'seasonal_range': seasonal_range,
        'seasonal_volatility': seasonal_std,
        'consistency_score': consistency_score,
        'strength_rating': 'Strong' if seasonal_range > 0.02 else 
                          'Moderate' if seasonal_range > 0.01 else 'Weak'
    }
```

## 实际应用案例

### 案例1: A股ETF的季节性特征
- **1月效应**: 通常表现较好（年初资金面宽松）
- **5月效应**: "五穷六绝七翻身"的传统
- **12月效应**: 年末效应，机构调仓

### 案例2: 商品ETF的季节性
- **农产品ETF**: 受季节性供需影响明显
- **能源ETF**: 冬夏用能高峰的影响
- **贵金属ETF**: 传统节日的需求周期

## 注意事项

### ⚠️ 分析局限性
- **样本偏差**: 历史数据可能不代表未来
- **制度变迁**: 市场制度变化影响季节性模式
- **外部冲击**: 突发事件可能打破季节性规律

### 🔍 最佳实践
- **多年验证**: 使用足够长的历史数据
- **显著性检验**: 确保发现的模式具有统计意义
- **动态调整**: 定期更新和验证季节性模式
- **风险控制**: 季节性策略也需要严格的风险管理

### 💡 实用建议
- **组合应用**: 将季节性分析与其他技术指标结合
- **分散投资**: 不要过度依赖单一的季节性模式
- **持续监控**: 密切关注季节性模式的变化
- **灵活应对**: 准备应对季节性失效的情况 