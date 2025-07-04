# VaR (风险价值) 指标

## 概述
VaR (Value at Risk) 是金融风险管理中最重要的指标之一，表示在特定置信水平下，投资组合在给定时间内可能遭受的最大损失。

## 计算方法

### 1. 历史模拟法
```python
# 基于历史收益率分布
daily_returns = price_data.pct_change().dropna()
confidence_level = 0.05  # 95%置信度
VaR_95 = daily_returns.quantile(confidence_level)
```

### 2. 参数法 (正态分布假设)
```python
import numpy as np
mean_return = daily_returns.mean()
std_return = daily_returns.std()
z_score = norm.ppf(confidence_level)  # 95%置信度对应的Z值
VaR_95 = mean_return + z_score * std_return
```

### 3. 蒙特卡洛模拟法
```python
# 生成随机收益率路径
num_simulations = 10000
simulated_returns = np.random.normal(mean_return, std_return, num_simulations)
VaR_95 = np.percentile(simulated_returns, confidence_level * 100)
```

## 关键参数

### 置信水平
- **90%**: VaR_90 (10%分位数)
- **95%**: VaR_95 (5%分位数) - **最常用**
- **99%**: VaR_99 (1%分位数) - 极端风险

### 时间窗口
- **1天**: 日VaR，短期风险控制
- **10天**: 两周VaR，中短期风险
- **22天**: 月VaR，月度风险管理

### 滚动窗口
- **30天**: 短期市场状况
- **63天**: 季度数据
- **252天**: 年度数据

## 应用策略

### 🎯 风险控制
```
单日止损阈值 = 投资本金 × |日VaR_95|
如果当日损失 > 止损阈值，则强制平仓
```

### 💰 仓位管理
```
最大仓位 = 风险预算 / |VaR_95|
例如：风险预算2%，VaR_95=-1.5%
最大仓位 = 2% / 1.5% = 133%（可加杠杆）
```

### 📊 组合风险
```
组合VaR ≠ 单个资产VaR之和
需要考虑相关性：
组合VaR = √(Σwi²×VaRi² + 2ΣΣwi×wj×ρij×VaRi×VaRj)
```

## 实际应用案例

### ETF风险分级
- **低风险ETF**: 日VaR_95 < 1%
- **中风险ETF**: 1% ≤ 日VaR_95 < 2.5%  
- **高风险ETF**: 日VaR_95 ≥ 2.5%

### 动态风险预算
```python
# 根据VaR调整仓位
def adjust_position(current_var, target_var, current_position):
    adjustment_ratio = target_var / abs(current_var)
    new_position = current_position * adjustment_ratio
    return min(new_position, 1.0)  # 最大100%仓位
```

## 优缺点分析

### ✅ 优点
- **直观易懂**: 直接以金额表示潜在损失
- **标准化**: 金融机构广泛使用的风险指标
- **可比性**: 不同资产间的风险可直接比较
- **监管认可**: 银行和保险公司监管要求

### ❌ 缺点
- **尾部风险盲区**: 无法描述超出VaR的极端损失
- **分布假设**: 参数法依赖正态分布假设
- **历史依赖**: 历史模拟法假设历史会重演
- **非叠加性**: 组合VaR计算复杂

## 改进方法

### CVaR (条件VaR)
```python
# 计算超出VaR的平均损失
mask = daily_returns <= VaR_95
CVaR_95 = daily_returns[mask].mean()
```

### 动态VaR
```python
# GARCH模型预测波动率
from arch import arch_model
model = arch_model(daily_returns, vol='GARCH', p=1, q=1)
model_fit = model.fit()
forecasted_vol = model_fit.forecast(horizon=1).variance.iloc[-1, 0] ** 0.5
dynamic_VaR = mean_return - z_score * forecasted_vol
```

## 注意事项

### ⚠️ 数据要求
- **最少样本**: 至少30个观测值
- **数据质量**: 清洗异常值和停牌数据
- **频率一致**: 计算频率与应用频率匹配

### 🔍 模型验证
- **回测检验**: 实际损失超过VaR的频率应接近(1-置信水平)
- **Kupiec检验**: 统计检验VaR模型的准确性
- **滚动验证**: 样本外数据验证模型稳定性

### 💡 最佳实践
- **多方法验证**: 同时使用多种VaR计算方法
- **定期更新**: 根据市场变化调整模型参数
- **压力测试**: 结合极端情景分析
- **风险分解**: 分析VaR的来源和驱动因素 