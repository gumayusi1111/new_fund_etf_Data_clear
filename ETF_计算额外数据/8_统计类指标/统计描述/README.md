# 统计描述 (Statistical Description)

## 概述
统计描述指标通过各种统计方法对ETF价格数据进行量化分析，提供价格分布特征、极值信息和统计属性，帮助投资者全面了解ETF的历史表现特征。

## 主要指标分类

### 1. 中心趋势指标

#### 算术平均数
- **公式**: μ = Σxi / n
- **用途**: 反映价格的平均水平
- **应用**: 基础统计参考
- **注意**: 受极值影响较大

#### 几何平均数
- **公式**: G = ⁿ√(x₁ × x₂ × ... × xₙ)
- **用途**: 适合收益率的平均计算
- **优势**: 复合增长率的精确表示
- **应用**: 年化收益率计算

#### 中位数 (Median)
- **定义**: 排序后位于中间位置的数值
- **特点**: 不受极值影响
- **用途**: 鲁棒的中心位置指标
- **应用**: 异常值检测

#### 众数 (Mode)
- **定义**: 出现频率最高的数值
- **应用**: 识别价格聚集区域
- **用途**: 心理价位分析

### 2. 离散程度指标

#### 标准差 (Standard Deviation)
- **公式**: σ = √(Σ(xi - μ)² / n)
- **用途**: 衡量数据离散程度
- **应用**: 风险度量的基础
- **年化**: σ_年化 = σ_日 × √252

#### 方差 (Variance)
- **公式**: σ² = Σ(xi - μ)² / n
- **关系**: 标准差的平方
- **用途**: 理论分析基础
- **应用**: 风险分解和归因

#### 极差 (Range)
- **公式**: R = Max - Min
- **特点**: 简单直观
- **缺陷**: 只考虑极值
- **应用**: 快速风险评估

#### 四分位距 (Interquartile Range, IQR)
- **公式**: IQR = Q3 - Q1
- **优势**: 排除极值影响
- **用途**: 鲁棒的离散度量
- **应用**: 异常值识别

### 3. 分布形状指标

#### 偏度 (Skewness)
- **公式**: Skew = E[(X-μ)³] / σ³
- **解释**: 
  - Skew > 0: 右偏，正收益较多
  - Skew < 0: 左偏，负收益较多
  - Skew = 0: 对称分布
- **应用**: 收益分布特征分析

#### 峰度 (Kurtosis)
- **公式**: Kurt = E[(X-μ)⁴] / σ⁴
- **解释**:
  - Kurt > 3: 尖峰分布，极值较多
  - Kurt < 3: 平峰分布，极值较少
  - Kurt = 3: 正态分布
- **应用**: 尾部风险评估

### 4. 极值统计

#### 最大值/最小值
- **N日最高价**: N日内的最高价格
- **N日最低价**: N日内的最低价格
- **用途**: 支撑阻力位识别
- **周期**: 20日、60日、252日

#### 价格分位数
- **定义**: 将数据按大小排序后的位置
- **常用分位数**: 
  - Q1 (25%分位数)
  - Q2 (50%分位数，中位数)
  - Q3 (75%分位数)
  - P5, P10, P90, P95分位数
- **应用**: 价格相对位置判断

#### 最大回撤 (Maximum Drawdown)
- **公式**: MDD = Max((Peak - Trough) / Peak)
- **计算**: 从历史最高点到后续最低点的最大跌幅
- **用途**: 下行风险评估
- **重要性**: 投资者心理承受能力

### 5. 连续性统计

#### 连涨/连跌天数
- **定义**: 连续上涨或下跌的交易日数
- **统计**: 最大连涨天数、最大连跌天数
- **应用**: 趋势持续性分析

#### 上涨/下跌频率
- **计算**: 上涨天数 / 总交易天数
- **用途**: 胜率统计
- **应用**: 策略评估基础

## 计算方法

### 基础统计指标
```python
def basic_statistics(data):
    """计算基础统计指标"""
    stats = {
        'mean': data.mean(),
        'median': data.median(),
        'std': data.std(),
        'var': data.var(),
        'min': data.min(),
        'max': data.max(),
        'range': data.max() - data.min(),
        'q1': data.quantile(0.25),
        'q3': data.quantile(0.75),
        'iqr': data.quantile(0.75) - data.quantile(0.25)
    }
    return stats
```

### 分布形状指标
```python
from scipy import stats

def distribution_stats(data):
    """计算分布形状指标"""
    return {
        'skewness': stats.skew(data),
        'kurtosis': stats.kurtosis(data),
        'jarque_bera': stats.jarque_bera(data)
    }
```

### 最大回撤计算
```python
def max_drawdown(price_series):
    """计算最大回撤"""
    cumulative_max = price_series.expanding().max()
    drawdown = (price_series - cumulative_max) / cumulative_max
    max_dd = drawdown.min()
    return abs(max_dd)
```

## 输出字段
- `Mean_Price_20`: 20日平均价格
- `Median_Price_20`: 20日中位价格
- `Max_Price_20`: 20日最高价
- `Min_Price_20`: 20日最低价
- `Price_Range_20`: 20日价格极差
- `Price_Std_20`: 20日价格标准差
- `Price_Percentile_10`: 价格10%分位数
- `Price_Percentile_90`: 价格90%分位数
- `Skewness_20`: 20日收益率偏度
- `Kurtosis_20`: 20日收益率峰度
- `Max_Drawdown_60`: 60日最大回撤
- `Average_Amplitude`: 平均振幅

## 实战应用

### 1. 价格定位分析
```python
def price_position_analysis(current_price, price_stats):
    """分析当前价格相对位置"""
    percentile = (current_price - price_stats['min']) / price_stats['range'] * 100
    
    if percentile > 80:
        return "高位区域"
    elif percentile < 20:
        return "低位区域"
    else:
        return "中位区域"
```

### 2. 风险等级评估
```python
def risk_level_assessment(volatility, max_drawdown, skewness):
    """风险等级评估"""
    risk_score = 0
    
    # 波动率评分
    if volatility > 0.3:
        risk_score += 3
    elif volatility > 0.2:
        risk_score += 2
    else:
        risk_score += 1
    
    # 最大回撤评分
    if max_drawdown > 0.2:
        risk_score += 3
    elif max_drawdown > 0.1:
        risk_score += 2
    else:
        risk_score += 1
    
    # 偏度评分（负偏度增加风险）
    if skewness < -0.5:
        risk_score += 2
    elif skewness < 0:
        risk_score += 1
    
    return "高风险" if risk_score >= 6 else "中风险" if risk_score >= 4 else "低风险"
```

### 3. 异常值检测
```python
def outlier_detection(data, method='iqr'):
    """异常值检测"""
    if method == 'iqr':
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = data[(data < lower_bound) | (data > upper_bound)]
    
    elif method == 'zscore':
        z_scores = np.abs(stats.zscore(data))
        outliers = data[z_scores > 3]
    
    return outliers
```

## 应用场景

### 1. 投资分析
- **历史业绩评估**: 通过统计指标了解ETF历史表现
- **风险收益特征**: 分析收益分布和风险水平
- **相对价值判断**: 当前价格在历史分布中的位置

### 2. 风险管理
- **下行风险**: 最大回撤、下行波动率
- **极端风险**: 峰度分析、尾部分布
- **组合风险**: 相关性和分散化效果

### 3. 策略开发
- **参数优化**: 基于历史统计特征设置参数
- **回测评估**: 使用统计指标评估策略表现
- **模型验证**: 检验模型假设的合理性

## 注意事项
- 统计指标基于历史数据，不等于未来表现
- 需要足够的样本量保证统计有效性
- 市场环境变化可能影响统计特征
- 应结合多个指标综合分析
- 注意数据质量和异常值处理 