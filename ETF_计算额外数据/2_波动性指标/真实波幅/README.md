# 真实波幅 (Average True Range - ATR)

## 指标概述
ATR由韦尔斯·怀尔德于1978年开发，是衡量价格波动性的核心指标。通过考虑跳空缺口影响，提供比简单价格范围更准确的波动率测量，是风险管理和仓位控制的基础工具。

## 计算公式

### 真实波幅 (TR)
```
TR = MAX(
    当日最高价 - 当日最低价,           # 当日波动幅度
    |当日最高价 - 前日收盘价|,         # 向上跳空影响  
    |当日最低价 - 前日收盘价|          # 向下跳空影响
)
```

### 平均真实波幅 (ATR)
```
ATR = TR的10日指数移动平均
ATR_10 = EMA(TR, 10)
```

### ATR百分比 (标准化)
```
ATR_Percent = (ATR_10 / 收盘价) × 100
用于跨ETF比较的关键指标
```

### 止损位计算
```
Stop_Loss = 收盘价 - (2.2 × ATR_10)
中国市场保守倍数设定
```

### 波动水平分级
```
Volatility_Level = 
  '高' if ATR_Percent > 3.0
  '低' if ATR_Percent < 1.5  
  '中' otherwise
```

### ATR变化率 (新增)
```
ATR_Change_Rate = (ATR_10_today - ATR_10_yesterday) / ATR_10_yesterday × 100
用于识别波动性突然变化，如重大消息影响
```

### ATR占区间比 (新增)
```
ATR_Ratio_HL = ATR_10 / (最高价 - 最低价) × 100  
反映波动效率，判断是震荡还是趋势性波动
```

## 最终输出字段 (7个核心字段)

### 字段定义
| 字段名 | 中文名 | 数据类型 | 描述 | 计算公式 |
|--------|--------|----------|------|----------|
| `tr` | 真实波幅 | float | 当日实际波动幅度 | MAX(H-L, \|H-PC\|, \|L-PC\|) |
| `atr_10` | 10日ATR | float | 核心波动性指标 | EMA(TR, 10) |
| `atr_percent` | ATR百分比 | float | 标准化波动率 | (atr_10/收盘价)×100 |
| `atr_change_rate` | ATR变化率 | float | 波动性日变化 | (atr_10今-atr_10昨)/atr_10昨×100 |
| `atr_ratio_hl` | ATR占区间比 | float | 波动效率 | atr_10/(最高价-最低价)×100 |
| `stop_loss` | 止损位 | float | 建议止损价位 | 收盘价 - 2.2×atr_10 |
| `volatility_level` | 波动水平 | string | 波动性分级 | 高/中/低 |

### 字段特征
| 字段名 | 实用性 | 使用频率 | 主要用途 |
|--------|--------|----------|----------|
| `tr` | 85% | 中等 | 技术分析基础数据 |
| `atr_10` | 95% | 极高 | 风险管理核心指标 |
| `atr_percent` | 90% | 高 | ETF筛选和比较 |
| `atr_change_rate` | 85% | 高 | 波动性变化预警 |
| `atr_ratio_hl` | 80% | 中等 | 波动效率分析 |
| `stop_loss` | 95% | 极高 | 风控止损位 |
| `volatility_level` | 80% | 中高 | 快速状态判断 |

## 核心应用

### 1. 止损设置
```python
# 基于ATR的科学止损
entry_price = 10.00
atr_10 = 0.15
multiplier = 2.2  # 中国市场保守设定

stop_loss = entry_price - (multiplier * atr_10)  # 9.67元
```

### 2. 仓位管理
```python
# 2%风险规则
account_capital = 100000
account_risk = 0.02
risk_per_share = 2.2 * atr_10  # 每股风险
position_size = (account_capital * account_risk) / risk_per_share
```

### 3. 波动性判断
```
volatility_level 分级标准:
- 低: atr_percent < 1.5% (市场平静，可能酝酿突破)
- 中: atr_percent 1.5%-3% (正常波动水平)  
- 高: atr_percent > 3% (高波动，加强风控)
```

### 4. 波动性变化监控 (新增)
```python
# ATR变化率应用
if atr_change_rate > 20:    # 波动性突然放大
    # 可能有重大消息，加强风控
elif atr_change_rate < -20: # 波动性快速收敛  
    # 市场趋于平静，可考虑建仓
```

### 5. 波动效率分析 (新增)
```python
# ATR占区间比应用
if atr_ratio_hl > 80:      # 波动充分释放
    # 可能转向震荡，适合网格策略
elif atr_ratio_hl < 50:    # 波动未充分释放
    # 可能继续突破，跟随趋势
```

## 中国市场优化

### 参数设置
- **周期**: 10日 (适合中国2周交易日)
- **倍数**: 2.2 (考虑T+1制度的保守设定)
- **涨跌停修正**: 接近±10%时TR×1.2

### 数据要求
基于日更11字段数据完全满足：
```
输入字段 (从11字段中使用):
- 最高价 (必需)
- 最低价 (必需)  
- 收盘价 (必需)
- 上日收盘 (必需)
- 涨幅% (可选，用于涨跌停修正)

输出字段 (7个):
- tr, atr_10, atr_percent, atr_change_rate, atr_ratio_hl, stop_loss, volatility_level
```

### ETF市场基准
- **宽基ETF**: 正常范围 0.8%-2.5%
- **行业ETF**: 正常范围 1.2%-4.0%  
- **主题ETF**: 正常范围 1.5%-5.0%

## 技术实现要点

### 计算顺序
1. **tr计算** - 基于当日和前日价格
2. **atr_10** - tr的10日EMA平滑
3. **atr_percent** - atr标准化处理
4. **atr_change_rate** - atr变化率计算
5. **atr_ratio_hl** - atr占区间比计算
6. **stop_loss** - 基于atr的止损位
7. **volatility_level** - 基于atr_percent分级

### 数据处理
```python
# 缺失数据处理
- 前日收盘价缺失: 跳过该日计算
- 价格异常值: H<L时使用前日数据
- 涨跌停修正: |涨幅%|>=9.8时TR×1.2

# 初始化要求  
- 最少需要10日历史数据计算atr_10
- 首日atr可用简单移动平均初始化
```

### 数值精度
- **tr**: 保留8位小数 (统一精度)
- **atr_10**: 保留8位小数 (统一精度)  
- **atr_percent**: 保留8位小数 (统一精度)
- **atr_change_rate**: 保留8位小数 (统一精度)
- **atr_ratio_hl**: 保留8位小数 (统一精度)
- **stop_loss**: 保留8位小数 (统一精度)
- **volatility_level**: 字符串类型

### 性能考虑
- **计算复杂度**: O(n) 线性时间
- **内存占用**: 仅需保存10日数据
- **优化建议**: 可使用增量计算避免重复计算

### 代码示例 (完整版)
```python
def calculate_atr_fields(df):
    # 1. tr计算
    df['tr'] = df.apply(lambda row: max(
        row['最高价'] - row['最低价'],
        abs(row['最高价'] - row['上日收盘']),
        abs(row['最低价'] - row['上日收盘'])
    ), axis=1)
    
    # 2. atr_10
    df['atr_10'] = df['tr'].ewm(span=10).mean()
    
    # 3. atr_percent  
    df['atr_percent'] = (df['atr_10'] / df['收盘价'] * 100).round(8)
    
    # 4. atr_change_rate (新增)
    df['atr_change_rate'] = (df['atr_10'].pct_change() * 100).round(8)
    
    # 5. atr_ratio_hl (新增)  
    df['atr_ratio_hl'] = (df['atr_10'] / (df['最高价'] - df['最低价']) * 100).round(8)
    
    # 6. stop_loss
    df['stop_loss'] = (df['收盘价'] - 2.2 * df['atr_10']).round(8)
    
    # 7. volatility_level
    df['volatility_level'] = df['atr_percent'].apply(
        lambda x: '高' if x > 3.0 else ('低' if x < 1.5 else '中')
    )
    
    return df[['tr', 'atr_10', 'atr_percent', 'atr_change_rate', 
               'atr_ratio_hl', 'stop_loss', 'volatility_level']]
```

## 适用场景
- ✅ 止损位设置 (风险管理核心)
- ✅ 仓位大小计算 (资金管理)  
- ✅ ETF波动性比较 (选股筛选)
- ✅ 市场状态判断 (策略调整)
- ✅ 波动性变化预警 (事件驱动识别) 
- ✅ 波动效率分析 (震荡vs突破判断)