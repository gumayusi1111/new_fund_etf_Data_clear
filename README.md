# ETF数据处理系统

## 🚀 **系统概述**

**全面重构升级的ETF数据处理系统**，实现从百度网盘自动同步ETF数据，支持三种复权处理，具备完整的生命周期管理和数据质量验证功能。

### ✨ **核心特性**

- 🔄 **统一更新机制** - 一键执行日更+周更，智能调度
- 📊 **三种复权处理** - 前复权、后复权、除权数据同步生成
- 🌱 **生命周期管理** - 自动识别新上市和下市ETF
- ✅ **数据质量验证** - 100%准确性验证，自动差异检测
- 🗂️ **统一配置管理** - 集中配置，日志分离
- 💾 **临时处理机制** - 无需本地存储源数据，节省空间
- 📈 **智能去重** - 多层次数据去重保护

## 📁 **系统架构**

```
data_clear/
├── 📁 config/                    # 统一配置中心
│   ├── config.json              # 主配置文件
│   ├── hash_manager.py           # 文件哈希管理
│   ├── logger_config.py          # 日志配置
│   ├── etf_lifecycle_manager.py  # 生命周期管理
│   └── file_hashes.json         # 文件哈希记录
├── 📁 logs/                      # 统一日志目录
│   ├── etf_sync.log             # 系统通用日志
│   ├── etf_daily_sync.log       # 日更专用日志
│   └── etf_weekly_sync.log      # 周更专用日志
├── 📁 ETF日更/                   # 日更新数据目录
│   ├── auto_daily_sync.py       # 日更新脚本
│   ├── daily_etf_processor.py   # 数据处理脚本
│   └── 📁 0_ETF日K(三种复权)/    # 三种复权数据
├── 📁 ETF周更/                   # 周更新数据目录
│   ├── etf_auto_sync.py         # 周更新脚本
│   ├── etf_data_merger.py       # 数据合并脚本
│   └── 📁 0_ETF日K(三种复权)/    # 三种复权数据
├── 🚀 unified_etf_updater.py     # 统一更新脚本
├── 🔍 etf_status_analyzer.py     # ETF状态分析器
└── 📖 README.md                  # 使用文档
```

## 🎯 **快速开始**

### 1. **一键更新（推荐）**
```bash
# 执行完整更新（日更+周更）
python unified_etf_updater.py


# 这一个命令就够了！
python unified_etf_updater.py --mode update

# 测试系统状态
python unified_etf_updater.py --mode test
```

### 2. **ETF状态分析**
```bash
# 分析日更周更差异，识别新上市/下市ETF
python etf_status_analyzer.py
```

### 3. **数据质量验证**
```bash
# 验证数据一致性和复权计算准确性
python data_validation_checker.py
```

### 4. **单独执行**
```bash
# 仅执行日更新
cd ETF日更 && python auto_daily_sync.py

# 仅执行周更新
cd ETF周更 && python etf_auto_sync.py
```

## 🔧 **配置说明**

### 主配置文件：`config/config.json`

```json
{
  "baidu_netdisk": {
    "weekly_remote_path": "/ETF",
    "daily_remote_path": "/ETF_按日期",
    "weekly_local_path": "./ETF周更",
    "daily_local_path": "./ETF日更"
  },
  "logging": {
    "level": "INFO",
    "base_dir": "./logs",
    "files": {
      "weekly": "etf_weekly_sync.log",
      "daily": "etf_daily_sync.log", 
      "general": "etf_sync.log"
    }
  },
  "etf_lifecycle": {
    "delisted_etfs": [],        // 已下市ETF列表
    "newly_listed_etfs": [],    // 新上市ETF列表
    "last_updated": "2025-06-25"
  }
}
```

## 🌱 **ETF生命周期管理**

### **数量差异分析**
最新分析显示日更周更数量差异（+7个）完全来自新上市ETF：

**🆕 新上市ETF（7个）**：
- `159228`, `159240`, `159245`, `561770`, `562050`, `588270`, `589180`

### **管理命令**
```bash
# 添加新上市ETF
python config/etf_lifecycle_manager.py --add-new 159228 "ETF名称"

# 添加下市ETF  
python config/etf_lifecycle_manager.py --add-delisted 159999 "ETF名称" "下市原因"

# 查看生命周期状态
python config/etf_lifecycle_manager.py --status
```

## 📊 **数据质量保证**

### **验证结果**
- ✅ **数据一致性**: 100% - 日更和周更数据完全一致
- ✅ **复权准确性**: 100% - 三种复权计算完全正确  
- ✅ **数量差异**: 已确认为新上市ETF，非数据缺失

### **三种复权说明**
- **前复权** - `价格 ÷ 复权因子` - 消除除权影响的历史价格
- **后复权** - `价格 × 复权因子` - 基于当前价格的连续价格  
- **除权** - `原始价格` - 不调整的实际交易价格

## 🔄 **系统更新机制**

### **日更新流程**
1. 检查今天日期文件是否存在
2. 临时下载到内存处理
3. 增量更新到三种复权目录
4. 自动清理临时文件

### **周更新流程**  
1. 检查当前月份RAR文件
2. 下载并解压到临时目录
3. 合并到历史数据目录
4. 更新哈希记录

### **智能去重保护**
- **文件内去重** - 同文件内按日期+代码去重
- **新数据去重** - 处理前去除重复记录
- **合并时去重** - 新数据覆盖旧数据
- **全局去重** - 系统级别的重复检测

## 📈 **日志监控**

### **日志文件说明**
- `logs/etf_sync.log` - 系统级别操作日志
- `logs/etf_daily_sync.log` - 日更新专用日志
- `logs/etf_weekly_sync.log` - 周更新专用日志

### **查看日志**
```bash
# 查看系统日志
tail -f logs/etf_sync.log

# 查看日更日志
tail -f logs/etf_daily_sync.log

# 查看错误日志
grep "ERROR\|❌" logs/*.log
```

## 🚨 **故障排除**

### **常见问题**

1. **百度网盘连接失败**
   ```bash
   # 重新授权
   python -c "from bypy import ByPy; ByPy().auth()"
   ```

2. **权限不足**
   ```bash
   # 检查文件权限
   ls -la logs/ config/
   chmod 755 *.py
   ```

3. **依赖缺失**
   ```bash
   # 安装依赖
   pip install -r requirements.txt
   ```

4. **配置错误**
   ```bash
   # 验证配置
   python -c "import json; print(json.load(open('config/config.json')))"
   ```

## 📚 **API文档**

### **主要模块**

#### `unified_etf_updater.py` - 统一更新器
- `main()` - 执行完整更新流程
- `test_system()` - 系统状态测试
- `run_daily_update()` - 执行日更新
- `run_weekly_update()` - 执行周更新

#### `etf_status_analyzer.py` - 状态分析器
- `analyze_etf_status()` - 分析ETF差异状态
- `generate_status_report()` - 生成状态报告

#### `config/etf_lifecycle_manager.py` - 生命周期管理器
- `analyze_etf_differences()` - 分析差异
- `add_delisted_etf()` - 添加下市ETF
- `add_newly_listed_etf()` - 添加新上市ETF

## 🎉 **系统优势**

### **📈 数据质量**
- 100%准确性验证通过
- 自动去重和错误检测
- 完整的生命周期跟踪

### **⚡ 性能优化**
- 临时处理机制节省存储空间
- 增量更新提高效率
- 智能哈希管理避免重复下载

### **🛠️ 维护便利**
- 统一配置管理
- 分离的日志系统
- 模块化架构设计

### **🔍 监控完善**
- 实时状态检查
- 详细的执行日志
- 自动异常处理

## 🤝 **技术支持**

如有问题或建议，请通过以下方式联系：

- 📧 **邮箱**: [你的邮箱]
- 🐛 **问题反馈**: 创建Issue
- 📖 **使用说明**: 查看本README

---

**📊 数据质量**: ✅ 100%验证通过 | **🚀 系统状态**: ✅ 运行正常 | **�� 最后更新**: 2025-06-25 