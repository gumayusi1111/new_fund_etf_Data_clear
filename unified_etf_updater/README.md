# 统一ETF更新器模块

这是一个模块化的ETF数据更新和管理系统，提供了完整的ETF数据处理流程。

## 模块结构

```
unified_etf_updater/
├── __init__.py          # 模块初始化，导出主要类
├── core.py              # 核心类：UnifiedETFUpdater
├── database.py          # 数据库管理器：DatabaseManager
├── git_manager.py       # Git管理器：GitManager
├── updaters.py          # 各类更新器：ETFUpdaters
└── README.md           # 本文档
```

## 主要功能模块

### 1. 核心模块 (`core.py`)
- `UnifiedETFUpdater`: 主要的更新器类
- 负责整体流程协调和系统状态测试
- 提供配置管理和日志记录

### 2. 数据库管理器 (`database.py`)
- `DatabaseManager`: 数据库导入管理
- 支持日更、周更、市场状况数据的导入
- 动态加载数据库模块，优雅处理依赖缺失

### 3. Git管理器 (`git_manager.py`)
- `GitManager`: Git自动提交管理
- 智能识别数据文件变更
- 自动生成提交信息和推送

### 4. 更新器模块 (`updaters.py`)
- `ETFUpdaters`: 各类ETF数据更新器
- 包含日更、周更、市场状况、ETF初筛等更新流程
- 智能跳过无新数据的更新

## 使用方式

### 直接使用模块
```python
from unified_etf_updater import UnifiedETFUpdater

# 初始化更新器
updater = UnifiedETFUpdater()

# 执行完整更新
results = updater.run_full_update()

# 或者执行系统测试
updater.test_system_status()
```

### 通过主启动器使用
```bash
# 根目录下的main.py提供了简单的启动入口
python main.py                    # 执行完整数据更新
python main.py --mode test        # 系统状态测试
python main.py --no-git          # 禁用Git自动提交
python main.py --no-push         # 禁用Git推送
python main.py --no-screening    # 禁用ETF初筛
```

## 配置管理

所有配置通过 `config/config.json` 文件管理：

- `database_import`: 数据库导入配置
- `git_auto_commit`: Git自动提交配置  
- `etf_screening`: ETF初筛配置

## 优势特点

1. **模块化设计**: 各功能模块独立，便于维护和扩展
2. **智能跳过**: 自动检测是否有新数据，避免无意义的处理
3. **配置驱动**: 通过配置文件灵活控制各功能模块
4. **优雅降级**: 缺失依赖时不影响其他功能正常运行
5. **统一日志**: 所有模块使用统一的日志系统
6. **命令行友好**: 支持丰富的命令行参数

## 向后兼容

原有的 `unified_etf_updater.py` 文件可以通过新的模块化结构完全替代，保持相同的功能和接口。 