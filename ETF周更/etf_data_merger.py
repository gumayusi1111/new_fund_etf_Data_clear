import os
import pandas as pd
import shutil
from typing import List, Set


def _collect_csv(directory: str) -> Set[str]:
    """列出目录中所有 csv 文件名集合（不含路径）。"""
    if not os.path.isdir(directory):
        return set()
    return {f for f in os.listdir(directory) if f.lower().endswith('.csv')}


def _read(path: str) -> pd.DataFrame:
    """读取 CSV，若文件不存在返回空 DataFrame。"""
    if not os.path.isfile(path):
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str)


def _merge(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """根据日期/time/trade_time 去重合并。"""
    if df_old.empty:
        return df_new.copy()
    if df_new.empty:
        return df_old.copy()

    key_cols = [c for c in ['日期', 'time', 'trade_time'] if c in df_old.columns]
    if not key_cols:
        # 无键列直接按顺序去重
        combined = pd.concat([df_new, df_old], ignore_index=True)
        return combined.drop_duplicates()

    combined = pd.concat([df_new, df_old], ignore_index=True)
    combined.drop_duplicates(subset=key_cols, keep='first', inplace=True)
    return combined


def merge_two_folders(hist_dir: str, new_dir: str) -> None:
    """合并两个文件夹下的 csv 文件，结果写回 hist_dir。"""
    if not os.path.isdir(hist_dir):
        raise ValueError(f"路径不存在或不是文件夹: {hist_dir}")
    if not os.path.isdir(new_dir):
        raise ValueError(f"路径不存在或不是文件夹: {new_dir}")

    names = _collect_csv(hist_dir) | _collect_csv(new_dir)

    for name in names:
        path_old = os.path.join(hist_dir, name)
        path_new = os.path.join(new_dir, name)
        df_old = _read(path_old)
        df_new = _read(path_new)

        merged = _merge(df_old, df_new)
        if not merged.empty:
            merged.to_csv(path_old, index=False, encoding='utf-8-sig')
        # 如果 new 文件存在且已合并，则删除
        if os.path.exists(path_new):
            os.remove(path_new)
        print(f'已合并: {name}')

    # 将 new_dir 中剩余 csv（可能是新上市）移动过来
    for name in os.listdir(new_dir):
        if not name.lower().endswith('.csv'):
            continue
        shutil.move(os.path.join(new_dir, name), os.path.join(hist_dir, name))
        print(f'移动新文件: {name}')


def merge_monthly_data(root_dir: str, months: List[str] = None) -> None:
    """在根目录下自动将指定月份子目录数据合并到对应历史目录。
    
    Args:
        root_dir: ETF数据根目录
        months: 要合并的月份列表，如 ['2025年5月', '2025年6月']，默认为5、6月
    """
    if months is None:
        months = ['2025年5月', '2025年6月']
    
    categories = [
        '0_ETF日K(前复权)',
        '0_ETF日K(后复权)',
        '0_ETF日K(除权)',
    ]

    for cat in categories:
        hist_dir = os.path.join(root_dir, cat)
        
        if not os.path.isdir(hist_dir):
            print(f'⚠️ 找不到历史目录: {hist_dir}')
            continue

        # 依次合并指定月份
        for month in months:
            month_dir = os.path.join(root_dir, f'{cat}_{month}')
            
            if os.path.isdir(month_dir):
                print(f'\n合并 {cat} - {month}数据...')
                merge_two_folders(hist_dir, month_dir)
            else:
                print(f'未找到月份目录: {month_dir}')

    print('\n全部分类合并完成 ✅')


if __name__ == '__main__':
    # 假设脚本放在 ETF_按代码 目录内
    script_dir = os.path.dirname(os.path.abspath(__file__))
    merge_monthly_data(script_dir) 