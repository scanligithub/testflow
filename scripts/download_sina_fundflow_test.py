# scripts/download_sina_fundflow_test.py
import requests
import pandas as pd
import time
import os
from tqdm import tqdm  # 引入进度条

# ====================== 配置 ======================
TEST_CODE = "sh.600519"        # 测试股票：贵州茅台
PAGE_SIZE = 50
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

COLUMN_MAP = {
    'opendate': 'date',
    'trade': 'close',
    'changeratio': 'pct_change',
    'turnover': 'turnover_rate',
    'netamount': 'net_flow_amount',
    'r0_net': 'main_net_flow',
    'r1_net': 'super_large_net_flow',
    'r2_net': 'large_net_flow',
    'r3_net': 'medium_small_net_flow'
}

# ====================== 核心函数 ======================
def get_fundflow_history(stock_code: str) -> pd.DataFrame:
    """获取单只股票全部历史资金流向（带分页进度条）"""
    all_data = []
    page = 1
    code_api = stock_code.replace('.', '')
    total_pages = None  # 动态推测总页数

    print(f"开始爬取 {stock_code} 资金流向数据...")

    # 使用 tqdm 进度条（未知总页数时用 leave=True）
    with tqdm(desc="分页下载", unit="页", leave=True) as pbar:
        while True:
            url = f"{SINA_API}?page={page}&num={PAGE_SIZE}&sort=opendate&asc=0&daima={code_api}"
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                resp.encoding = 'gbk'
                data = resp.json()

                if not data or len(data) == 0:
                    print(f"\n第 {page} 页无数据，停止爬取。")
                    break

                all_data.extend(data)
                current_count = len(data)

                # 动态更新进度条
                pbar.set_postfix({
                    "本页": current_count,
                    "累计": len(all_data)
                })
                pbar.update(1)

                # 尝试推测总页数（仅首次）
                if total_pages is None and len(data) < PAGE_SIZE:
                    total_pages = page
                    pbar.total = page
                    pbar.refresh()

                if len(data) < PAGE_SIZE:
                    break

                page += 1
                time.sleep(0.3)
            except Exception as e:
                print(f"\n请求第 {page} 页失败: {e}")
                break

    if not all_data:
        print("未获取到任何数据")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    print(f"\n原始数据共 {len(df)} 行，{len(df.columns)} 列")
    return df

def clean_and_save(df: pd.DataFrame, stock_code: str):
    """清洗 + 保存 Parquet (ZSTD) + CSV (UTF-8)"""
    if df.empty:
        return

    available_cols = [k for k in COLUMN_MAP.keys() if k in df.columns]
    if not available_cols:
        print("缺少关键字段，跳过保存")
        return

    df_clean = df[available_cols].copy()
    df_clean = df_clean.rename(columns=COLUMN_MAP)

    # 类型转换
    df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce')
    numeric_cols = df_clean.columns.drop('date')
    df_clean[numeric_cols] = df_clean[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # 添加代码列
    df_clean['code'] = stock_code
    df_clean = df_clean.sort_values('date').reset_index(drop=True)

    base_name = f"fundflow_{stock_code}"

    # 保存 Parquet (ZSTD 压缩)
    parquet_path = os.path.join(OUTPUT_DIR, f"{base_name}.parquet")
    try:
        df_clean.to_parquet(parquet_path, index=False, compression='zstd')
        print(f"Parquet 已保存：{parquet_path}")
    except Exception as e:
        print(f"Parquet 保存失败: {e}")

    # 保存 CSV (UTF-8)
    csv_path = os.path.join(OUTPUT_DIR, f"{base_name}.csv")
    try:
        df_clean.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"CSV 已保存：{csv_path}")
    except Exception as e:
        print(f"CSV 保存失败: {e}")

    # 打印摘要
    print(f" → 记录数: {len(df_clean):,}")
    print(f" → 日期范围: {df_clean['date'].min().date()} ~ {df_clean['date'].max().date()}")

# ====================== 主函数 ======================
def main():
    print("=" * 60)
    print("新浪财经资金流向数据爬取测试（带进度条 + 双格式输出）")
    print(f"目标股票: {TEST_CODE}")
    print("=" * 60)

    raw_df = get_fundflow_history(TEST_CODE)
    if not raw_df.empty:
        print("\n原始字段示例:")
        print(raw_df.head(1).T)
        print("-" * 60)
        clean_and_save(raw_df, TEST_CODE)
    else:
        print("测试失败：未获取到数据")

    print("\n测试完成！输出文件在 output/ 目录")

if __name__ == "__main__":
    main()
