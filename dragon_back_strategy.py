import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：龙回头缩量回踩战法 (Dragon-Back)
# 核心逻辑：
# 1. 寻找近期（20日内）曾有暴力启动（连续涨停或大阳线）的股票。
# 2. 股价随后进入缩量回调阶段，回踩至起涨点（水平支撑线）。
# 3. 筛选条件：价格 5.0-20.0元，排除ST、创业板(30)、科创板(688)。
# 4. 买入信号：缩量回踩守住支撑位，成交量萎缩到极点后开始温和放大。
# ==========================================

DATA_DIR = './stock_data/'
NAMES_FILE = 'stock_names.csv'

def analyze_stock(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 基础过滤：代码格式
        code = os.path.basename(file_path).split('.')[0]
        if code.startswith(('30', '688')) or 'ST' in code: return None
        
        # 获取最新价格和基本面限制
        last_row = df.iloc[-1]
        close_price = last_row['收盘']
        if not (5.0 <= close_price <= 20.0): return None

        # 战法逻辑计算
        # 1. 寻找过去20天内的强势起涨点 (暴涨波段)
        recent_20 = df.tail(20).copy()
        max_vol_idx = recent_20['成交量'].idxmax()
        start_price = df.loc[max_vol_idx, '开盘'] # 定义为起涨点支撑位
        
        # 2. 回踩深度校验：当前价在起涨点附近（上下3%波动）
        price_diff = abs(close_price - start_price) / start_price
        is_at_support = price_diff <= 0.03
        
        # 3. 缩量校验：当前成交量必须小于起涨当天成交量的 50% (典型洗盘)
        vol_ratio = last_row['成交量'] / df.loc[max_vol_idx, '成交量']
        is_shrinking = vol_ratio < 0.5

        # 4. 信号分级
        score = 0
        advice = "待观察"
        
        if is_at_support and is_shrinking:
            score = 70
            advice = "试错性买入 (轻仓)"
            # 如果收盘价站稳在5日线之上，加分
            if close_price > recent_20['收盘'].rolling(5).mean().iloc[-1]:
                score += 20
                advice = "重点关注 (半仓进攻)"
            # 如果今日量能开始较昨日温和放大，说明有资金回流
            if last_row['成交量'] > df.iloc[-2]['成交量']:
                score += 10
                advice = "一击必中 (核心重仓)"

        if score >= 70:
            return {
                'code': code,
                'latest_close': close_price,
                'support_price': round(start_price, 2),
                'signal_score': score,
                'operation_advice': advice
            }
    except Exception:
        return None

def main():
    stock_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    # 并行处理提高速度
    with Pool(cpu_count()) as p:
        results = p.map(analyze_stock, stock_files)
    
    results = [r for r in results if r is not None]
    
    # 匹配股票名称
    if os.path.exists(NAMES_FILE):
        names_df = pd.read_csv(NAMES_FILE)
        names_df['code'] = names_df['code'].astype(str).str.zfill(6)
        final_df = pd.DataFrame(results)
        if not final_df.empty:
            final_df = final_df.merge(names_df, on='code', how='left')
            
            # 优中选优：按分数排序
            final_df = final_df.sort_values(by='signal_score', ascending=False).head(5)
            
            # 保存结果
            now = datetime.now()
            dir_path = now.strftime('%Y-%m')
            os.makedirs(dir_path, exist_ok=True)
            file_name = f"dragon_back_strategy_{now.strftime('%Y%m%d_%H%M%S')}.csv"
            save_path = os.path.join(dir_path, file_name)
            final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
            print(f"复盘完成，选出 {len(final_df)} 只潜力股。结果已保存至 {save_path}")

if __name__ == "__main__":
    main()
