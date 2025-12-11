import yfinance as yf
import pandas as pd
import os 
import io

# ====================================================================
# --- 配置參數 ---
# 請將 FILE_PATH 設回您目前正在使用的檔案名稱
FILE_PATH = 'stock_2449_data.csv'   
TICKER = '2449.TW'                  
REQUIRED_COLUMNS = ['Close', 'High', 'Low', 'Open', 'Volume'] 


# ====================================================================
# 第一部分：數據下載函式
# ====================================================================

def fetch_latest_data(ticker, start_date):
    """從 yfinance 下載最新數據，並只保留需要的欄位。"""
    # 獲取從指定日期到今天的數據
    data = yf.download(ticker, start=start_date) 
    
    if not all(col in data.columns for col in REQUIRED_COLUMNS):
        print("❌ 警告：下載數據缺少部分標準欄位。")
        return None

    # 關鍵修正 1: 確保新數據只有需要的欄位，且順序正確
    data = data[REQUIRED_COLUMNS]
    data.index.name = 'Date' 
    
    # 強制扁平化欄位 (避免 yfinance 的 MultiIndex 污染)
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    
    return data

# ====================================================================
# 第二部分：腳本主體：讀取、清理、追加與儲存
# ====================================================================

print(f"🔄 正在運行數據更新腳本 (股票代號: {TICKER})...")

# 1. 讀取舊數據
if not os.path.exists(FILE_PATH):
    print(f"❌ 錯誤：找不到數據文件 {FILE_PATH}。")
    exit()

try:
    # 讀取時使用 usecols=range(6) 強制只讀取前 6 列 (繞過多餘的 header/數據)
    old_df = pd.read_csv(FILE_PATH, 
                         index_col=0,  # 將第 0 列 (Date) 設為索引
                         parse_dates=True,
                         usecols=range(len(REQUIRED_COLUMNS) + 1)) 
                         
    old_df.columns = REQUIRED_COLUMNS # 確保欄位名稱正確
    old_df.index.name = 'Date'       # 確保索引名稱為 Date
    
    # 強制舊數據的欄位也是扁平的
    if isinstance(old_df.columns, pd.MultiIndex):
        old_df.columns = old_df.columns.get_level_values(0)

except Exception as e:
    print(f"❌ 錯誤：讀取舊數據文件失敗，請檢查 CSV 格式或路徑。錯誤訊息: {e}")
    exit()

# 2. 清理 NaT 索引
old_df = old_df[pd.notna(old_df.index)] 

if old_df.empty:
    print("❌ 錯誤：舊數據中沒有有效的日期索引，無法繼續。")
    exit()

last_date = old_df.index[-1].strftime('%Y-%m-%d')
print(f"📜 已清理 NaT。最後有效日期：{last_date}。開始下載最新數據...")

# 3. 下載最新數據
new_raw_df = fetch_latest_data(TICKER, last_date)
# ... (錯誤檢查省略) ...

# 4. 移除重複數據並檢查是否需要更新
new_data_only = new_raw_df.loc[new_raw_df.index > old_df.index[-1]]

if new_data_only.empty:
    print("✅ 數據已是最新，無需追加。")
    exit()

# 5. 合併數據 
updated_df = pd.concat([old_df, new_data_only])

# 關鍵修正 2: 再次強制確保最終的 DataFrame 只有 5 個乾淨的欄位
updated_df = updated_df[REQUIRED_COLUMNS] 

# 6. 儲存回 CSV 檔案 (覆蓋舊檔案)
updated_df.to_csv(FILE_PATH, date_format='%Y-%m-%d', index=True, header=True)
print(f"🎉 數據更新成功！已儲存 {len(new_data_only)} 筆新數據到 {FILE_PATH}。")