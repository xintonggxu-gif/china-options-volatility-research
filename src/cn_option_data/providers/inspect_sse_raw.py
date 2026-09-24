import akshare as ak

# 1. 获取上交所当日期权合约表
df = ak.option_current_day_sse()

# 2. 查看基本信息
print("行数：", len(df))

print("\n原始字段名：")
for i, column in enumerate(df.columns):
    print(i, repr(column))

print("\n字段类型：")
print(df.dtypes)

print("\n前5行：")
print(df.head().to_string())

# 3. 原样保存为CSV
output_file = "sse_option_current_day_raw.csv"

df.to_csv(
    output_file,
    index=False,
    encoding="utf-8-sig",  # 用Excel打开中文不会乱码
)

print(f"\nCSV已经保存：{output_file}")