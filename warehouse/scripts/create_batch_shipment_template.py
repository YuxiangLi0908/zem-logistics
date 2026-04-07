import pandas as pd
import os

# 确保输出目录存在
output_dir = os.path.join('warehouse', 'templates', 'export_file')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 创建模板数据
columns = ['柜号', '仓点', 'CBM', '卡板', '预约时间', 'ISA', 'pickup time', 'PickUp number', 'Shipment ID', '预约账号', '预约类型', '装车类型']

# 创建一个空的DataFrame
df = pd.DataFrame(columns=columns)

# 添加示例数据
df.loc[0] = ['TRXU1234567', 'LA-91761', 28, 20, '2026-04-10', 'ISA123456', '2026-04-10 09:00', 'PU123456', 'SHIP123456', 'Carrier Central1', 'FTL', '卡板']
df.loc[1] = ['MSCU7654321', 'LA-91761', 25, 18, '', '', '', '', '', '', '', '']
df.loc[2] = ['', '', '', '', '', '', '', '', '', '', '', '']  # 空行，用于分组
df.loc[3] = ['CMAU9876543', 'NJ-07001', 30, 22, '2026-04-11', 'ISA654321', '2026-04-11 10:00', 'PU654321', 'SHIP654321', 'ZEM-AMF', '外配', '地板']

# 保存为Excel文件
output_path = os.path.join(output_dir, 'batch_shipment_template.xlsx')
df.to_excel(output_path, index=False)
print(f"模板文件已生成: {output_path}")
