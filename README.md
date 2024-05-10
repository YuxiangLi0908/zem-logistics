# zem-logistics
### Todos
- [X] BOL export
- [X] Enable editing after palletization
- [X] Shipment batch management
- [X] Change offload logic to enable shipment scheduled for not offload and palletized container/packling lists
- [X] DO export
- [X] Quote management - enable edit after creation
- [X] Delivery confirmation and POD upload
- [X] Add more search criteria to BOL. e.g. container number, appointment number, destination etc.
- [ ] Packing list deliver status check
- [ ] Add L/W/H to packing list to identify oversize
- [ ] Record the people/user for each operation

# Last Update
### 2024-02-01
- 修改了拆柜逻辑，允许同一地址合并拆柜
  - 暂扣留仓分开拆
  - 新增Pallet Model用来记录pallet和packing list关系： many to many
  - 拆柜后不可修改数据（需要看看是不是要改成可修改）
  - pallet count，cbm，pcs，weight的计算需要依靠Pallet model
    - 预约、出库、BOL
  - 拆柜单导出

### 2024-04-20
-  新增撤销入库、撤销预约功能
-  新增询价管理模块
-  完善客户管理模块
-  增加托盘标打印功能
-  解决pdf导出时中文乱码问题
-  页面优化

### 2024-04-28
-  增加DO导出
-  增加确认送达，POD回传
   -  upload to sharepoint site
-  BOL字体优化
-  预约出库增加3rd party地址

### 2024-05-10
-  调整托盘标字体大小、布局
-  完善直送订单出入库逻辑
-  增添财务管理模块
   - 设置是同权限accounting group 
   - 托盘数据导出
- 完善询盘功能
  - 历史询盘记录查询和更新
- 维护amazon和walmart仓库地址
- 拆柜单导出增加唛头
- 更新部分表格的filter
  - 添加BOL搜索字段：柜号、预约号、目的地
  - 优化table filter布局