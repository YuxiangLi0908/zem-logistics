# zem-logistics
### Todos
- [X] BOL export
- [X] Enable editing after palletization
- [X] Shipment batch management
- [X] Change offload logic to enable shipment scheduled for not offload and palletized container/packling lists
- [ ] DO export
- [ ] Quote management - enable edit after creation
- [ ] Delivery confirmation and POD upload
- [ ] Packing list deliver status check
- [ ] Add L/W/H to packing list to identify oversize
- [ ] Add more search criteria to BOL. e.g. container number, appointment number, destination etc.
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