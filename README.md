# zem-logistics
### Todos
- [X] BOL export
- [ ] Enable editing after palletization
- [ ] Shipment batch management
- [ ] Change offload logic to enable shipment scheduled for not offload and palletized container/packling lists
- [ ] DO export
- [ ] Record the people/user for each operation

# Last Update
- 修改了拆柜逻辑，允许同一地址合并拆柜
  - 暂扣留仓分开拆
  - 新增Pallet Model用来记录pallet和packing list关系： many to many
  - 拆柜后不可修改数据（需要看看是不是要改成可修改）
  - pallet count，cbm，pcs，weight的计算需要依靠Pallet model
    - 预约、出库、BOL
  - 拆柜单导出
- 待完成
  - DO导出
