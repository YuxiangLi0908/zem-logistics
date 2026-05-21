from django.contrib.auth.models import User
from django.utils import timezone
import pytz
from datetime import datetime
from warehouse.models.shipment_bindlog import ShipmentBindingLog


class ShipmentBindingPermission:
    """权限校验类"""
    
    @staticmethod
    def has_public_permission(user: User) -> bool:
        """检查是否有公仓绑定权限"""
        return user.groups.filter(name="shipment_bind_public").exists() or user.is_superuser
    
    @staticmethod
    def has_other_permission(user: User) -> bool:
        """检查是否有私仓绑定权限"""
        return user.groups.filter(name="shipment_bind_other").exists() or user.is_superuser
    
    @staticmethod
    def has_permission(user: User, delivery_type: str) -> bool:
        """检查用户是否有该仓库类型的操作权限"""
        if not delivery_type:
            return True  # 没有指定仓库类型时，默认允许
        
        if delivery_type == 'public':
            return ShipmentBindingPermission.has_public_permission(user)
        elif delivery_type == 'other':
            return ShipmentBindingPermission.has_other_permission(user)
        
        return True  # 其他类型，默认允许


class ShipmentBindingLogger:
    """操作日志记录类"""
    
    @staticmethod
    def get_beijing_time() -> datetime:
        """获取北京时间（naive datetime，直接表示北京时间）"""
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now_utc = timezone.now()  # 获取当前 aware 的 UTC 时间
        beijing_time = now_utc.astimezone(beijing_tz)  # 转换为北京时间 aware
        
        # 返回 naive datetime，小时、分钟、秒都保留正确的北京时间
        return datetime(
            year=beijing_time.year,
            month=beijing_time.month,
            day=beijing_time.day,
            hour=beijing_time.hour,
            minute=beijing_time.minute,
            second=beijing_time.second,
            microsecond=beijing_time.microsecond,
        )
    
    @staticmethod
    def _get_po_info(po_type: str, po_id: int):
        """获取PO的详细信息"""
        from warehouse.models.pallet import Pallet
        from warehouse.models.packing_list import PackingList
        
        try:
            if po_type == 'pallet':
                po = Pallet.objects.get(id=po_id)
                po_display = str(po)
                delivery_type = po.delivery_type
                
                # 获取柜号
                container_number = None
                if po.container_number:
                    container_number = po.container_number.container_number
                
                # 获取仓点
                destination = po.destination if hasattr(po, 'destination') else None
                
                # 获取仓库（根据实际情况调整）
                warehouse = None
                
                return {
                    'po': po,
                    'po_display': po_display,
                    'delivery_type': delivery_type,
                    'container_number': container_number,
                    'destination': destination,
                    'warehouse': warehouse,
                }
            elif po_type == 'packing_list':
                po = PackingList.objects.get(id=po_id)
                po_display = str(po)
                delivery_type = po.delivery_type
                
                # 获取柜号
                container_number = None
                if po.container_number:
                    container_number = po.container_number.container_number
                
                # 获取仓点
                destination = po.destination if hasattr(po, 'destination') else None
                
                # 获取仓库
                warehouse = None
                
                return {
                    'po': po,
                    'po_display': po_display,
                    'delivery_type': delivery_type,
                    'container_number': container_number,
                    'destination': destination,
                    'warehouse': warehouse,
                }
        except Exception as e:
            print(f"获取PO信息失败: {e}")
            return None
    
    @staticmethod
    def log_bind(
        operator: User,
        po_type: str,
        po_id: int,
        shipment_batch_number: str,
        operation_button: str = None,
        note: str = None,
        container_number: str = None,
        destination: str = None,
        warehouse: str = None,
        shipment_type: str = 'actual',
        delivery_type: str = None,
        skip_get_po_info: bool = False,
    ):
        """
        记录绑定操作
        
        参数:
            operator: 操作用户
            po_type: PO类型 ('pallet' 或 'packing_list')
            po_id: PO ID 或 PO_ID 字段值
            shipment_batch_number: Shipment批次号
            operation_button: 操作按钮名称（可选）
            note: 备注（可选）
            container_number: 柜号（可选，不传则自动获取）
            destination: 仓点（可选，不传则自动获取）
            warehouse: 仓库（可选，不传则自动获取）
            delivery_type: 仓库类型（可选，不传则自动获取）
            skip_get_po_info: 是否跳过通过po_id查询PO信息（默认False）
            
        返回:
            ShipmentBindingLog 对象
        """
        po_display = f"{po_type}-{po_id}"
        final_delivery_type = delivery_type
        
        # 如果需要获取PO信息
        if not skip_get_po_info:
            # 获取PO信息
            po_info = ShipmentBindingLogger._get_po_info(po_type, po_id)
            
            # 如果没有获取到PO信息，也尝试记录
            if po_info:
                po_display = po_info['po_display']
                if delivery_type is None:
                    final_delivery_type = po_info['delivery_type']
                
                # 如果用户没有传这些信息，使用PO的信息
                if container_number is None:
                    container_number = po_info['container_number']
                if destination is None:
                    destination = po_info['destination']
                if warehouse is None:
                    warehouse = po_info['warehouse']
        
        # 记录日志
        log = ShipmentBindingLog.objects.create(
            operation_type='bind',
            po_type=po_type,
            po_id=po_id,
            po_display=po_display,
            shipment_batch_number=shipment_batch_number,
            delivery_type=final_delivery_type,
            container_number=container_number,
            destination=destination,
            warehouse=warehouse,
            operator=operator,
            operator_username=operator.username,
            operation_button=operation_button,
            operation_time_beijing=ShipmentBindingLogger.get_beijing_time(),
            note=note,
            shipment_type=shipment_type,
        )
        
        return log
    
    @staticmethod
    def log_unbind(
        operator: User,
        po_type: str,
        po_id: int,
        shipment_batch_number: str = None,
        operation_button: str = None,
        note: str = None,
        container_number: str = None,
        destination: str = None,
        warehouse: str = None,
        shipment_type: str = 'actual',
        delivery_type: str = None,
        skip_get_po_info: bool = False,
    ):
        """
        记录解绑操作
        
        参数:
            operator: 操作用户
            po_type: PO类型 ('pallet' 或 'packing_list')
            po_id: PO ID 或 PO_ID 字段值
            shipment_batch_number: Shipment批次号（可选，不传则自动获取）
            operation_button: 操作按钮名称（可选）
            note: 备注（可选）
            container_number: 柜号（可选，不传则自动获取）
            destination: 仓点（可选，不传则自动获取）
            warehouse: 仓库（可选，不传则自动获取）
            delivery_type: 仓库类型（可选，不传则自动获取）
            skip_get_po_info: 是否跳过通过po_id查询PO信息（默认False）
            
        返回:
            ShipmentBindingLog 对象
        """
        po_display = f"{po_type}-{po_id}"
        final_delivery_type = delivery_type
        
        # 如果需要获取PO信息
        if not skip_get_po_info:
            # 获取PO信息
            po_info = ShipmentBindingLogger._get_po_info(po_type, po_id)
            
            # 如果没有获取到PO信息，也尝试记录
            if po_info:
                po_display = po_info['po_display']
                if delivery_type is None:
                    final_delivery_type = po_info['delivery_type']
                
                # 如果用户没有传 shipment_batch_number，从PO获取
                if shipment_batch_number is None:
                    po = po_info['po']
                    if po.shipment_batch_number:
                        shipment_batch_number = po.shipment_batch_number.shipment_batch_number
                
                # 如果用户没有传这些信息，使用PO的信息
                if container_number is None:
                    container_number = po_info['container_number']
                if destination is None:
                    destination = po_info['destination']
                if warehouse is None:
                    warehouse = po_info['warehouse']
        
        # 记录日志
        log = ShipmentBindingLog.objects.create(
            operation_type='unbind',
            po_type=po_type,
            po_id=po_id,
            po_display=po_display,
            shipment_batch_number=shipment_batch_number,
            delivery_type=final_delivery_type,
            container_number=container_number,
            destination=destination,
            warehouse=warehouse,
            operator=operator,
            operator_username=operator.username,
            operation_button=operation_button,
            operation_time_beijing=ShipmentBindingLogger.get_beijing_time(),
            note=note,
            shipment_type=shipment_type,
        )
        
        return log
    
    @staticmethod
    async def log_shipment_operation(
        operator,
        pallet_ids,
        packinglist_ids,
        shipment_batch_number,
        operation_button,
        operation_type,
        shipment_type='actual'
    ):
        """
        记录 shipment 绑定/解绑操作的公共函数
        
        参数:
            operator: 操作用户
            pallet_ids: pallet ID 列表
            packinglist_ids: packinglist ID 列表
            shipment_batch_number: Shipment 批次号
            operation_button: 操作按钮名称
            operation_type: 操作类型 ('bind' 或 'unbind')
            shipment_type: shipment 类型 (默认 'actual')
        """
        from warehouse.models.pallet import Pallet
        from warehouse.models.packing_list import PackingList
        from asgiref.sync import sync_to_async
        
        # 处理 Pallet 日志
        if pallet_ids:
            @sync_to_async
            def process_pallets():
                pallets = list(
                    Pallet.objects.filter(id__in=pallet_ids).select_related('container_number')
                )
                
                # 按 container_number 和 PO_ID 分组
                pallet_log_groups = {}
                for pallet in pallets:
                    container_num = pallet.container_number.container_number if pallet.container_number else None
                    po_id = pallet.PO_ID
                    key = (container_num, po_id)
                    
                    if key not in pallet_log_groups:
                        pallet_log_groups[key] = {
                            'container_number': container_num,
                            'po_id': po_id,
                            'destination': pallet.destination,
                            'warehouse': pallet.location,
                            'delivery_type': pallet.delivery_type,
                        }
                
                # 记录 Pallet 日志
                for log_data in pallet_log_groups.values():
                    if operation_type == 'bind':
                        ShipmentBindingLogger.log_bind(
                            operator=operator,
                            po_type='pallet',
                            po_id=log_data['po_id'],
                            shipment_batch_number=shipment_batch_number,
                            operation_button=operation_button,
                            shipment_type=shipment_type,
                            container_number=log_data['container_number'],
                            destination=log_data['destination'],
                            warehouse=log_data['warehouse'],
                            delivery_type=log_data['delivery_type'],
                            skip_get_po_info=True,
                        )
                    else:
                        ShipmentBindingLogger.log_unbind(
                            operator=operator,
                            po_type='pallet',
                            po_id=log_data['po_id'],
                            shipment_batch_number=shipment_batch_number,
                            operation_button=operation_button,
                            shipment_type=shipment_type,
                            container_number=log_data['container_number'],
                            destination=log_data['destination'],
                            warehouse=log_data['warehouse'],
                            delivery_type=log_data['delivery_type'],
                            skip_get_po_info=True,
                        )
            
            await process_pallets()
        
        # 处理 PackingList 日志
        if packinglist_ids:
            @sync_to_async
            def process_packing_lists():
                packing_lists = list(
                    PackingList.objects.filter(id__in=packinglist_ids).select_related('container_number')
                )
                
                # 按 container_number 和 PO_ID 分组
                pl_log_groups = {}
                for pl in packing_lists:
                    container_num = pl.container_number.container_number if pl.container_number else None
                    po_id = pl.PO_ID
                    key = (container_num, po_id)
                    
                    if key not in pl_log_groups:
                        pl_log_groups[key] = {
                            'container_number': container_num,
                            'po_id': po_id,
                            'destination': pl.destination,
                            'warehouse': None,
                            'delivery_type': pl.delivery_type,
                        }
                
                # 记录 PackingList 日志
                for log_data in pl_log_groups.values():
                    if operation_type == 'bind':
                        ShipmentBindingLogger.log_bind(
                            operator=operator,
                            po_type='packing_list',
                            po_id=log_data['po_id'],
                            shipment_batch_number=shipment_batch_number,
                            operation_button=operation_button,
                            shipment_type=shipment_type,
                            container_number=log_data['container_number'],
                            destination=log_data['destination'],
                            warehouse=log_data['warehouse'],
                            delivery_type=log_data['delivery_type'],
                            skip_get_po_info=True,
                        )
                    else:
                        ShipmentBindingLogger.log_unbind(
                            operator=operator,
                            po_type='packing_list',
                            po_id=log_data['po_id'],
                            shipment_batch_number=shipment_batch_number,
                            operation_button=operation_button,
                            shipment_type=shipment_type,
                            container_number=log_data['container_number'],
                            destination=log_data['destination'],
                            warehouse=log_data['warehouse'],
                            delivery_type=log_data['delivery_type'],
                            skip_get_po_info=True,
                        )
            
            await process_packing_lists()
    
    @staticmethod
    async def log_shipment_operation_by_objects(
        operator,
        pallets,
        packing_lists,
        shipment_batch_number,
        operation_button,
        operation_type,
        shipment_type='actual'
    ):
        """
        记录 shipment 绑定/解绑操作的公共函数（接受对象列表）
        
        参数:
            operator: 操作用户
            pallets: pallet 对象列表
            packing_lists: packinglist 对象列表
            shipment_batch_number: Shipment 批次号
            operation_button: 操作按钮名称
            operation_type: 操作类型 ('bind' 或 'unbind')
            shipment_type: shipment 类型 (默认 'actual')
        """
        from asgiref.sync import sync_to_async
        
        # 处理 Pallet 日志
        if pallets:
            @sync_to_async
            def process_pallets():
                # 按 container_number 和 PO_ID 分组
                pallet_log_groups = {}
                for pallet in pallets:
                    container_num = pallet.container_number.container_number if pallet.container_number else None
                    po_id = pallet.PO_ID
                    key = (container_num, po_id)
                    
                    if key not in pallet_log_groups:
                        pallet_log_groups[key] = {
                            'container_number': container_num,
                            'po_id': po_id,
                            'destination': pallet.destination,
                            'warehouse': pallet.location,
                            'delivery_type': pallet.delivery_type,
                        }
                
                # 记录 Pallet 日志
                for log_data in pallet_log_groups.values():
                    if operation_type == 'bind':
                        ShipmentBindingLogger.log_bind(
                            operator=operator,
                            po_type='pallet',
                            po_id=log_data['po_id'],
                            shipment_batch_number=shipment_batch_number,
                            operation_button=operation_button,
                            shipment_type=shipment_type,
                            container_number=log_data['container_number'],
                            destination=log_data['destination'],
                            warehouse=log_data['warehouse'],
                            delivery_type=log_data['delivery_type'],
                            skip_get_po_info=True,
                        )
                    else:
                        ShipmentBindingLogger.log_unbind(
                            operator=operator,
                            po_type='pallet',
                            po_id=log_data['po_id'],
                            shipment_batch_number=shipment_batch_number,
                            operation_button=operation_button,
                            shipment_type=shipment_type,
                            container_number=log_data['container_number'],
                            destination=log_data['destination'],
                            warehouse=log_data['warehouse'],
                            delivery_type=log_data['delivery_type'],
                            skip_get_po_info=True,
                        )
            
            await process_pallets()
        
        # 处理 PackingList 日志
        if packing_lists:
            @sync_to_async
            def process_packing_lists():
                # 按 container_number 和 PO_ID 分组
                pl_log_groups = {}
                for pl in packing_lists:
                    container_num = pl.container_number.container_number if pl.container_number else None
                    po_id = pl.PO_ID
                    key = (container_num, po_id)
                    
                    if key not in pl_log_groups:
                        pl_log_groups[key] = {
                            'container_number': container_num,
                            'po_id': po_id,
                            'destination': pl.destination,
                            'warehouse': None,
                            'delivery_type': pl.delivery_type,
                        }
                
                # 记录 PackingList 日志
                for log_data in pl_log_groups.values():
                    if operation_type == 'bind':
                        ShipmentBindingLogger.log_bind(
                            operator=operator,
                            po_type='packing_list',
                            po_id=log_data['po_id'],
                            shipment_batch_number=shipment_batch_number,
                            operation_button=operation_button,
                            shipment_type=shipment_type,
                            container_number=log_data['container_number'],
                            destination=log_data['destination'],
                            warehouse=log_data['warehouse'],
                            delivery_type=log_data['delivery_type'],
                            skip_get_po_info=True,
                        )
                    else:
                        ShipmentBindingLogger.log_unbind(
                            operator=operator,
                            po_type='packing_list',
                            po_id=log_data['po_id'],
                            shipment_batch_number=shipment_batch_number,
                            operation_button=operation_button,
                            shipment_type=shipment_type,
                            container_number=log_data['container_number'],
                            destination=log_data['destination'],
                            warehouse=log_data['warehouse'],
                            delivery_type=log_data['delivery_type'],
                            skip_get_po_info=True,
                        )
            
            await process_packing_lists()
