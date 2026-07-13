from django.contrib import admin
from simple_history.admin import SimpleHistoryAdmin

from warehouse.models.clearance import Clearance
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.fleet import Fleet
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.invoice import (
    Invoice,
    InvoiceItem,
    InvoiceStatement,
    InvoiceStatus,
)
from warehouse.models.invoicev2 import (
    Invoicev2,
    InvoiceItemv2,
    InvoiceStatusv2,
)
from warehouse.models.invoice_details import (
    InvoiceDelivery,
    InvoicePreport,
    InvoiceWarehouse,
)
from warehouse.models.offload import Offload
from warehouse.models.offload_status import AbnormalOffloadStatus
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.pallet_destroyed import PalletDestroyed
from warehouse.models.pallet_exception import PalletException
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.quotation_master import QuotationMaster
from warehouse.models.quote import Quote
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.shipment_status import ShipmentStatus
from warehouse.models.power_automate_webhook_raw import PowerAutomateWebhookRaw
from warehouse.models.terminal49_webhook_raw import T49Raw
from warehouse.models.transaction import Transaction
from warehouse.models.transfer_location import TransferLocation
from warehouse.models.vessel import Vessel
from warehouse.models.maersk_price_rate import MaerskPriceRate
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.shipment_bindlog import ShipmentBindingLog
from warehouse.models.system_parameter import SystemParameter
from warehouse.models.dropship_cargo import DropshipCargo
from warehouse.models.dropship_inventory import DropshipInventory
from warehouse.models.dropship_shipment import DropshipShipment
from warehouse.models.dropship_shipment_detail import DropshipShipmentDetail

# Register your models here.
admin.site.register(Clearance, SimpleHistoryAdmin)
admin.site.register(Container, SimpleHistoryAdmin)
admin.site.register(Customer, SimpleHistoryAdmin)
admin.site.register(Offload, SimpleHistoryAdmin)
admin.site.register(AbnormalOffloadStatus, SimpleHistoryAdmin)
admin.site.register(Order, SimpleHistoryAdmin)
admin.site.register(PackingList, SimpleHistoryAdmin)
admin.site.register(Retrieval, SimpleHistoryAdmin)
admin.site.register(Shipment, SimpleHistoryAdmin)
admin.site.register(ZemWarehouse, SimpleHistoryAdmin)
admin.site.register(Pallet, SimpleHistoryAdmin)
admin.site.register(Quote, SimpleHistoryAdmin)
admin.site.register(Invoice, SimpleHistoryAdmin)
admin.site.register(InvoiceStatus, SimpleHistoryAdmin)
admin.site.register(InvoiceItem, SimpleHistoryAdmin)
admin.site.register(InvoiceStatement, SimpleHistoryAdmin)
admin.site.register(PowerAutomateWebhookRaw)
admin.site.register(T49Raw, SimpleHistoryAdmin)
admin.site.register(Vessel, SimpleHistoryAdmin)
admin.site.register(Fleet, SimpleHistoryAdmin)
admin.site.register(FleetShipmentPallet, SimpleHistoryAdmin)
admin.site.register(ShipmentStatus, SimpleHistoryAdmin)
admin.site.register(PoCheckEtaSeven, SimpleHistoryAdmin)
admin.site.register(InvoicePreport, SimpleHistoryAdmin)
admin.site.register(InvoiceWarehouse, SimpleHistoryAdmin)
admin.site.register(InvoiceDelivery, SimpleHistoryAdmin)
admin.site.register(TransferLocation, SimpleHistoryAdmin)
admin.site.register(QuotationMaster)
admin.site.register(FeeDetail)
admin.site.register(Transaction)
admin.site.register(PalletDestroyed, SimpleHistoryAdmin)
admin.site.register(PalletException, SimpleHistoryAdmin)
admin.site.register(Invoicev2, SimpleHistoryAdmin)
admin.site.register(InvoiceItemv2, SimpleHistoryAdmin)
admin.site.register(InvoiceStatusv2, SimpleHistoryAdmin)
admin.site.register(MaerskPriceRate, SimpleHistoryAdmin)
admin.site.register(SystemParameter, SimpleHistoryAdmin)
admin.site.register(DropshipCargo, SimpleHistoryAdmin)
admin.site.register(DropshipInventory, SimpleHistoryAdmin)
admin.site.register(DropshipShipment, SimpleHistoryAdmin)
admin.site.register(DropshipShipmentDetail, SimpleHistoryAdmin)


class ShipmentBindingLogAdmin(SimpleHistoryAdmin):
    list_display = ['operator_username', 'operation_type', 'po_type', 'po_display', 
                   'shipment_batch_number', 'delivery_type', 'container_number', 
                   'operation_button', 'operation_time_beijing']
    list_filter = ['operation_type', 'po_type', 'delivery_type', 'operation_time_beijing', 'operator']
    search_fields = ['operator_username', 'po_display', 'shipment_batch_number', 'container_number', 'note']
    readonly_fields = ['operation_time_utc', 'operation_time_beijing', 'operator_username']
    date_hierarchy = 'operation_time_beijing'
    
    def has_add_permission(self, request):
        return False  # 不允许手动添加
    
    def has_change_permission(self, request, obj=None):
        return False  # 不允许修改
    
    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser  # 只有超级用户可以删除


admin.site.register(ShipmentBindingLog, ShipmentBindingLogAdmin)
