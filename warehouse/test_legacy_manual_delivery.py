from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bs4 import BeautifulSoup
from django.template.loader import render_to_string
from django.test import RequestFactory, SimpleTestCase

from warehouse.views.receivable_accounting import ReceivableAccounting


MODULE = "warehouse.views.receivable_accounting"


class LegacyManualDeliveryTests(SimpleTestCase):
    def setUp(self):
        self.view = ReceivableAccounting()
        self.order = SimpleNamespace(
            vessel_id=SimpleNamespace(vessel_etd=datetime(2024, 9, 20, tzinfo=timezone.utc)),
            container_number=SimpleNamespace(container_type="40HQ", manually_order_type=""),
            order_type="转运组合", warehouse=None,
            customer_name=SimpleNamespace(zem_name="test"),
        )
        self.error = f"找不到生效日期在{self.order.vessel_id.vessel_etd}之前的receivable报价表"
        self.groups = [
            {"PO_ID": "PO1", "destination": "ONT8", "total_pallets": 2,
             "total_cbm": 4, "total_weight_lbs": 100, "delivery_method": "卡车",
             "shipping_marks": "MARK1"},
            {"PO_ID": "PO2", "destination": "LAX9", "total_pallets": 3,
             "total_cbm": 0, "delivery_method": "暂扣", "shipping_marks": "MARK2"},
        ]

    def test_only_old_missing_quotes_qualify(self):
        eligible = self.view._allows_legacy_manual_delivery
        self.assertTrue(eligible(self.order, None, self.error))
        self.assertFalse(eligible(self.order, object(), None))
        self.assertFalse(eligible(self.order, None, "查询报价表时发生错误: connection failed"))
        self.order.vessel_id.vessel_etd = datetime(2025, 1, 1, tzinfo=timezone.utc)
        error = f"找不到生效日期在{self.order.vessel_id.vessel_etd}之前的receivable报价表"
        self.assertFalse(eligible(self.order, None, error))
        self.order.vessel_id = None
        self.assertFalse(eligible(self.order, None, self.error))

    def test_manual_rows_include_every_destination_without_inferred_type_or_price(self):
        result = self.view._legacy_manual_delivery_items(self.groups)
        self.assertEqual([r['destination'] for r in result['normal_items']], ['ONT8', 'LAX9'])
        self.assertEqual(result['combina_groups'], [])
        for row in result['normal_items']:
            self.assertEqual(row['delivery_category'], '')
            self.assertIsNone(row['rate'])
            self.assertFalse(row['is_hold'])

    def test_open_and_render_both_editors_without_any_quote_rules(self):
        for delivery_type in ('public', 'other'):
            with self.subTest(delivery_type=delivery_type), \
                patch(f'{MODULE}.Order') as orders, \
                patch(f'{MODULE}.Invoicev2') as invoices, \
                patch(f'{MODULE}.InvoiceStatusv2') as statuses, \
                patch(f'{MODULE}.Pallet') as pallets, \
                patch(f'{MODULE}.PackingList') as packing, \
                patch(f'{MODULE}.FeeDetail') as fees, \
                patch.object(self.view, '_get_quotation_for_order', return_value=(None, self.error)), \
                patch.object(self.view, '_get_pallet_groups_by_po', return_value=(self.groups, [], {})), \
                patch.object(self.view, '_get_existing_invoice_items', return_value={}), \
                patch.object(self.view, '_get_existing_activation_items', return_value=[]), \
                patch.object(self.view, '_determine_is_combina') as determine, \
                patch.object(self.view, '_process_unbilled_items') as automatic:
                orders.objects.select_related.return_value.get.return_value = self.order
                invoices.objects.filter.return_value.exclude.return_value.exists.return_value = False
                invoices.objects.get.return_value = SimpleNamespace(
                    id=1, invoice_number='INV1', receivable_is_locked=False)
                statuses.objects.get_or_create.return_value = (MagicMock(), False)
                pallets.objects.filter.return_value.exclude.return_value.exclude.return_value.values.return_value.annotate.return_value.order_by.return_value = []
                packing.objects.filter.return_value.aggregate.return_value = {'total_cbm': 4}
                request = RequestFactory().get('/receivable_accounting/', {
                    'container_number': 'OLD1', 'invoice_id': '1', 'delivery_type': delivery_type})
                request.user = SimpleNamespace(username='tester')
                template, context = self.view.handle_container_delivery_post(request)
                self.assertTrue(context['manual_delivery'])
                self.assertEqual(len(context['normal_items']), 2)
                self.assertEqual(context['normal_items'][0]['cbm_ratio'], 1)
                self.assertNotIn('error_messages', context)
                fees.objects.get.assert_not_called()
                determine.assert_not_called()
                automatic.assert_not_called()
                html = render_to_string(template, {**context, 'user': None})
                self.assertIn('历史柜手动录费', html)
                self.assertIn('value="combine"', html)
                self.assertIn('ONT8', html)
                self.assertIn('LAX9', html)
                soup = BeautifulSoup(html, 'html.parser')
                for select in soup.select('.delivery-type-select'):
                    self.assertIsNotNone(select.find('option', value='combine'))
                    self.assertIsNotNone(select.find('option', value='amazon'))
                if delivery_type == 'public':
                    regular = render_to_string(template, {**context, 'manual_delivery': False, 'user': None})
                    regular_soup = BeautifulSoup(regular, 'html.parser')
                    for select in regular_soup.select('.delivery-type-select'):
                        self.assertIsNone(select.find('option', value='combine'))

    def test_saved_combine_stays_an_editable_row(self):
        existing = MagicMock(id=7, PO_ID='PO1', delivery_type='combine', rate=75,
                             amount=150, qty=2, cbm=4, weight=100, cbm_ratio=1)
        for delivery_type, key in [('public', 'PO1'), ('other', 'PO1-MARK1')]:
            result = self.view._legacy_manual_delivery_items(self.groups, {key: existing}, delivery_type)
            self.assertEqual(result['combina_groups'], [])
            self.assertEqual(result['normal_items'][0]['delivery_category'], 'combine')
            self.assertEqual(result['normal_items'][0]['rate'], 75)
            self.assertEqual(result['normal_items'][0]['amount'], 150)

    def test_save_combine_preserves_user_rate_without_region_lookup(self):
        data = [{'po_id': 'PO1', 'delivery_category': 'combine', 'rate': '75',
                 'pallets': '2', 'amount': '150', 'surcharges': '0', 'destination': 'ONT8'}]
        with patch(f'{MODULE}.Order') as orders, \
             patch(f'{MODULE}.InvoiceItemv2') as items, \
             patch.object(self.view, '_get_quotation_for_order', return_value=(None, self.error)), \
             patch.object(self.view, '_get_fee_details') as fees:
            orders.objects.select_related.return_value.get.return_value = self.order
            result = self.view.batch_save_delivery_item(
                SimpleNamespace(container_number='OLD1'), object(), data, 'delivery_public', {}, 'tester')
            fees.assert_not_called()
            self.assertEqual(items.return_value.rate, 75)
            self.assertEqual(items.return_value.amount, 150)
            items.return_value.save.assert_called_once()
            self.assertIn('success_messages', result)

    def test_new_containers_still_require_quote_on_combine_save(self):
        self.order.vessel_id.vessel_etd = datetime(2025, 1, 1, tzinfo=timezone.utc)
        self.order.retrieval_id = SimpleNamespace(retrieval_destination_area='LA')
        error = f"找不到生效日期在{self.order.vessel_id.vessel_etd}之前的receivable报价表"
        with patch(f'{MODULE}.Order') as orders, \
             patch.object(self.view, '_get_quotation_for_order', return_value=(None, error)), \
             patch.object(self.view, '_get_fee_details', return_value={'error_messages': error}):
            orders.objects.select_related.return_value.get.return_value = self.order
            with self.assertRaisesMessage(ValueError, error):
                self.view._search_region([], self.order.container_number)
