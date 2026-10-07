import ast
import uuid
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from django.template.loader import render_to_string
from django.contrib.auth.models import AnonymousUser
from django.test import SimpleTestCase
from bs4 import BeautifulSoup

from warehouse.models.container import Container
from warehouse.models.pallet import Pallet


class InventoryPoIdentifiersTests(SimpleTestCase):
    def adjust(self, identifiers, count=1):
        """Run the actual adjustment function with database writes mocked."""
        source = (Path(__file__).parent / "views/post_port/warehouse/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        handler = next(n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef) and n.name == "handle_update_po_post")
        adjust = next(n for n in ast.walk(handler) if isinstance(n, ast.FunctionDef) and n.name == "adjust_pallets")
        adjust.decorator_list = []
        container = Container(id=1, container_number="TEST")
        old = Pallet(id=1, container_number=container, PO_ID="PO1", shipping_mark="PALLET-MARK",
                     fba_id="PALLET-FBA", ref_id="PALLET-REF")
        manager = MagicMock()
        manager.select_related.return_value.filter.return_value.order_by.return_value = [old]
        factory = MagicMock(side_effect=Pallet)
        factory.objects = manager
        namespace = dict(
            Pallet=factory, transaction=SimpleNamespace(atomic=nullcontext), uuid=uuid, seed=0,
            plt_ids=[1], n_pallet_new=count, pallet_identifiers=identifiers,
            destination_new="SBD1", address_new="", zipcode_new="", delivery_method_new="卡车派送",
            delivery_type_new="public", location_new="LA", note_new="",
            self=SimpleNamespace(_is_hold_delivery_method=lambda value: False),
            # PackingList form data must never supply card identifiers.
            shipping_mark_new=["OTHER-MARK"] * 8,
            fba_id_new=["OTHER-FBA"] * 8, ref_id_new=["OTHER-REF"] * 8,
        )
        exec(compile(ast.Module(body=[adjust], type_ignores=[]), "inventory-adjustment", "exec"), namespace)
        return namespace["adjust_pallets"]()

    def test_explicit_card_values_apply_to_kept_and_added_cards(self):
        rows = self.adjust({"shipping_mark": " CARD ", "fba_id": " FBA ", "ref_id": " REF "}, 2)
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual((row.shipping_mark, row.fba_id, row.ref_id), ("CARD", "FBA", "REF"))

    def test_old_page_preserves_identifiers_even_when_packinglist_differs(self):
        for row in self.adjust(dict.fromkeys(("shipping_mark", "fba_id", "ref_id")), 2):
            self.assertEqual((row.shipping_mark, row.fba_id, row.ref_id),
                             ("PALLET-MARK", "PALLET-FBA", "PALLET-REF"))

    def test_explicit_blank_clears_only_card_fields(self):
        row = self.adjust({"shipping_mark": "", "fba_id": " ", "ref_id": ""})[0]
        self.assertEqual((row.shipping_mark, row.fba_id, row.ref_id), ("", "", ""))

    def test_page_has_separate_card_and_packinglist_inputs(self):
        html = render_to_string("post_port/inventory/02_inventory_po_update.html", {
            "user": AnonymousUser(),
            "pallet": {"shipping_mark": "CARD", "fba_id": "CARD-FBA", "ref_id": "CARD-REF"},
            "packing_list": [{"id": 2, "shipping_mark": "PL", "fba_id": "PL-FBA", "ref_id": None}],
        })
        soup = BeautifulSoup(html, "html.parser")
        for name, value in {"pallet_shipping_mark": "CARD", "pallet_fba_id": "CARD-FBA",
                            "pallet_ref_id": "CARD-REF", "shipping_mark_new": "PL",
                            "fba_id_new": "PL-FBA", "ref_id_new": ""}.items():
            self.assertEqual(soup.find("input", attrs={"name": name})["value"], value)
