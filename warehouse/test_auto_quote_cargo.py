import copy
import json
import uuid
from unittest.mock import patch

from django.contrib.auth.models import User
from django.http import Http404
from django.test import RequestFactory, TransactionTestCase

from warehouse.models.auto_quote import AutoQuoteAddress, AutoQuoteBatch, AutoQuoteCargoGroup, AutoQuoteItem
from warehouse.test_auto_quote import parameters
from warehouse.utils.auto_quote import batch_summary, create_batch
from warehouse.utils.auto_quote_cargo import cargo_options, delete_cargo_group, save_cargo_group, start_cargo_group
from warehouse.utils.quote_analysis_storage import get_profile
from warehouse.views.post_port.auto_quote import auto_quote_get, auto_quote_post


class CargoGroupTests(TransactionTestCase):
    def setUp(self):
        self.user = User.objects.create_user("cargo-owner")
        self.other = User.objects.create_user("cargo-other")
        self.staff = User.objects.create_user("cargo-staff", is_staff=True)
        self.date = parameters()["pickupDate"]
        self.warehouses = [{"warehouse": "NJ current", "city": "Newark", "state": "NJ", "postCode": "07101", "detailAddress": "1 Test Street"}]
        self.profiles = []
        for index in range(3):
            value = copy.deepcopy(parameters())
            value["items"][0]["weight"] = 500 + index * 200
            value["declaredValue"] = 100 + index * 100
            value["needLiftgate"] = index == 1
            value["destinationType"] = 2 if index == 1 else 1
            if index == 2:
                value["items"].append(dict(value["items"][0], length=24, width=20, weight=50))
            profile = get_profile(value)
            self.profiles.append(profile)
            AutoQuoteBatch.objects.create(group="NJ", parameters=value, profile=profile, operator=self.user,
                                          submission_id=uuid.uuid4(), status="completed")
        AutoQuoteAddress.objects.bulk_create([AutoQuoteAddress(group="NJ", city="Boston", state="MA", zipcode="02108",
            address=f"{index} Test St", fingerprint=str(index)) for index in range(2)])
        self.group = save_cargo_group(self.user, None, "Daily cargo", [profile.pk for profile in self.profiles])

    def start(self, token=None, user=None):
        return start_cargo_group(user or self.user, self.group.pk, "NJ", self.date, token or uuid.uuid4(), self.warehouses)

    def request(self, step, **values):
        request = RequestFactory().post("/post_nsop/", {"step": step, **values})
        request.user = self.user
        return request

    def test_one_full_address_batch_per_aq_with_original_conditions(self):
        batches, reused = self.start()
        self.assertFalse(reused)
        self.assertEqual([batch.profile_id for batch in batches], [profile.pk for profile in self.profiles])
        self.assertEqual([batch.items.count() for batch in batches], [2, 2, 2])
        self.assertEqual([batch.cargo_group_position for batch in batches], [1, 2, 3])
        self.assertEqual([len(batch.parameters["items"]) for batch in batches], [1, 1, 2])
        self.assertEqual([batch.parameters["declaredValue"] for batch in batches], ["100", "200", "300"])
        self.assertTrue(batches[1].parameters["needLiftgate"])
        self.assertEqual(batches[1].parameters["destinationType"], 2)
        self.assertTrue(all(batch.parameters["pickupDate"] == self.date for batch in batches))
        self.assertTrue(all(batch.parameters["originWarehouse"] == "NJ current" for batch in batches))
        self.assertEqual([list(batch.items.values_list("address_snapshot", flat=True)) for batch in batches][0],
                         list(batches[2].items.values_list("address_snapshot", flat=True)))
        self.assertEqual(batch_summary(batches[0])["cargo_group_name"], "Daily cargo")

    def test_network_retry_and_other_tab_reuse_then_new_run_after_completion(self):
        token = uuid.uuid4()
        first, _ = self.start(token)
        second, reused = self.start(token)
        self.assertTrue(reused)
        self.assertEqual([batch.pk for batch in first], [batch.pk for batch in second])
        third, reused = self.start()
        self.assertTrue(reused)
        self.assertEqual([batch.pk for batch in first], [batch.pk for batch in third])
        AutoQuoteBatch.objects.filter(pk__in=[batch.pk for batch in first]).update(status="completed")
        for batch in first:
            batch.items.update(status="success")
        again, reused = self.start(token)
        self.assertTrue(reused)
        self.assertEqual([batch.pk for batch in first], [batch.pk for batch in again])
        fresh, reused = self.start()
        self.assertFalse(reused)
        self.assertTrue(set(batch.pk for batch in first).isdisjoint(batch.pk for batch in fresh))

    def test_snapshot_survives_group_edit_delete_and_address_changes(self):
        batches, _ = self.start()
        old_parameters = copy.deepcopy(batches[0].parameters)
        save_cargo_group(self.user, self.group.pk, "Renamed group", [self.profiles[2].pk])
        AutoQuoteAddress.objects.update(address="Changed later")
        delete_cargo_group(self.user, self.group.pk)
        batches[0].refresh_from_db()
        self.assertIsNone(batches[0].cargo_group_id)
        self.assertEqual(batches[0].cargo_group_name, "Daily cargo")
        self.assertEqual(batches[0].parameters, old_parameters)
        self.assertNotEqual(batches[0].items.first().address_snapshot["address"], "Changed later")
        self.assertEqual(batches[0].items.count(), 2)

    def test_member_order_can_change_without_changing_existing_tasks(self):
        batches, _ = self.start()
        ids = [self.profiles[2].pk, self.profiles[0].pk]
        save_cargo_group(self.user, self.group.pk, self.group.name, ids)
        self.assertEqual(list(self.group.members.values_list("profile_id", flat=True)), ids)
        next_batches, reused = self.start()
        self.assertFalse(reused)
        self.assertEqual([batch.profile_id for batch in next_batches], ids)
        batches[0].refresh_from_db()
        self.assertEqual(batches[0].cargo_group_position, 1)

    def test_invalid_member_creates_no_batches_or_items(self):
        warehouses = self.warehouses
        count = AutoQuoteBatch.objects.count()
        with self.assertRaisesRegex(ValueError, "AQ"):
            start_cargo_group(self.user, self.group.pk, "NJ", self.date, uuid.uuid4(), [])
        self.assertEqual(AutoQuoteBatch.objects.count(), count)
        self.assertFalse(AutoQuoteItem.objects.exists())
        self.assertEqual(self.warehouses, warehouses)

    def test_later_create_failure_rolls_back_earlier_batches(self):
        count = AutoQuoteBatch.objects.count()
        calls = 0
        def fail_later(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ValueError("Second member could not be saved")
            return create_batch(*args, **kwargs)
        with patch("warehouse.utils.auto_quote_cargo.create_batch", side_effect=fail_later), self.assertRaisesRegex(ValueError, "Second member"):
            self.start()
        self.assertEqual(AutoQuoteBatch.objects.count(), count)
        self.assertFalse(AutoQuoteItem.objects.exists())

    def test_visibility_and_mutations_are_owner_or_staff_only(self):
        self.assertEqual(cargo_options(self.other)["cargo_groups"], [])
        self.assertEqual(cargo_options(self.other)["profiles"], [])
        self.assertEqual(len(cargo_options(self.staff)["cargo_groups"]), 1)
        for action in (
            lambda: self.start(user=self.other),
            lambda: delete_cargo_group(self.other, self.group.pk),
            lambda: save_cargo_group(self.other, self.group.pk, "Stolen", [self.profiles[0].pk]),
        ):
            with self.assertRaises(Http404):
                action()
        with self.assertRaisesRegex(ValueError, "无权"):
            save_cargo_group(self.other, None, "Other group", [self.profiles[0].pk])

    def test_groups_survive_removing_original_history(self):
        AutoQuoteBatch.objects.all().delete()
        options = cargo_options(self.user)
        self.assertEqual(set(profile["id"] for profile in options["profiles"]), set(profile.pk for profile in self.profiles))
        self.assertEqual(len(self.start()[0]), 3)

    def test_invalid_members_and_duplicate_names_leave_group_unchanged(self):
        ids = list(self.group.members.values_list("profile_id", flat=True))
        for values in ([], [ids[0], ids[0]], [True], [ids[0], False], [1.5], ["invalid"], [999999]):
            with self.subTest(values=values), self.assertRaises(ValueError):
                save_cargo_group(self.user, self.group.pk, "Invalid", values)
            self.group.refresh_from_db()
            self.assertEqual(self.group.name, "Daily cargo")
            self.assertEqual(list(self.group.members.values_list("profile_id", flat=True)), ids)
        with self.assertRaisesRegex(ValueError, "同名"):
            save_cargo_group(self.user, None, self.group.name, ids)

    def test_group_endpoints_start_once_and_list_snapshot_metadata(self):
        request = RequestFactory().get("/post_nsop/", {"kind": "cargo_groups"})
        request.user = self.user
        self.assertEqual(len(json.loads(auto_quote_get(request).content)["cargo_groups"]), 1)
        request = self.request("auto_quote_cargo_group_start", cargo_group=self.group.pk, group="NJ",
                               pickup_date=self.date, submission_id=str(uuid.uuid4()))
        with patch("warehouse.views.post_port.auto_quote.gateway_config"), \
             patch("warehouse.views.post_port.auto_quote.SystemParameter.get_zem_warehouse_addresses", return_value=self.warehouses), \
             patch("warehouse.views.post_port.auto_quote.schedule_worker") as schedule:
            response = auto_quote_post(request)
        self.assertEqual(response.status_code, 200, response.content)
        schedule.assert_called_once()
        body = json.loads(response.content)
        self.assertEqual(len(body["batches"]), 3)
        self.assertEqual(body["batches"][1]["cargo_group_position"], 2)
        self.assertEqual(body["batches"][1]["cargo_group_name"], self.group.name)

    def test_retry_preserves_group_label_without_becoming_part_of_original_submission(self):
        batches, _ = self.start()
        parent = batches[0]
        parent.items.update(status="failed")
        parent.status = "completed"
        parent.save(update_fields=["status"])
        retry = create_batch(parent.group, parent.parameters, self.user.pk, uuid.uuid4(), parent=parent)
        self.assertEqual(retry.parent_id, parent.pk)
        self.assertEqual(retry.cargo_group_name, parent.cargo_group_name)
        self.assertEqual(retry.cargo_group_position, parent.cargo_group_position)
        self.assertIsNone(retry.cargo_submission_id)
