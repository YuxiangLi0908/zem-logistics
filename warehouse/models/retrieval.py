from datetime import datetime, timedelta

from django.db import models
from simple_history.models import HistoricalRecords


class Retrieval(models.Model):
    retrieval_id = models.CharField(max_length=255, null=True)
    shipping_order_number = models.CharField(max_length=255, null=True, blank=True)
    master_bill_of_lading = models.CharField(max_length=255, null=True, blank=True)
    retrive_by_zem = models.BooleanField(default=True, blank=True)
    retrieval_carrier = models.CharField(max_length=100, null=True, blank=True)
    arrival_location = models.CharField(max_length=100, null=True, blank=True, verbose_name="到仓位置")
    unpacking_status = models.CharField(
        max_length=10,
        choices=[
            ("0", "未拆柜"),
            ("1", "已拆柜"),
            ("2", "拆柜中"),
        ],
        default="0",
        verbose_name="拆柜状态",
    )
    origin_port = models.CharField(max_length=255, null=True, blank=True)
    destination_port = models.CharField(max_length=255, null=True, blank=True)
    shipping_line = models.CharField(max_length=255, null=True, blank=True)
    retrieval_destination_precise = models.CharField(
        max_length=200, null=True, blank=True
    )
    assigned_by_appt = models.BooleanField(default=False, blank=True)
    retrieval_destination_area = models.CharField(max_length=20, null=True, blank=True)
    scheduled_at = models.DateTimeField(null=True, blank=True)
    target_retrieval_timestamp = models.DateTimeField(null=True, blank=True)
    target_retrieval_timestamp_lower = models.DateTimeField(null=True, blank=True)
    actual_retrieval_timestamp = models.DateTimeField(null=True, blank=True)
    trucking_fee = models.FloatField(null=True, blank=True)
    chassis_fee = models.FloatField(null=True, blank=True)
    is_trucking_fee_paid = models.BooleanField(default=False, blank=True)
    is_chassis_fee_paid = models.BooleanField(default=False, blank=True)
    trucking_fee_paid_at = models.FloatField(null=True, blank=True)
    chassis_fee_paid_at = models.FloatField(null=True, blank=True)
    note = models.TextField(null=True, blank=True)
    arrive_at_destination = models.BooleanField(default=False, blank=True)
    arrive_at = models.DateTimeField(null=True, blank=True)
    empty_returned = models.BooleanField(default=False, blank=True)
    empty_returned_at = models.DateTimeField(null=True, blank=True)
    # temporary fields
    temp_t49_lfd = models.DateTimeField(null=True, blank=True)
    temp_t49_available_for_pickup = models.BooleanField(default=False, blank=True)
    temp_t49_pod_arrive_at = models.DateTimeField(null=True, blank=True)
    temp_t49_pod_discharge_at = models.DateTimeField(null=True, blank=True)
    temp_t49_hold_status = models.BooleanField(default=False, blank=True)
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["retrieval_id"]),
            models.Index(fields=["target_retrieval_timestamp"]),
        ]

    def __str__(self) -> str:
        return self.retrieval_id

    @property
    def pickup_schedule_status(self) -> str:
        today = datetime.now().date()
        if self.target_retrieval_timestamp.date() <= today:
            return "past_due"
        elif self.target_retrieval_timestamp.date() <= today + timedelta(days=1):
            return "need_attention"
        else:
            return "on_time"

    @property
    def lfd_status(self) -> str:
        today = datetime.now().today()
        if self.temp_t49_lfd.date() <= today:
            return "past_due"
        elif self.temp_t49_lfd.date() <= today + timedelta(days=3):
            return "need_attention"
        else:
            return "on_time"

    @property
    def arrive_at_warehouse_status(self) -> str:
        today = datetime.now().date()
        if self.actual_retrieval_timestamp.date() <= today:
            return "past_due"
        else:
            return "on_time"

    @property
    def offload_status(self) -> str:
        today = datetime.now().date()
        if self.arrive_at.date() < today + timedelta(days=-1):
            return "past_due"
        elif self.arrive_at.date() < today + timedelta(days=-2):
            return "need_attention"
        else:
            return "on_time"
