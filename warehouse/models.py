from django.db import models

class Customer(models.Model):
    name = models.CharField(max_length=200)

    def __str__(self) -> str:
        return self.name
    
class ZEMWarehouse(models.Model):
    name = models.CharField(max_length=200)

    def __str__(self) -> str:
        return self.name

class Container(models.Model):
    id = models.BigAutoField(primary_key=True)
    order_type = models.CharField(max_length=255, null=True)
    container_id = models.CharField(max_length=255, null=True)
    customer_name = models.ForeignKey(Customer, null=True, on_delete=models.CASCADE)
    created_at = models.DateField()
    eta = models.DateField()
    shipping_line = models.CharField(max_length=255)
    container_type = models.CharField(max_length=255)
    departure_port = models.CharField(max_length=255)
    destination_port = models.CharField(max_length=255)
    warehouse = models.ForeignKey(ZEMWarehouse, null=True, on_delete=models.CASCADE)
    port_arrived_at = models.DateTimeField(null=True)
    port_picked_at = models.DateTimeField(null=True)
    warehouse_arrived_at = models.DateTimeField(null=True)
    unpacked_at = models.DateTimeField(null=True)
    pickup_method = models.CharField(max_length=255, null=True)
    pickup_id = models.CharField(max_length=255, null=True)
    pickup_scheduled_at = models.DateTimeField(null=True)
    pickup_appointment = models.DateField(null=True)
    palletized_at = models.DateTimeField(null=True)

    def __str__(self):
        return self.container_id


class PackingList(models.Model):
    id = models.BigAutoField(primary_key=True)
    container_id = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    product_name = models.CharField(max_length=255, null=True)
    delivery_method = models.CharField(max_length=255)
    shipping_mark = models.CharField(max_length=255, null=True)
    fba_id = models.CharField(max_length=255, null=True)
    destination = models.CharField(max_length=255)
    address = models.CharField(max_length=255, null=True)
    zipcode = models.CharField(max_length=20, null=True)
    ref_id = models.CharField(max_length=255, null=True)
    pcs = models.IntegerField()
    unit_weight_kg = models.FloatField(null=True)
    total_weight_kg = models.FloatField(null=True)
    unit_weight_lbs = models.FloatField(null=True)
    total_weight_lbs = models.FloatField(null=True)
    cbm = models.FloatField()
    n_pallet = models.IntegerField(null=True)

    def __str__(self):
        return f"{self.container_id} - {self.destination}"