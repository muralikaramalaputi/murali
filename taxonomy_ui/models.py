# from django.db import models

# class PartMaster(models.Model):
#     part_number = models.CharField(max_length=100, unique=True)

#     updated_at = models.DateTimeField(
#         auto_now_add=True   # 👈 IMPORTANT
#     )

#     dimensions = models.CharField(max_length=255, blank=True, null=True)
#     description = models.TextField(blank=True, null=True)
#     cost = models.CharField(max_length=100, blank=True, null=True)
#     material = models.CharField(max_length=255, blank=True, null=True)
#     vendor_name = models.CharField(max_length=255, blank=True, null=True)
#     currency = models.CharField(max_length=50, blank=True, null=True)
#     category_raw = models.CharField(max_length=255, blank=True, null=True)
#     category_master = models.CharField(max_length=255, blank=True, null=True)
#     source_system = models.CharField(max_length=50, blank=True, null=True)
#     source_file = models.CharField(max_length=255, blank=True, null=True)

#     class Meta:
#         db_table = "part_master"


from django.db import models

class PartMaster(models.Model):

    # --- Excel / DB Columns (32) ---
    portfolio = models.TextField(blank=True, null=True)
    profit_center_key = models.TextField(blank=True, null=True)
    commodity_level_0 = models.TextField(blank=True, null=True)
    commodity_level_1 = models.TextField(blank=True, null=True)
    commodity_level_2 = models.TextField(blank=True, null=True)

    material_no = models.TextField(blank=True, null=True)   # NOT unique now

    material_description = models.TextField(blank=True, null=True)
    gr_quantity = models.TextField(blank=True, null=True)

    vendor_name = models.TextField(blank=True, null=True)
    vendor_no = models.TextField(blank=True, null=True)
    parent_vendor = models.TextField(blank=True, null=True)
    vendor_region = models.TextField(blank=True, null=True)
    vendor_country = models.TextField(blank=True, null=True)

    internal_external = models.TextField(blank=True, null=True)
    bcc = models.TextField(blank=True, null=True)

    plant_name = models.TextField(blank=True, null=True)
    plant_country = models.TextField(blank=True, null=True)
    plant_region = models.TextField(blank=True, null=True)

    deflation_strategy = models.TextField(blank=True, null=True)
    development_plan = models.TextField(blank=True, null=True)
    mpa = models.TextField(blank=True, null=True)
    productivity = models.TextField(blank=True, null=True)
    rebate = models.TextField(blank=True, null=True)

    single_source = models.TextField(blank=True, null=True)
    sole_source = models.TextField(blank=True, null=True)
    strategic_status = models.TextField(blank=True, null=True)
    payment_terms = models.TextField(blank=True, null=True)
    stock = models.TextField(blank=True, null=True)

    gr_amount_aop_fx = models.TextField(blank=True, null=True)
    gr_amount_hana_fx = models.TextField(blank=True, null=True)
    gr_month = models.TextField(blank=True, null=True)
    gr_year = models.TextField(blank=True, null=True)

    # --- Metadata ---
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "material_master"   # ✅ IMPORTANT
        