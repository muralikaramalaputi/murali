from django.db import migrations

class Migration(migrations.Migration):

    dependencies = [
        ('taxonomy_ui', '0004_alter_partmaster_options_alter_partmaster_id_alter_partmaster_updated_at'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS material_master (
                id BIGSERIAL PRIMARY KEY,

                portfolio TEXT,
                profit_center_key TEXT,
                commodity_level_0 TEXT,
                commodity_level_1 TEXT,
                commodity_level_2 TEXT,

                material_no TEXT,
                material_description TEXT,
                gr_quantity TEXT,

                vendor_name TEXT,
                vendor_no TEXT,
                parent_vendor TEXT,
                vendor_region TEXT,
                vendor_country TEXT,

                internal_external TEXT,
                bcc TEXT,

                plant_name TEXT,
                plant_country TEXT,
                plant_region TEXT,

                deflation_strategy TEXT,
                development_plan TEXT,
                mpa TEXT,
                productivity TEXT,
                rebate TEXT,

                single_source TEXT,
                sole_source TEXT,
                strategic_status TEXT,
                payment_terms TEXT,
                stock TEXT,

                gr_amount_aop_fx TEXT,
                gr_amount_hana_fx TEXT,
                gr_month TEXT,
                gr_year TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            reverse_sql="DROP TABLE IF EXISTS material_master;"
        )
    ]
