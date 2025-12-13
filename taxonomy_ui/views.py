# taxonomy_ui/views.py

import os
import io
import sys
import subprocess
from collections import defaultdict

import pandas as pd
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from .models import PartMaster
from taxonomy_ui.stage2_adapter import run_stage2_from_django

# Path to background_stage1.py
STAGE1_SCRIPT = os.path.join(settings.BASE_DIR, "background_stage1.py")


# ----------------------------------------------------------
# Column list for checkbox UI
# ----------------------------------------------------------
COLUMN_CHOICES = [
    "part_number", "updated_at", "stock_qty", "vendor_code", "abc_class",
    "commodity_code", "utilization_score", "material_group", "risk_rating",
    "cost", "purchase_uom", "notes", "description_clean", "drawing_no",
    "is_standard_part", "order_uom", "spec_grade", "spec_finish", "material",
    "dimensions", "last_modified", "description", "category_master",
    "analysis_comment", "created_date", "plant", "currency", "flag",
    "checkout_status", "remarks", "approval_status", "revision_no",
    "material_type", "avg_lead_time_days", "spec_weight", "no", "cad_type",
    "storage_location", "quantity", "criticality_index", "category_raw",
    "engineer_name", "active_flag", "file_size_mb", "valuation_type",
    "spec_tolerance", "movement_frequency", "order_date", "delivery_date",
    "pdf_page", "date", "due_date", "file_name", "sources", "lifecycle_state",
    "vendor_name", "cad_file", "source_system", "source_file",
]


# ----------------------------------------------------------
# Home
# ----------------------------------------------------------
def home(request):
    return render(request, "taxonomy_ui/home.html")


# ----------------------------------------------------------
# Stage 1 DB parts view
# ----------------------------------------------------------
def part_list(request):
    parts = PartMaster.objects.all().values()
    df = pd.DataFrame(parts)

    if df.empty:
        rows = []
        columns = []
    else:
        if "sources" in df.columns:
            df["sources"] = df["sources"].astype(str).str.replace(",", ",\n")

        rows = df.to_dict(orient="records")
        columns = list(df.columns)

    return render(
        request,
        "taxonomy_ui/parts_list.html",
        {
            "columns": columns,
            "rows": rows,
        },
    )


# ----------------------------------------------------------
# UPLOAD + PROCESS (Stage 2)
# ----------------------------------------------------------
def upload_and_process(request):
    """
    Upload user file(s), run Stage 2, show preview, and enable downloads.
    """

    # Always define variables first
    df = None
    has_df = False
    download_link = None
    output_filename = None
    all_columns = COLUMN_CHOICES
    error = None
    saved_count = 0  # NEW: Track saved records

    # -----------------------
    # GET REQUEST
    # -----------------------
    if request.method == "GET":
        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "df": df,
                "has_df": has_df,
                "download_link": download_link,
                "output_filename": output_filename,
                "all_columns": all_columns,
                "error": error,
                "saved_count": saved_count,  # NEW
            },
        )

    # -----------------------
    # POST REQUEST
    # -----------------------
    uploaded_files = request.FILES.getlist("files")

    if not uploaded_files:
        error = "No files were submitted!"
        return render(
            request,
            "taxonomy_ui/upload.html",
            {
                "df": df,
                "has_df": has_df,
                "download_link": download_link,
                "output_filename": output_filename,
                "all_columns": all_columns,
                "error": error,
                "saved_count": saved_count,
            },
        )

    try:
        # Run Stage 2
        output_bytes, filename = run_stage2_from_django(uploaded_files)

        # Save file
        output_dir = os.path.join(settings.MEDIA_ROOT, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)

        with open(output_path, "wb") as f:
            f.write(output_bytes)

        download_link = f"/download-full/{filename}/"
        output_filename = filename

        # Load preview DataFrame
        df = pd.read_excel(io.BytesIO(output_bytes))

        # ----------------------------------------------------------------------
        # FIX BLOCK: Prevent NaTType utcoffset crash in Django templates
        # ----------------------------------------------------------------------
        df = df.where(pd.notnull(df), None)

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(
                    lambda x: x.to_pydatetime() if isinstance(x, pd.Timestamp) else None
                )
        # ----------------------------------------------------------------------

        # NEW: SAVE TO DATABASE
        # ----------------------------------------------------------------------
        from django.utils import timezone
        
        for _, row in df.iterrows():
            try:
                # Map DataFrame columns to model fields
                PartMaster.objects.create(
                    part_number=str(row.get('part_number', '')),
                    updated_at=row.get('updated_at') or timezone.now(),
                    dimensions=str(row.get('dimensions', '')) if pd.notna(row.get('dimensions')) else None,
                    description=str(row.get('description', '')) if pd.notna(row.get('description')) else None,
                    cost=str(row.get('cost', '')) if pd.notna(row.get('cost')) else None,
                    material=str(row.get('material', '')) if pd.notna(row.get('material')) else None,
                    vendor_name=str(row.get('vendor_name', '')) if pd.notna(row.get('vendor_name')) else None,
                    currency=str(row.get('currency', '')) if pd.notna(row.get('currency')) else None,
                    category_raw=str(row.get('category_raw', '')) if pd.notna(row.get('category_raw')) else None,
                    category_master=str(row.get('category_master', '')) if pd.notna(row.get('category_master')) else None,
                    source_system=str(row.get('source_system', '')) if pd.notna(row.get('source_system')) else None,
                    source_file=filename,  # Track which file this came from
                )
                saved_count += 1
            except Exception as row_error:
                print(f"Error saving row: {row_error}")
                continue
        
        print(f"✅ Saved {saved_count} records to database")
        # ----------------------------------------------------------------------

        if "sources" in df.columns:
            df["sources"] = df["sources"].astype(str).str.replace(",", ",\n")

        has_df = not df.empty

    except Exception as e:
        error = str(e)
        df = None
        has_df = False
        import traceback
        print(f"❌ Error in upload_and_process: {traceback.format_exc()}")

    # Final render
    return render(
        request,
        "taxonomy_ui/upload.html",
        {
            "df": df,
            "has_df": has_df,
            "download_link": download_link,
            "output_filename": output_filename,
            "all_columns": all_columns,
            "error": error,
            "saved_count": saved_count,  # NEW
        },
    )


# ----------------------------------------------------------
# FULL OUTPUT DOWNLOAD
# ----------------------------------------------------------
def download_full_output(request, filename):
    output_path = os.path.join(settings.MEDIA_ROOT, "output", filename)

    if not os.path.exists(output_path):
        return HttpResponse("File not found.", status=404)

    with open(output_path, "rb") as f:
        data = f.read()

    response = HttpResponse(
        data,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


# ----------------------------------------------------------
# SELECTED COLUMNS DOWNLOAD
# ----------------------------------------------------------
def download_selected_columns(request):
    if request.method != "POST":
        return HttpResponse("Invalid method", status=405)

    output_filename = request.POST.get("output_filename")
    selected_columns = request.POST.getlist("selected_columns")

    if not output_filename:
        return HttpResponse("Missing output file reference.", status=400)

    output_path = os.path.join(settings.MEDIA_ROOT, "output", output_filename)

    if not os.path.exists(output_path):
        return HttpResponse("Output file not found.", status=404)

    df = pd.read_excel(output_path)

    if selected_columns:
        df = df[[c for c in selected_columns if c in df.columns]]

    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    response = HttpResponse(
        buffer.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = (
        f'attachment; filename="selected_{output_filename}"'
    )
    return response


# ----------------------------------------------------------
# REFRESH STAGE 1
# ----------------------------------------------------------
def run_stage1_refresh(request):
    if request.method != "POST":
        return JsonResponse(
            {"status": "error", "message": "Invalid method"},
            status=405,
        )

    try:
        subprocess.Popen([sys.executable, STAGE1_SCRIPT])
        return JsonResponse({"status": "ok", "message": "Stage 1 started"})
    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": str(e)},
            status=500,
        )
