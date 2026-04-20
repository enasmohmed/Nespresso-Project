from django import template
from django.template.loader import render_to_string
from django.utils.text import slugify
from django.utils.safestring import mark_safe
import json

register = template.Library()


@register.filter
def get_item(dictionary, key):
    """يرجع القيمة من dict باستخدام المفتاح، والخلايا الفارغة أو nan تظهر فارغة"""
    if isinstance(dictionary, dict):
        val = dictionary.get(key, "")
        if val is None:
            return ""
        s = str(val).strip()
        if s.lower() in ("nan", "nat", "none", "<nat>"):
            return ""
        return val
    return ""


@register.filter
def get_failed_shipments_count(kpi_table):
    """من قائمة صفوف KPI يرجّع قيمة Failed Shipments (Miss أو Miss (>24h)) من عمود 2025، أو 0"""
    if not kpi_table:
        return 0
    for row in kpi_table:
        if isinstance(row, dict) and row.get("KPI") in ("Miss", "Miss (>24h)"):
            val = row.get("2025") if isinstance(row.get("2025"), (int, float)) else row.get("2025")
            try:
                return int(float(val)) if val is not None else 0
            except (TypeError, ValueError):
                return 0
    return 0


@register.filter
def trim(value):
    if isinstance(value, str):
        return value.strip()
    return value


@register.simple_tag(takes_context=True, name="render_chart")
def render_chart(context, sub_id_or_title):
    """
    🎯 Render chart component - يبحث بالـ ID أولاً، ثم العنوان كـ fallback
    Usage:
      - {% render_chart sub.id %}  (باراميتر واحد - ID أو title)
    """
    try:
        # ✅ Debug: طباعة sub_id_or_title في بداية render_chart
        print(f"🔍 [render_chart START] sub_id_or_title: '{sub_id_or_title}'")
        cid = "chart-" + slugify(str(sub_id_or_title))

        # ✅ الحصول على البيانات من context
        render_context = {}

        def _get_sub_table_canvas_id(tab_dict, sub_id):
            """استخدام canvas_id من الجدول إن وُجد لضمان عدم تكرار الـ id بين شارتين."""
            if not tab_dict or not isinstance(tab_dict, dict):
                return None
            for st in (tab_dict.get("sub_tables") or []):
                if isinstance(st, dict) and str(st.get("id", "")).strip().lower() == str(sub_id).strip().lower():
                    return st.get("canvas_id") or None
            return None

        # ✅ محاولة الوصول إلى tab من context بطرق مختلفة
        tab = None
        try:
            if hasattr(context, "get"):
                tab = context.get("tab")
            if not tab and hasattr(context, "flatten"):
                flattened = context.flatten() or {}
                tab = flattened.get("tab")
            if not tab and hasattr(context, "__dict__"):
                tab = getattr(context, "tab", None)
        except Exception as e:
            print(f"⚠️ [render_chart] Error getting tab from context: {e}")

        # ✅ بناء render_context
        if hasattr(context, "flatten"):
            render_context = context.flatten() or {}
        else:
            try:
                render_context = dict(context)
            except Exception:
                render_context = {}

        # ✅ إضافة tab إذا لم يكن موجوداً
        if tab and "tab" not in render_context:
            render_context["tab"] = tab

        # ✅ استخدام canvas_id من الجدول إن وُجد (مهم لتاب Inbound: شارت 24h وشارت 18h)
        if tab:
            sub_canvas = _get_sub_table_canvas_id(tab, sub_id_or_title)
            if sub_canvas:
                cid = str(sub_canvas).strip()

        render_context = dict(render_context)
        render_context["canvas_id"] = cid

        print(f"🔍 [render_chart] render_context keys: {list(render_context.keys())}")
        print(f"🔍 [render_chart] tab exists: {'tab' in render_context}")

        def collect_datasets_from_sub_table(ctx):
            """
            ✅ البحث بالـ ID أولاً، ثم العنوان كـ fallback
            """
            if not isinstance(ctx, dict):
                print(f"⚠️ [collect_datasets] ctx is not dict: {type(ctx)}")
                return []

            # ✅ البحث عن tab في context بطرق مختلفة
            tab = None
            if "tab" in ctx:
                tab = ctx["tab"]
            elif hasattr(context, "get") and context.get("tab"):
                tab = context.get("tab")
            elif hasattr(context, "tab"):
                tab = context.tab

            if not tab or not isinstance(tab, dict):
                print(
                    f"⚠️ [collect_datasets] tab not found or not dict. ctx keys: {list(ctx.keys())}"
                )
                return []

            if "sub_tables" not in tab or not isinstance(tab["sub_tables"], list):
                print(
                    f"⚠️ [collect_datasets] sub_tables not found in tab. tab keys: {list(tab.keys())}"
                )
                return []

            sub_id_or_title_str = str(sub_id_or_title).strip()
            sub_id_or_title_lower = sub_id_or_title_str.lower()

            # شارت KPI Actual & Contractual: ليس sub_table — يجب ألا يقع في fallback إلى tab.chart_data (48h)
            if sub_id_or_title_lower == "b2b-outbound-sheet-kpi-chart":
                ck = tab.get("chart_data_sheet_kpi")
                if ck and isinstance(ck, list) and len(ck) > 0:
                    print(
                        f"✅ [render_chart] collect_datasets: chart_data_sheet_kpi for '{sub_id_or_title}' ({len(ck)} datasets)"
                    )
                    return ck
                return []

            # ✅ البحث عن sub_table المطابق بالـ ID أولاً
            for sub_table in tab["sub_tables"]:
                if not isinstance(sub_table, dict):
                    continue

                # ✅ محاولة المطابقة بالـ ID
                sub_table_id = sub_table.get("id", "")
                if (
                    sub_table_id
                    and str(sub_table_id).strip().lower() == sub_id_or_title_lower
                ):
                    sub_chart_data = sub_table.get("chart_data", [])
                    if (
                        sub_chart_data
                        and isinstance(sub_chart_data, list)
                        and len(sub_chart_data) > 0
                    ):
                        print(
                            f"✅ [render_chart] Found {len(sub_chart_data)} datasets in sub_table (by ID): '{sub_id_or_title}'"
                        )
                        print(
                            f"🔍 [render_chart] Chart data names: {[ds.get('name', 'N/A') for ds in sub_chart_data]}"
                        )
                        return sub_chart_data
                    else:
                        print(
                            f"⚠️ [render_chart] No chart_data in sub_table (by ID): '{sub_id_or_title}' (chart_data: {sub_chart_data})"
                        )
                        # ✅ لا نرجع [] هنا، نترك البحث يستمر بالـ title

                # ✅ Fallback: المطابقة بالعنوان
                sub_title_in_table = str(sub_table.get("title", "")).strip().lower()

                # ✅ تطبيع الأسماء للمقارنة (إزالة رموز خاصة)
                def normalize_name(name):
                    return (
                        name.replace("—", "-")
                        .replace("–", "-")
                        .replace("  ", " ")
                        .strip()
                    )

                sub_id_or_title_normalized = normalize_name(sub_id_or_title_lower)
                sub_title_in_table_normalized = normalize_name(sub_title_in_table)

                # ✅ مطابقة مباشرة أو بعد التطبيع
                if (
                    sub_title_in_table == sub_id_or_title_lower
                    or sub_title_in_table_normalized == sub_id_or_title_normalized
                ):
                    sub_chart_data = sub_table.get("chart_data", [])
                    if (
                        sub_chart_data
                        and isinstance(sub_chart_data, list)
                        and len(sub_chart_data) > 0
                    ):
                        print(
                            f"✅ [render_chart] Found {len(sub_chart_data)} datasets in sub_table (by title): '{sub_id_or_title}'"
                        )
                        print(
                            f"🔍 [render_chart] Chart data names: {[ds.get('name', 'N/A') for ds in sub_chart_data]}"
                        )
                        return sub_chart_data
                    else:
                        print(
                            f"⚠️ [render_chart] No chart_data in sub_table (by title): '{sub_id_or_title}' (chart_data: {sub_chart_data})"
                        )
                        # ✅ لا نرجع [] هنا، نترك البحث يستمر

            print(
                f"⚠️ [render_chart] No matching sub_table found for: '{sub_id_or_title}'"
            )
            # ✅ Fallback: البحث في tab.chart_data إذا لم نجد sub_table مطابق
            if "tab" in ctx and isinstance(ctx["tab"], dict):
                tab_chart_data = ctx["tab"].get("chart_data", [])
                tab_name = str(ctx["tab"].get("name", "")).strip().lower()
                sub_id_or_title_lower = str(sub_id_or_title).strip().lower()

                if tab_chart_data and isinstance(tab_chart_data, list):
                    # ✅ إذا كان sub_id_or_title يطابق tab.name تقريباً (يحتوي على نفس الكلمات الأساسية)، استخدم كل tab.chart_data
                    is_seaport_or_airport = (
                        "seaport" in tab_name or "airport" in tab_name
                    ) and (
                        "seaport" in sub_id_or_title_lower
                        or "airport" in sub_id_or_title_lower
                    )

                    # ✅ تطبيع الأسماء للمقارنة
                    def normalize_for_comparison(name):
                        return (
                            name.replace("—", "-")
                            .replace("–", "-")
                            .replace("  ", " ")
                            .strip()
                            .lower()
                        )

                    tab_name_normalized = (
                        normalize_for_comparison(tab_name) if tab_name else ""
                    )
                    sub_id_or_title_normalized = normalize_for_comparison(
                        sub_id_or_title_lower
                    )

                    # ✅ إذا كان sub_id_or_title يطابق tab.name، استخدم كل tab.chart_data مباشرة
                    if (
                        tab_name_normalized
                        and sub_id_or_title_normalized == tab_name_normalized
                    ) or (is_seaport_or_airport and len(tab_chart_data) > 2):
                        print(
                            f"✅ [render_chart] Using all tab.chart_data (tab.name match or seaport/airport with multiple datasets): {len(tab_chart_data)} datasets"
                        )
                        return tab_chart_data

                    # ✅ فلترة البيانات حسب related_table إذا كان موجوداً
                    filtered_tab_data = []
                    for dataset in tab_chart_data:
                        related_table = (
                            str(dataset.get("related_table", "")).strip().lower()
                        )
                        # ✅ مطابقة flexible: إذا كان related_table يحتوي على sub_id_or_title أو العكس
                        if related_table and (
                            related_table == sub_id_or_title_lower
                            or sub_id_or_title_lower in related_table
                            or related_table in sub_id_or_title_lower
                        ):
                            filtered_tab_data.append(dataset)

                    if filtered_tab_data:
                        print(
                            f"✅ [render_chart] Found {len(filtered_tab_data)} datasets in tab.chart_data (fallback)"
                        )
                        return filtered_tab_data
                    else:
                        # ✅ إذا لم نجد مطابقة، نستخدم كل tab.chart_data
                        print(
                            f"✅ [render_chart] Using all tab.chart_data as fallback: {len(tab_chart_data)} datasets"
                        )
                        return tab_chart_data
            return []

        # ✅ البحث فقط في sub_table المطابق - لا fallback
        datasets = collect_datasets_from_sub_table(render_context)

        # ✅ Debug printing
        print(
            f"🔍 [render_chart] sub_id_or_title: '{sub_id_or_title}', datasets count: {len(datasets)}"
        )
        if datasets:
            print(
                f"🔍 [render_chart] datasets names: {[ds.get('name', 'N/A') for ds in datasets]}"
            )
        else:
            # ✅ Debug: طباعة جميع sub_tables المتاحة
            if "tab" in render_context and isinstance(render_context["tab"], dict):
                tab = render_context["tab"]
                if "sub_tables" in tab and isinstance(tab["sub_tables"], list):
                    print(f"🔍 [render_chart] Available sub_tables:")
                    for idx, sub in enumerate(tab["sub_tables"]):
                        if isinstance(sub, dict):
                            sub_id = sub.get("id", "N/A")
                            sub_title = sub.get("title", "N/A")
                            chart_data_count = len(sub.get("chart_data", []))
                            print(
                                f"  [{idx}] id: '{sub_id}', title: '{sub_title}', chart_data: {chart_data_count} datasets"
                            )

        # ✅ Fallback: إذا لم نجد datasets في sub_table، جرب tab.chart_data أو tab.chart_data_pods
        if not datasets or len(datasets) == 0:
            if "tab" in render_context and isinstance(render_context["tab"], dict):
                tab_dict = render_context["tab"]
                # شارت PODs (عمودين): عند طلب "B2B Outbound PODs" نستخدم chart_data_pods
                if "pods" in str(sub_id_or_title).strip().lower() and tab_dict.get("chart_data_pods"):
                    datasets = tab_dict.get("chart_data_pods", [])
                    if datasets:
                        print(f"✅ [render_chart] Using tab.chart_data_pods: {len(datasets)} datasets")
                if not datasets:
                    sd = str(sub_id_or_title).strip().lower()
                    if sd == "b2b-outbound-sheet-kpi-chart":
                        ck = tab_dict.get("chart_data_sheet_kpi")
                        if ck:
                            datasets = ck
                            print(
                                f"✅ [render_chart] Using tab.chart_data_sheet_kpi: {len(datasets)} datasets"
                            )
                if not datasets:
                    tab_chart_data = tab_dict.get("chart_data", [])
                    print(
                        f"🔍 [render_chart FALLBACK] tab.chart_data exists: {tab_chart_data is not None}, length: {len(tab_chart_data) if tab_chart_data else 0}"
                    )
                    print(
                        f"🔍 [render_chart FALLBACK] sub_id_or_title: '{sub_id_or_title}'"
                    )
                    print(
                        f"🔍 [render_chart FALLBACK] tab.name: '{render_context['tab'].get('name', 'N/A')}'"
                    )
                    if tab_chart_data and isinstance(tab_chart_data, list):
                        # ✅ إذا كان sub_id_or_title يطابق tab.name، استخدم كل tab.chart_data مباشرة
                        tab_name = (
                            str(render_context["tab"].get("name", "")).strip().lower()
                        )
                        sub_id_or_title_lower = str(sub_id_or_title).strip().lower()

                        # ✅ تطبيع الأسماء للمقارنة (إزالة رموز خاصة ومسافات زائدة)
                        def normalize_for_comparison(name):
                            return (
                                name.replace("—", "-")
                                .replace("–", "-")
                                .replace("  ", " ")
                                .strip()
                                .lower()
                            )

                        tab_name_normalized = (
                            normalize_for_comparison(tab_name) if tab_name else ""
                        )
                        sub_id_or_title_normalized = normalize_for_comparison(
                            sub_id_or_title_lower
                        )

                        # ✅ إذا كان sub_id_or_title يطابق tab.name، استخدم كل tab.chart_data
                        # ✅ أيضاً: إذا كان tab.name يحتوي على "seaport" أو "airport" و sub_id_or_title يطابق، استخدم كل tab.chart_data
                        is_seaport_or_airport = (
                            "seaport" in tab_name_normalized
                            or "airport" in tab_name_normalized
                        )
                        is_name_match = (
                            tab_name_normalized
                            and sub_id_or_title_normalized == tab_name_normalized
                        )

                        if is_name_match or (
                            is_seaport_or_airport
                            and sub_id_or_title_normalized in tab_name_normalized
                        ):
                            datasets = tab_chart_data
                            print(
                                f"✅ [render_chart] Using all tab.chart_data (tab.name match): {len(datasets)} datasets"
                            )
                            print(
                                f"🔍 [render_chart] tab_name_normalized: '{tab_name_normalized}', sub_id_or_title_normalized: '{sub_id_or_title_normalized}'"
                            )
                            print(
                                f"🔍 [render_chart] Chart data names: {[ds.get('name', 'N/A') for ds in datasets]}"
                            )
                        else:
                            # ✅ فلترة البيانات حسب related_table إذا كان موجوداً
                            filtered_tab_data = []
                            for dataset in tab_chart_data:
                                related_table = (
                                    str(dataset.get("related_table", "")).strip().lower()
                                )
                                if related_table and (
                                    related_table == sub_id_or_title_lower
                                    or sub_id_or_title_lower in related_table
                                    or related_table in sub_id_or_title_lower
                                ):
                                    filtered_tab_data.append(dataset)

                            if filtered_tab_data:
                                datasets = filtered_tab_data
                                print(
                                    f"✅ [render_chart] Using filtered tab.chart_data: {len(datasets)} datasets"
                                )
                            else:
                                # ✅ إذا كان tab.chart_data يحتوي على datasets متعددة (مثل Seaport/Airport)، استخدمه كله
                                if len(tab_chart_data) > 2:
                                    datasets = tab_chart_data
                                    print(
                                        f"✅ [render_chart] Using all tab.chart_data (multiple datasets detected): {len(datasets)} datasets"
                                    )
                                else:
                                    datasets = tab_chart_data
                                    print(
                                        f"✅ [render_chart] Using all tab.chart_data: {len(datasets)} datasets"
                                    )

        # ✅ استخدام datasets مباشرة من sub_table - لا حاجة للفلترة
        render_context["chart_data"] = datasets

        chart_context = render_context.get("chart")
        if not isinstance(chart_context, dict):
            chart_context = {}
        chart_context.setdefault("canvas_id", cid)
        chart_context.setdefault("title", str(sub_id_or_title))
        render_context["chart"] = chart_context

        final_chart_data = render_context.get("chart_data", [])
        print(
            f"✅ [render_chart] Rendering chart with {len(final_chart_data)} datasets"
        )
        print(
            f"🔍 [render_chart] Final chart_data related_tables: {[ds.get('related_table', 'N/A') for ds in final_chart_data]}"
        )

        # ✅ إذا لم يكن هناك datasets، لا نعرض الشارت
        if not final_chart_data or len(final_chart_data) == 0:
            print(
                f"⚠️ [render_chart] No datasets found for '{sub_id_or_title}', skipping chart"
            )
            return mark_safe("")  # ✅ إرجاع string فارغ بدلاً من HTML

        # ✅ تحويل chart_data إلى JSON string لاستخدامه في JavaScript
        chart_data_json = json.dumps(final_chart_data, default=str)
        render_context["chart_data_json"] = chart_data_json
        print(
            f"🔍 [render_chart] chart_data_json length: {len(chart_data_json)} characters"
        )
        print(f"🔍 [render_chart] chart_data_json preview: {chart_data_json[:200]}...")

        html = render_to_string(
            "components/charts/chartjs/components/chart-excel-sheet.html",
            render_context,
        )
        print(f"✅ [render_chart] HTML length: {len(html)} characters")
        # ✅ استخدام mark_safe لضمان عرض HTML بشكل صحيح
        return mark_safe(html)
    except Exception as e:
        import traceback

        print(f"❌ [render_chart] Error: {e}")
        print(traceback.format_exc())
        return f'<div class="alert alert-warning">⚠️ Error loading chart: {str(e)}</div>'



@register.filter
def split(value, key):
    return value.split(key)

@register.filter
def strip_spaces(value):
    """Remove leading and trailing spaces"""
    if value is None:
        return ''
    return str(value).strip()


@register.filter
def normalize(value):
    if not value:
        return ""
    return value.strip().lower()