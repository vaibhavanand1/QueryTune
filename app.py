import streamlit as st
# import pyperclip
import sqlglot
from sqlglot import exp
import platform
import streamlit.components.v1 as components

# ----------------------------------------------------------
PARTITION_COLS = {
    "sp_refurb.dashboard_jobrow": "created_at",
    "sp_refurb.dashboard_jobrow_sub_job_row": "id",
    "sp_refurb.dashboard_subjobrow": "created_at",
    "sp_refurb.dashboard_issue": "created_at",
    "sp_refurb.dashboard_car": "created_at",
    "sp_refurb.dashboard_refurbcar": "created_at",
    "sp_refurb.dashboard_refurbcartorefurbcar": "created_at",
    "sp_refurb.dashboard_workshop": "created_at",
    "sp_refurb.dashboard_carpart": "created_at",
    "sp_refurb.dashboard_nodetopodvaluemapping": "created_at",
    "sp_refurb.dashboard_refurbcarworkshopusermapping": "created_at",
    "sp_refurb.dashboard_city": "created_at",
    "sp_refurb.inspection_inspectionrequest": "created_at",
    "sp_refurb.dashboard_joblist": "created_at",
    "sp_refurb.dashboard_joblistreportmapping": "id",
    "sp_refurb.dashboard_jobrowuserfixmapping": "created_at",
    "sp_refurb.auth_user": "id",
    "sp_refurb.dashboard_jobrowfixmetadata": "created_at",
    "sp_refurb.dashboard_fix": "created_at",
    "sp_refurb.dashboard_joblistbudgetapprovalstatus": "created_at",
    "sp_refurb.dashboard_microcohort": "created_at",
    "sp_web.listing_lead": "time",
    "sp_web.listing_leadprofile": "id",
    "sp_web.make_variant": "created_on",
    "sp_web.make_model": "created_on",
    "sp_web.make_make": "created_on",
    "sp_refurb.dashboard_jobrowtojobrowmapping": "created_at",
    "sp_refurb.dashboard_microjobrow": "created_at",
    "sp_refurb.dashboard_jobcard": "created_at",
    "sp_refurb.dashboard_microjobrowuserfixmapping": "created_at",
    "sp_catalogue_inventory.spinny_variant": "created_at",
    "sp_catalogue_inventory.variant_to_mmvy_mapping": "created_at",
    "sp_catalogue_inventory.category_to_node_mapping": "created_at",
    "sp_catalogue_inventory.product_consumption_log": "created_at",
    "sp_catalogue_inventory.product": "created_at",
    "sp_refurb.dashboard_joblistmetadata": "created_at",
    "sp_catalogue_inventory.request_item": "created_at",
    "sp_catalogue_inventory.reserved_sku_for_request_item": "created_at",
    "sp_catalogue_inventory.request_to_node_item": "created_at",
    "sp_catalogue_inventory.request": "created_at",
    "sp_catalogue_inventory.issue_request_to_mmvy_value_mapping": "created_at",
    "sp_catalogue_inventory.request_node_item_activity_log": "created_at",
    "sp_refurb.dashboard_workshopbaytype": "created_at",
    "sp_refurb.dashboard_joblistusermapping": "created_at",
    "sp_refurb.dashboard_microjobrowmetadata": "created_at",
    "sp_web.spinny_auth_user": "id",
    "sp_web.address_city": "id",
    "sp_catalogue_inventory.replaced_request_item_mapping": "created_at",
    "sp_car_part.sp_car_part_partcost": "id",
    "sp_car_part.sp_car_part_fix": "id",
    "sp_car_part.sp_car_part_fixtype": "id",
    "sp_car_part.sp_car_part_node": "id",
    "sp_refurb.dashboard_fieldtype": "created_at",
    "sp_catalogue_inventory.request_search_data": "created_at",
    "sp_refurb.dashboard_journey": "created_at",
    "sp_refurb.dashboard_jobrowmetadata": "created_at",
    "sp_refurb.dashboard_fix_fix_category": "id",
    "sp_refurb.dashboard_fixcategory": "created_at",
    "sp_refurb.dashboard_subjobrow_fixes": "id",
    "sp_refurb.dashboard_subjobrowimage": "created_at",
    "sp_refurb.dashboard_requestslugmapping": "id",
    "sp_refurb.file_filemodelrelation": "created_at",
    "sp_refurb.file_fileupload": "created_at",
    "sp_refurb.dashboard_microjobrowfixmetadata": "created_at",
    "sp_refurb.dashboard_microjobrowcostmetadata": "created_at",
    "sp_refurb.dashboard_joblisttojoblistmapping": "created_at",
    "sp_refurb.dashboard_issuetocomplexitymapping": "created_at",
    "sp_refurb.dashboard_complexity": "created_at",
    "sp_Refurb.dashboard_entityuserrolemapping": "created_at",
    "sp_Refurb.auth_user": "id",
    "sp_Refurb.dashboard_workshop": "created_at",
    "sp_Refurb.attendance_dailyattendance": "created_at",
    "sp_Refurb.dashboard_usermetric": "created_at",
    "sp_Refurb.dashboard_usercomplexityconfig": "created_at",
    "sp_Refurb.dashboard_workshoptechnicianmapping": "created_at",
    "sp_Refurb.dashboard_workshopbaytype": "created_at",
    "sp_Refurb.dashboard_usereligibleworktype": "created_at",
    "sp_Refurb.dashboard_technicianteam": "created_at",
    "sp_Refurb.dashboard_grouptechnicianmapping": "created_at",
    "sp_Refurb.dashboard_group": "created_at",
    "sp_Refurb.dashboard_technicianassignment": "created_at",
    "sp_refurb.dashboard_issuecomplexityactiveversion": "created_at",
    "sp_refurb.dashboard_workshopbaytypetojobrowmapping": "created_at",
    "sp_refurb.dashboard_refurbcargroupassignment": "created_at",
    "sp_refurb.dashboard_group": "created_at",
    "sp_refurb.dashboard_autoassignmentinfo": "created_at",
    "sp_web.listing_leadinspectionrel": "created_on",
    "sp_web.operations_fieldtask": "id",
    "sp_refurb.dashboard_upcomingcar": "created_at",
    "sp_catalogue_inventory.category": "created_at",
    "sp_catalogue_inventory.product_sku_location": "created_at",
    "sp_catalogue_inventory.product_sku": "created_at",
    "sp_catalogue_inventory.product_entity": "created_at",
    "sp_catalogue_inventory.location": "created_at",
    "sp_web.shifting_car_services_cardelivery": "created_at",
    "sp_web.car_movement_deliverytorefub": "created_at",
    "sp_web.listing_procurredcar": "time_create",
    "sp_web.payments_paymentorder": "created_time",
    "sp_catalogue_inventory.product_sku_reservation_log": "created_at",
    "sp_web.listing_inspectiondataitem": "id",
    "sp_web.listing_approval": "created_time",
    "sp_web.address_hub": "id",
    "sp_web.address_locality": "id",
    "sp_car_inventory.sp_car_listing_carlisting": "created_at",
    "sp_car_inventory.sp_car_listing_carlistingtransition": "created_at",
    "sp_web.listing_listing": "id",
    "sp_web.buy_lead_carpurchase": "created_time",
    "sp_web.status_status": "id",
    "sp_supply.leads_leaddataitem": "created_at",
    "sp_supply.leads_leaddatatype": "created_at",
    "sp_catalogue_inventory.request_to_request_item": "created_at",
    "sp_catalogue_inventory.brand": "created_at",
    "sp_catalogue_inventory.brand_type": "created_at",
    "sp_catalogue_inventory.inventory_entity": "created_at",
    "sp_refurb.dashboard_invoice": "created_at",
    "sp_refurb.dashboard_invoicemetadata": "created_at",
    "sp_refurb.outbound_outboundjourney": "created_at",
    "sp_refurb.outbound_outboundrefurbcar": "created_at",
    "sp_web.pricing_pricevalue": "created_on",
    "sp_web.buy_lead_testdrive": "created_time",
    "sp_web.visits_visit": "created_on",
    "sp_web.visits_visittype": "id",
    "sp_report_data.sp_report_data_issueitem": "id",
    "sp_report_data.sp_report_data_fixitem_issue_item": "id",
    "sp_report_data.sp_report_data_fixitem": "id",
    "sp_car_part.sp_car_part_treeedge": "id",
    "sp_web.taggit_taggeditem": "id",
    "sp_web.django_content_type": "id",
    "sp_web.taggit_tag": "id",
    "sp_web.listing_servicehistorycheck": "created_time",
    "sp_web.pricing_pricevaluecomment": "id",
    "sp_web.pricing_listingpricecommenttype": "id",
    "sp_refurb.inspection_inspectiontype": "created_at",
    "sp_refurb.dashboard_microcarpart": "created_at",
    "sp_report_data.sp_report_data_issueseverityforprocurement": "id",
    "sp_auction.auction_profiles": "id",
    "sp_auction.listing_auctions": "created_at",
    "sp_auction.auction_types": "id",
    "sp_auction.price_values": "created_at",
    "sp_auction.auction_winners": "created_at",
    "sp_auction.dealers": "created_at",
    "sp_auction.auth_user": "created_at",
    "sp_auction.dealer_listing_bids": "created_at",
    "sp_auction.auction_statuses": "id",
    "sp_auction.hub_car_entries": "created_at",
    "sp_auction.hub_car_exits": "created_at",
    "sp_web.listing_leadactivitylog": "created_at",
    "sp_report_data.sp_report_analytics_issuecategoryforprocurement": "id",
    "sp_web.listing_milestonehistory": "created_at",
    "sp_web.workflow_usertask": "created_time",
    "sp_web.listing_leadtaskdetails": "created_at",
    "sp_car_part.sp_node_structure_modification_userrequest": "id",
    "sp_car_part.sp_report_template_templatetreeversion": "id",
    "sp_car_part.sp_report_template_templatejson": "id",
    "sp_car_part.sp_report_template_templateprocessflow": "id",
    "sp_web.file_fileset": "id",
    "sp_web.file_filesetrelationship": "id",
    "sp_web.file_fileupload": "slug",
    "sp_web.file_filelabel": "id",
    "sp_refurb.evidence_evidencetomediatypemapping": "created_at",
    "sp_refurb.evidence_evidence": "created_at",
    "sp_refurb.evidence_evidencerequest": "created_at",
    "sp_refurb.file_filecategory": "created_at",
    "sp_refurb.dashboard_entityuserrolemapping": "created_at",
    "sp_refurb.dashboard_technicianteam": "created_at",
    "sp_refurb.dashboard_workshoptechnicianmapping": "created_at",
    "sp_refurb.dashboard_usercomplexityconfig": "created_at",
    "sp_refurb.dashboard_usereligibleworktype": "created_at",
    "sp_catalogue_mongo.car_specs": "created_at",
    "sp_web.spinny_auth_contactdetails": "id",
    "sp_report_data.sp_report_data_report": "id",
    "sp_web.buy_lead_buylead": "created_on",
    "sp_web.webresults_webarticle": "id",
    "sp_web.external_listing_externallistingplatform": "id",
    "sp_web.external_listing_externalbuyrequest": "created_on",
    "sp_web.external_listing_externallisting": "created_on",
    "sp_web.external_listing_listingplatformaccounts": "id",
    "sp_web.buy_lead_carinterest": "created_on",
    "sp_web.spinny_auth_user_groups": "id",
    "sp_web.buy_lead_activitylog": "created_at",
    "sp_web.accounts_accounts": "created_at",
    "sp_phonecall.campaigns": "id",
    "sp_phonecall.call_logs": "start_time",
    "sp_auction.hubs": "created_at",
    "sp_auction.wallet_transactions": "created_at",
    "sp_auction.tickets": "created_at",
    "sp_auction.delivery_checklists": "created_at",
    "sp_auction.checklist_items": "id",
    "sp_web.listing_stolenvehicle": "created_at",
    "sp_web.django_comments": "id",
    "sp_web.services_service": "created_time",
    "sp_web.sp_payments_receivables": "created_at",
    "sp_web.sp_payments_contract": "created_at",
    "sp_web.buy_lead_buyer": "id",
    "sp_web.loan_application_spinnyloanapplication": "created_at",
    "sp_loan.loan_application_bankloanapplication": "id",
    "sp_web.loan_application_bankloanapplication": "created_at",
    "sp_web.bank_bank": "id",
    "sp_loan.loan_application_bankloanapplicationactivitylog": "id",
    "sp_web.loan_application_spinnyloanapplicationlog": "created_at",
    "sp_analytics_views.hr_report": "hr_finish_time",
    "sp_analytics_views.mr_report": "mr_finish_time",
    "sp_analytics_views.cr_report": "cr_finish_time",
    "sp_analytics_views.pd_report": "pd_finish_time",
    "sp_analytics_views.i3_report": "i3_finish_time",
    "sp_analytics_views.qc_report": "qc_finish_time",
    "sp_refurb.dashboard_workshopjobrowmapping": "created_at",
    "sp_web.payments_payment": "created_time",
    "sp_web.payments_paymentitem": "id",
    "sp_analytics_views.i1_report": "i1_finish_time",
    "sp_analytics_views.i2_report": "i2_finish_time",
    "sp_analytics_views.i4_report": "i4_finish_time",
    "sp_analytics_views.auc_report": "auc_finish_time",
    "sp_web.buy_lead_testdrivefeedback": "created_time",
    "sp_web.buy_lead_testdrivefeedback_car_issue": "id",
    "sp_web.buy_lead_testdrivecarissue": "id",
    "sp_analytics_views.invoice_reports": "invoice_date",
    "sp_analytics_views.report_data": "report_created_at",
    "sp_clearquote.sp_clearquote_cqdatachecklist": "id",
    "sp_web.auth_user": "id",
    "sp_refurb.attendance_dailyattendance": "created_at",
    "sp_report_data.sp_report_data_issuebucketing": "id",
    "sp_report_data.sp_report_data_issuebucketing_bucket_values": "id",
    "sp_report_data.sp_report_data_bucketvalue": "id",
    "sp_report_data.sp_report_data_bucket": "id",
    "sp_refurb.dashboard_joblistcarscore": "created_at",
    "sp_ticket_master.car": "created_at",
    "sp_ticket_master.ticket": "created_at",
    "sp_ticket_master.sub_query": "created_at",
    "sp_ticket_master.query": "created_at",
    "sp_ticket_master.query_heading": "created_at",
    "sp_ticket_master.ticket_parent_child_mapping": "created_at",
    "sp_autopilot.webhook_logs": "created_time",
    "sp_catalogue_inventory.request_item_to_inventory_activity": "created_at",
    "sp_catalogue_inventory.inventory_activity": "created_at",
    "sp_catalogue_inventory.inventory_activity_product_sku": "id",
    "sp_Catalogue_inventory.product_sku": "created_at",
    "sp_report_data.sp_report_data_issueitemrelatedmedia": "id",
    "sp_catalogue_inventory.document": "id",
    "sp_catalogue_inventory.vendor": "created_at",
    "sp_refurb.dashboard_taxcode": "created_at",
    "sp_catalogue_inventory.transfer_order_item_to_product_sku": "created_at",
    "sp_inspections.sp_inspections_cancellationreasonrelatedmedia": "id",
    "sp_refurb.django_content_type": "id",
    "sp_web.file_management_deduction": "id",
    "sp_web.file_management_extraitem": "id",
    "sp_web.listing_paymentrequestlog": "created_at",
    "sp_logistics_mongo.movement_declaration": "created_at",
    "sp_refurb.dashboard_microjobrowsku": "created_at",
    "sp_refurb.dashboard_varianttovariantgroup": "created_at",
    "sp_refurb.dashboard_carparttree": "created_at",
    "sp_refurb.dashboard_carpartfixtat": "created_at",
    "sp_refurb.dashboard_jobtatmultiplier": "created_at",
    "sp_refurb.dashboard_technicianassignment": "created_at",
    "sp_refurb.dashboard_useractivity": "created_at",
    "sp_refurb.dashboard_usermetric": "created_at",
    "sp_catalogue.attributes_attribute": "created_at",
    "sp_refurb.dashboard_refurbcaractivity": "created_at",
    "sp_refurb.dashboard_carpartlabourcostmapping": "created_at",
    "sp_refurb.outbound_outboundstatushistory": "created_at",
    "sp_catalogue_inventory.product_field_value": "created_at",
    "sp_catalogue_inventory.product_update_logs": "id",
    "sp_catalogue_inventory.alternate_product_mapping": "created_at",
    "sp_catalogue_inventory.file_model_relation": "created_at",
    "sp_catalogue_inventory.file_upload": "created_at",
    "sp_catalogue_inventory.custom_content_type": "id",
    "sp_catalogue_inventory.make_model_variant_year": "created_at",
    "sp_catalogue_inventory.alternate_product_to_mmvy_mapping": "created_at",
    "sp_autopilot.trip": "created_time",
    "sp_autopilot.trip_activity": "created_time",
    "sp_autopilot.platform_users": "created_time",
    "sp_autopilot.logistics_user": "created_time",
    "sp_autopilot.time_slots": "created_time",
    "sp_autopilot.locations": "created_time",
    "sp_autopilot.pincode": "created_time",
    "sp_autopilot.vendor_api_call_log": "created_time",
    "sp_autopilot.booked_slots": "created_time",
    "sp_auction.make_model": "created_on",
    "sp_payments.spinny_pay_payable": "created_at",
    "sp_payments.spinny_pay_transactionrequest": "created_at",
    "sp_payments.spinny_pay_carinventory": "created_at",
    "sp_car_part.sp_car_part_parentchildrel": "id",
    "sp_web.address_cluster_cities": "id",
    "sp_web.address_cluster": "created_date",
    "sp_refurb.dashboard_jobcardusermapping": "created_at",
    "sp_refurb.customer_customer": "created_at",
    "sp_refurb.dashboard_jobcard_micro_job_row": "id",
    "sp_refurb.dashboard_microfix": "created_at",
    "sp_analytics_views.integrated_report": "lead_id",
    "sp_refurb.dashboard_rowtransfer": "id",
    "sp_refurb.dashboard_modelhistory": "created_at"
}

# ----------------------------------------------------------
# 🧠 Placeholder transformation logic
# (Replace with your actual script later)
# ----------------------------------------------------------
def transform_query(query: str, PARTITION_COLS: dict) -> str:
    """
    This is a placeholder for your actual SQL transformation logic.
    Replace this function with your script that adds partition filters.
    """
    # ============================================================
    # ===============  MAIN LOGIC ================================
    # ============================================================

    def process_all_selects(parsed_exp):
        """
        Process all SELECT blocks (CTEs, subqueries, main query).
        Ensures that every SELECT node in the tree is visited once.
        """
        for select_exp in parsed_exp.find_all(exp.Select):
            process_select(select_exp)
        return parsed_exp


    def process_select(select_exp: exp.Select):
        """
        Process a SELECT block:
        - Handle FROM & JOINs
        - Recurse into subqueries & CTEs
        """
        from_exp = select_exp.args.get("from")
        if from_exp:
            process_from(from_exp)

        for join_exp in select_exp.args.get("joins", []):
            process_join(join_exp)

        # Recurse into subqueries
        for subquery in select_exp.find_all(exp.Subquery):
            process_select(subquery.this)

        # Recurse into CTEs (important for WITH clauses)
        if select_exp.args.get("with"):
            for cte in select_exp.args["with"].expressions:
                process_select(cte.this)

        return select_exp


    def process_from(from_exp: exp.From):
        """
        Process the FROM clause — add partition filter in WHERE.
        """
        this_exp = from_exp.this

        if isinstance(this_exp, exp.Subquery):
            process_select(this_exp.this)

        elif isinstance(this_exp, exp.Table):
            table_name = this_exp.name
            alias = get_table_alias_or_name(this_exp)
            partition_col = get_partition_col(table_name)
            if partition_col:
                add_partition_to_where(from_exp.parent, alias, partition_col)


    def process_join(join_exp: exp.Join):
        """
        Process JOIN — add partition filter in the ON clause.
        """
        this_exp = join_exp.this

        if isinstance(this_exp, exp.Subquery):
            process_select(this_exp.this)

        elif isinstance(this_exp, exp.Table):
            table_name = this_exp.name
            alias = get_table_alias_or_name(this_exp)
            partition_col = get_partition_col(table_name)
            if partition_col:
                add_partition_to_join(join_exp, alias, partition_col)

    # ============================================================
    # ===============  HELPER FUNCTIONS ==========================
    # ============================================================

    def get_partition_col(table_name: str):
        """
        Returns partition column name for a table (match by suffix or full name).
        """
        for full_name, part_col in PARTITION_COLS.items():
            if full_name.endswith(table_name):
                return part_col
        return None


    def get_table_alias_or_name(table_exp: exp.Table):
        # """
        # Safely return alias if present, otherwise table name.
        # """
        # if table_exp.alias:
        #     return table_exp.alias
        # return table_exp.name
        """Return table alias if present, else None."""
        return table_exp.alias or None


    def add_partition_to_where(select_exp: exp.Select, alias: str, partition_col: str):
        """
        Add "AND alias.partition_col IS NOT NULL" into WHERE.
        """
        if not select_exp:
            return

        col_ref = exp.column(partition_col, table=alias if alias else None)

        # Check if already exists
        existing_conditions = [
            c for c in select_exp.find_all(exp.Is)
            if isinstance(c, exp.Is)
            and isinstance(c.this, exp.Column)
            and c.this.name == partition_col
        ]
        if existing_conditions:
            return  # skip duplicates

        new_condition = exp.Is(this=col_ref, expression=exp.Null()).not_()  # IS NOT NULL

        # Merge or create WHERE
        where_exp = select_exp.args.get("where")
        if where_exp:
            combined = exp.and_(where_exp.this, new_condition)
            select_exp.set("where", exp.Where(this=combined))
        else:
            select_exp.set("where", exp.Where(this=new_condition))


    def add_partition_to_join(join_exp: exp.Join, alias: str, partition_col: str):
        """
        Add "AND alias.partition_col IS NOT NULL" into JOIN ON clause.
        """
        on_exp = join_exp.args.get("on")
        if not on_exp:
            return

        col_ref = exp.column(partition_col, table=alias if alias else None)

        # Skip if already exists
        existing_conditions = [
            c for c in on_exp.find_all(exp.Is)
            if isinstance(c.this, exp.Column) and c.this.name == partition_col
        ]
        if existing_conditions:
            return

        new_condition = exp.Is(this=col_ref, expression=exp.Null()).not_()
        combined = exp.and_(on_exp, new_condition)
        join_exp.set("on", combined)
    
    parsed = sqlglot.parse_one(query, read='trino')
    processed = process_all_selects(parsed)
    processed_query = processed.sql(pretty=True, dialect='trino')
    return processed_query

# ====================================================
# 🖥️ Streamlit UI
# ====================================================
import streamlit as st

# ---------- PAGE CONFIG ----------
st.set_page_config(
    page_title="QueryTune",
    page_icon="/Users/vaibh/Documents/QueryTune_App/QueryTune/images/code.png",
    layout="wide",
)

st.title("QueryTune - sql partition filter adder")

# ---------- INPUT QUERY ----------
input_query = st.text_area(
    "",
    height=300,
    key="input_query",
    placeholder="Paste your sql query here..."
)

# ---------- RUN BUTTON ----------
if st.button("➕ Add Partition Filters", type="primary"):
    if input_query.strip():
        transformed = transform_query(input_query, PARTITION_COLS)
        st.session_state["output_query"] = transformed

# ---------- OUTPUT QUERY ----------
if "output_query" in st.session_state and st.session_state["output_query"]:
    st.subheader("Transformed Query:")
    st.code(st.session_state["output_query"], language="sql")