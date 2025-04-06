# -*- coding: utf-8 -*-

from datetime import timedelta

import pandas as pd
from feast.value_type import ValueType
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.types import String, Int64, Float64, Float32
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import on_demand_feature_view
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.data_format import ParquetFormat
from feast import RequestSource
from typing import Any

zipcode = Entity(name="zipcode", value_type=ValueType.INT64)

zipcode_source = FileSource(
    name="Zipcode source",
    path="data/zipcode_table.parquet",
    file_format=ParquetFormat(),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

zipcode_features = FeatureView(
    name="zipcode_features",
    entities=[zipcode],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="city", dtype=String),
        Field(name="state", dtype=String),
        Field(name="location_type", dtype=String),
        Field(name="tax_returns_filed", dtype=Int64),
        Field(name="population", dtype=Int64),
        Field(name="total_wages", dtype=Int64),
    ],
    source=zipcode_source,
)

dob_ssn = Entity(
    name="dob_ssn",
    value_type=ValueType.STRING,
    description="Date of birth and last four digits of social security number",
)

credit_history_source = FileSource(
    name="Credit history",
    path="data/credit_history.parquet",
    file_format=ParquetFormat(),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

credit_history = FeatureView(
    name="credit_history",
    entities=[dob_ssn],
    ttl=timedelta(days=90),
    schema=[
        Field(name="credit_card_due", dtype=Int64),
        Field(name="mortgage_due", dtype=Int64),
        Field(name="student_loan_due", dtype=Int64),
        Field(name="vehicle_loan_due", dtype=Int64),
        Field(name="hard_pulls", dtype=Int64),
        Field(name="missed_payments_2y", dtype=Int64),
        Field(name="missed_payments_1y", dtype=Int64),
        Field(name="missed_payments_6m", dtype=Int64),
        Field(name="bankruptcies", dtype=Int64),
    ],
    source=credit_history_source,
)


input_request = RequestSource(
    name="application_data",
    schema=[
        Field(name='loan_amnt', dtype=Int64),
    ]
)

@on_demand_feature_view(
   sources=[
       credit_history,
       input_request,
   ],
   schema=[
     Field(name='total_debt_due', dtype=Float64),
   ],
   mode="pandas",
)
def total_debt_calc(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df['total_debt_due'] = (
        features_df['credit_card_due'] + features_df['mortgage_due'] + 
        features_df['student_loan_due'] + features_df['vehicle_loan_due'] + 
        features_df['loan_amnt']
    ).astype(float)
    return df 

credit_score_feature_service_v1 = FeatureService(
    name="credit_score_service_v1",
    features=[
        zipcode_features,
        credit_history,
        total_debt_calc,
    ],
)

customer_data_source = FileSource(
    name="Customer Information",
    path="data/customer_data.parquet",
    file_format=ParquetFormat(),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

customer_features_view = FeatureView(
    name="customer_features",
    entities=[dob_ssn],
    ttl=None,
    schema=[
        Field(name="age", dtype=Int64),
        Field(name="income", dtype=Float32),
        Field(name="marital_status", dtype=String),
        Field(name="num_dependents", dtype=Int64),
        Field(name="credit_score", dtype=Float32),
        Field(name="num_credit_cards", dtype=Int64),
        Field(name="debt_to_income_ratio", dtype=Float32),
        Field(name="avg_transaction_amount", dtype=Float32),
        Field(name="num_transactions_last_month", dtype=Int64),
        Field(name="loan_amount", dtype=Float32),
        Field(name="loan_term_months", dtype=Int64),
        Field(name="interest_rate", dtype=Float32),
        Field(name="missed_payments_last_year", dtype=Int64),
        Field(name="bankruptcies", dtype=Int64),
        Field(name="device_type", dtype=String),
        Field(name="num_devices_linked", dtype=Int64),
        Field(name="city_id", dtype=Int64),
        Field(name="state_id", dtype=Int64),
        Field(name="country_code", dtype=String),
        Field(name="avg_session_duration", dtype=Float32),
        Field(name="last_login_days_ago", dtype=Int64),
        Field(name="account_balance", dtype=Float32),
        Field(name="savings_balance", dtype=Float32),
        Field(name="mortgage_balance", dtype=Float32),
        Field(name="vehicle_loan_balance", dtype=Float32),
        Field(name="student_loan_balance", dtype=Float32),
        Field(name="payment_history_score", dtype=Float32),
        Field(name="payment_frequency", dtype=Int64),
        Field(name="num_employers_past_5_years", dtype=Int64),
        Field(name="current_employer_tenure", dtype=Int64),
        Field(name="employment_status", dtype=String),
        Field(name="education_level", dtype=String),
        Field(name="home_ownership_status", dtype=String),
        Field(name="rent_amount", dtype=Float32),
        Field(name="utility_bills_last_month", dtype=Float32),
        Field(name="mobile_phone_bill", dtype=Float32),
        Field(name="internet_service_bill", dtype=Float32),
        Field(name="insurance_policies", dtype=Int64),
        Field(name="insurance_claims_last_year", dtype=Int64),
        Field(name="preferred_language", dtype=String),
        Field(name="citizenship_status", dtype=String),
        Field(name="social_media_usage_hours_per_day", dtype=Float32),
        Field(name="streaming_service_subscriptions", dtype=Int64),
        Field(name="online_purchases_last_month", dtype=Int64),
        Field(name="in_store_purchases_last_month", dtype=Int64),
        Field(name="avg_cart_value", dtype=Float32),
        Field(name="return_rate", dtype=Float32),
        Field(name="customer_tier", dtype=String),
        Field(name="loyalty_points", dtype=Int64),
        Field(name="loyalty_status", dtype=String),
        Field(name="customer_lifetime_value", dtype=Float32),
        Field(name="churn_risk_score", dtype=Float32),
        Field(name="num_service_calls_last_year", dtype=Int64),
        Field(name="avg_service_call_duration_minutes", dtype=Float32),
        Field(name="fraud_reports_last_year", dtype=Int64),
        Field(name="fraud_risk_score", dtype=Float32),
        Field(name="security_questions_set", dtype=String),
        Field(name="password_reset_count_last_year", dtype=Int64),
        Field(name="email_open_rate", dtype=Float32),
        Field(name="sms_response_rate", dtype=Float32),
        Field(name="app_push_response_rate", dtype=Float32),
        Field(name="device_trust_score", dtype=Float32),
        Field(name="num_linked_accounts", dtype=Int64),
        Field(name="primary_bank_id", dtype=Int64),
        Field(name="secondary_bank_id", dtype=Int64),
        Field(name="preferred_payment_method", dtype=String),
        Field(name="num_failed_transactions_last_month", dtype=Int64),
        Field(name="avg_payment_delay_days", dtype=Float32),
        Field(name="subscription_status", dtype=String),
        Field(name="num_subscriptions", dtype=Int64),
        Field(name="subscription_revenue", dtype=Float32),
        Field(name="website_visits_last_month", dtype=Int64),
        Field(name="mobile_app_logins_last_month", dtype=Int64),
        Field(name="desktop_app_logins_last_month", dtype=Int64),
        Field(name="avg_pages_per_visit", dtype=Float32),
        Field(name="bounce_rate", dtype=Float32),
        Field(name="time_spent_per_visit_minutes", dtype=Float32),
        Field(name="customer_support_rating", dtype=Float32),
        Field(name="complaints_last_year", dtype=Int64),
        Field(name="complaints_resolved", dtype=Int64),
        Field(name="product_review_count", dtype=Int64),
        Field(name="avg_product_rating", dtype=Float32),
        Field(name="wishlist_item_count", dtype=Int64),
        Field(name="cart_abandonment_rate", dtype=Float32),
        Field(name="num_friends_referred", dtype=Int64),
        Field(name="referral_conversion_rate", dtype=Float32),
        Field(name="promo_codes_used_last_year", dtype=Int64),
        Field(name="gift_card_redemption_count", dtype=Int64),
        Field(name="event_participation_count", dtype=Int64),
        Field(name="personalized_offer_acceptance_rate", dtype=Float32),
        Field(name="survey_participation_count", dtype=Int64),
        Field(name="survey_avg_score", dtype=Float32),
        Field(name="net_promoter_score", dtype=Float32),
    ],
    online=True,
    source=customer_data_source,
    tags={"team": "analytics", "use_case": "customer_segmentation"},
)

loan_table_source = FileSource(
    name="Loan history",
    path="data/loan_table.parquet",
    file_format=ParquetFormat(),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

loan_table = FeatureView(
    name="loan_view",
    entities=[dob_ssn],
    ttl=timedelta(days=90),
    schema=[
        Field(name="person_age", dtype=Int64),
        Field(name="loan_intent", dtype=String),
        Field(name="person_income", dtype=Int64),
    ],
    source=loan_table_source,
)

credit_score_feature_service_v2 = FeatureService(
    name="credit_score_service_v2",
    features=[
        zipcode_features,
        credit_history,
        total_debt_calc,
        loan_table
    ],
)