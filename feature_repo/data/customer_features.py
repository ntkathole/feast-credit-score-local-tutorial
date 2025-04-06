import pandas as pd
import numpy as np

# Number of rows
n_rows = 1000

# Realistic feature names (100 total)
columns = [
    "customer_id", "age", "income", "marital_status", "num_dependents",
    "credit_score", "num_credit_cards", "debt_to_income_ratio", "avg_transaction_amount",
    "num_transactions_last_month", "loan_amount", "loan_term_months", "interest_rate",
    "missed_payments_last_year", "bankruptcies", "device_type", "num_devices_linked",
    "city_id", "state_id", "country_code", "avg_session_duration", "last_login_days_ago",
    "account_balance", "savings_balance", "mortgage_balance", "vehicle_loan_balance",
    "student_loan_balance", "payment_history_score", "payment_frequency",
    "num_employers_past_5_years", "current_employer_tenure", "employment_status",
    "education_level", "home_ownership_status", "rent_amount", "utility_bills_last_month",
    "mobile_phone_bill", "internet_service_bill", "insurance_policies", "insurance_claims_last_year",
    "preferred_language", "citizenship_status", "social_media_usage_hours_per_day",
    "streaming_service_subscriptions", "online_purchases_last_month", "in_store_purchases_last_month",
    "avg_cart_value", "return_rate", "customer_tier", "loyalty_points", "loyalty_status",
    "customer_lifetime_value", "churn_risk_score", "account_open_date",
    "num_service_calls_last_year", "avg_service_call_duration_minutes",
    "fraud_reports_last_year", "fraud_risk_score", "security_questions_set",
    "password_reset_count_last_year", "email_open_rate", "sms_response_rate",
    "app_push_response_rate", "device_trust_score", "num_linked_accounts",
    "primary_bank_id", "secondary_bank_id", "preferred_payment_method",
    "num_failed_transactions_last_month", "avg_payment_delay_days",
    "subscription_status", "num_subscriptions", "subscription_revenue",
    "website_visits_last_month", "mobile_app_logins_last_month",
    "desktop_app_logins_last_month", "avg_pages_per_visit", "bounce_rate",
    "time_spent_per_visit_minutes", "customer_support_rating",
    "complaints_last_year", "complaints_resolved", "product_review_count",
    "avg_product_rating", "wishlist_item_count", "cart_abandonment_rate",
    "num_friends_referred", "referral_conversion_rate", "promo_codes_used_last_year",
    "gift_card_redemption_count", "event_participation_count",
    "personalized_offer_acceptance_rate", "survey_participation_count",
    "survey_avg_score", "net_promoter_score"
]

# Random data generation
np.random.seed(42)
data = {}
for col in columns:
    if "id" in col or "num_" in col or "count" in col:
        data[col] = np.random.randint(0, 1000, n_rows)
    elif "amount" in col or "balance" in col or "income" in col or "value" in col:
        data[col] = np.random.uniform(1000, 50000, n_rows).round(2)
    elif "score" in col or "rate" in col or "risk" in col:
        data[col] = np.random.uniform(0, 1, n_rows).round(4)
    elif "date" in col:
        data[col] = pd.date_range(start="2022-01-01", periods=n_rows, freq="D")
    elif "status" in col or "type" in col or "level" in col or "language" in col:
        data[col] = np.random.choice(["A", "B", "C"], n_rows)
    else:
        data[col] = np.random.uniform(0, 100, n_rows).round(2)

# Add Feast timestamps
data["event_timestamp"] = pd.date_range(start="2025-01-01", periods=n_rows, freq="H")
data["created_timestamp"] = pd.date_range(start="2025-01-01", periods=n_rows, freq="H")

birth_dates = pd.to_datetime(
    np.random.choice(pd.date_range('1970-01-01', '2000-12-31'), size=n_rows)
)
last4_ssn = np.random.randint(1000, 9999, size=n_rows)
dob_ssn = birth_dates.strftime('%Y%m%d') + "_" + last4_ssn.astype(str)
data["dob_ssn"] = dob_ssn

# Create DataFrame
df = pd.DataFrame(data)
df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
df["created_timestamp"] = pd.to_datetime(df["created_timestamp"], utc=True)

# Save as Parquet
df.to_parquet("customer_data.parquet")
