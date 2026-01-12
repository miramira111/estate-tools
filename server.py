# -*- coding: utf-8 -*-
"""
媒介契約管理システム バックエンド
- Flask + セッション認証
- Supabase Postgres によるデータ保持
- レコード単位ロック機能
"""

import csv
import io
import json
import os
import re
from datetime import datetime, timedelta
from functools import wraps

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, Response, jsonify, request, send_from_directory, session
from flask_cors import CORS


# ------------------------------------------------------------
# 基本設定
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.env")

LOCK_DURATION_MINUTES = 2
DEFAULT_GOAL = {"storeTarget": 0, "staffTargets": {}, "includeStaff": []}


def current_month_key():
    now = datetime.now()
    return f"{now.year:04d}-{now.month:02d}"


# ------------------------------------------------------------
# 設定読み込み
# ------------------------------------------------------------
def load_config():
    config = {}
    # 環境変数から読み込み（Render用）
    for key in os.environ:
        config[key] = os.environ[key]

    # config.envがあれば読み込み（ローカル開発用）
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                # 環境変数が設定されていない場合のみ上書き
                if key.strip() not in config:
                    config[key.strip()] = value.strip()
    return config


CONFIG = load_config()

# データベース接続文字列
DATABASE_URL = CONFIG.get("DATABASE_URL")


def get_users():
    """config.envまたは環境変数からユーザー一覧を取得"""
    users = {}
    for key, value in CONFIG.items():
        if key.startswith("USER_") or key == "ADMIN_USER":
            parts = value.split(":", 2)
            if len(parts) == 3:
                login_id, password, display_name = parts
                users[login_id] = {
                    "password": password,
                    "display_name": display_name,
                    "is_admin": key == "ADMIN_USER"
                }
    return users


USERS = get_users()


# ------------------------------------------------------------
# データベース接続
# ------------------------------------------------------------
def get_db_connection():
    """データベース接続を取得"""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn


# ------------------------------------------------------------
# Flask アプリ
# ------------------------------------------------------------
app = Flask(__name__, static_folder="static")
app.permanent_session_lifetime = timedelta(hours=8)
app.secret_key = CONFIG.get("SECRET_KEY", "default-secret-key")
CORS(app, supports_credentials=True)


# ------------------------------------------------------------
# 共通ユーティリティ
# ------------------------------------------------------------
def parse_date(date_str):
    """日付文字列をdateオブジェクトに変換"""
    if not date_str or date_str == "":
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def format_date(d):
    """dateオブジェクトを文字列に変換"""
    if d is None:
        return ""
    return d.isoformat()


def format_datetime(dt):
    """datetimeオブジェクトを文字列に変換"""
    if dt is None:
        return ""
    return dt.isoformat()


# ------------------------------------------------------------
# app_settings テーブル操作
# ------------------------------------------------------------
def load_app_setting(key, default=None):
    """app_settingsから設定を読み込み"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM app_settings WHERE key = %s", (key,))
            row = cur.fetchone()
            if row is None:
                return default if default is not None else {}
            return row["value"]


def save_app_setting(key, data):
    """app_settingsに設定を保存"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_settings (key, value)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, json.dumps(data, ensure_ascii=False))
            )
        conn.commit()


def load_masters():
    return load_app_setting("masters", {})


def save_masters(data):
    save_app_setting("masters", data)


def load_customer_masters():
    default = {
        "meta": {"name": "顧客管理マスターデータ"},
        "inquiry_source_sell": [],
        "inquiry_source_buy": [],
        "property_type": [],
        "staff": [],
        "status_sell": [],
        "status_buy": [],
        "contact_method": [],
        "progress_status": []
    }
    return load_app_setting("customer_masters", default)


def save_customer_masters(data):
    data["meta"] = data.get("meta", {})
    data["meta"]["updated_at"] = datetime.now().isoformat()
    save_app_setting("customer_masters", data)


def load_status_colors():
    return load_app_setting("status_colors", {})


def save_status_colors(data):
    save_app_setting("status_colors", data)


def load_goals_data():
    data = load_app_setting("goals", {
        "default": DEFAULT_GOAL,
        "monthly": {current_month_key(): DEFAULT_GOAL},
        "annual": {}
    })
    if not isinstance(data, dict):
        data = {}

    monthly = data.get("monthly")
    if monthly is None:
        legacy_goal = normalize_goal(data)
        month_key = current_month_key()
        data = {"default": legacy_goal, "monthly": {month_key: legacy_goal}, "annual": {}}
        save_app_setting("goals", data)
        monthly = data["monthly"]

    normalized_monthly = {}
    if isinstance(monthly, dict):
        for key, goal in monthly.items():
            normalized_monthly[key] = normalize_goal(goal)

    default_goal = normalize_goal(data.get("default") or DEFAULT_GOAL)
    annual = data.get("annual") or {}
    normalized_annual = {}
    if isinstance(annual, dict):
        for key, goal in annual.items():
            normalized_annual[str(key)] = normalize_goal(goal)

    return {"default": default_goal, "monthly": normalized_monthly, "annual": normalized_annual}


def save_goals_data(data):
    save_app_setting("goals", data)


def load_sales_data():
    data = load_app_setting("sales", {
        "default": {"store": 0, "staff": {}},
        "monthly": {current_month_key(): {"store": 0, "staff": {}}},
        "annual": {}
    })
    if not isinstance(data, dict):
        data = {}
    monthly = data.get("monthly") or {}
    annual = data.get("annual") or {}
    default = normalize_sales(data.get("default"))
    normalized_monthly = {}
    for key, rec in monthly.items():
        normalized_monthly[key] = normalize_sales(rec)
    normalized_annual = {}
    for key, rec in annual.items():
        normalized_annual[str(key)] = normalize_sales(rec)
    return {"default": default, "monthly": normalized_monthly, "annual": normalized_annual}


def save_sales_data(data):
    save_app_setting("sales", data)


def load_case_numbers():
    return load_app_setting("case_numbers", {"sell": {}, "buy": {}, "investment": {}})


def save_case_numbers(data):
    save_app_setting("case_numbers", data)


# ------------------------------------------------------------
# 目標・売上ユーティリティ
# ------------------------------------------------------------
def normalize_goal(goal):
    normalized = {"storeTarget": 0, "staffTargets": {}, "includeStaff": []}
    if isinstance(goal, dict):
        normalized.update(goal)

    try:
        normalized["storeTarget"] = max(0, int(normalized.get("storeTarget") or 0))
    except (TypeError, ValueError):
        normalized["storeTarget"] = 0

    staff_targets = {}
    for name, target in (normalized.get("staffTargets") or {}).items():
        try:
            staff_targets[name] = max(0, int(target))
        except (TypeError, ValueError):
            continue
    normalized["staffTargets"] = staff_targets

    include_staff = []
    for name in normalized.get("includeStaff") or []:
        if isinstance(name, str) and name.strip():
            include_staff.append(name.strip())
    normalized["includeStaff"] = include_staff

    return normalized


def get_goal_for_month(month_key, goals_data=None):
    goals = goals_data or load_goals_data()
    return goals.get("monthly", {}).get(month_key) or goals.get("default") or DEFAULT_GOAL


def save_goal_for_month(month_key, goal_body):
    goals = load_goals_data()
    goals.setdefault("monthly", {})
    goals["monthly"][month_key] = normalize_goal(goal_body)
    save_goals_data(goals)
    return goals


def get_goal_for_year(year_key, goals_data=None, fallback_to_default=True):
    goals = goals_data or load_goals_data()
    found = goals.get("annual", {}).get(str(year_key))
    if found:
        return found
    if fallback_to_default:
        return goals.get("default") or DEFAULT_GOAL
    return None


def save_goal_for_year(year_key, goal_body):
    goals = load_goals_data()
    goals.setdefault("annual", {})
    normalized = normalize_goal(goal_body)
    normalized["storeTarget"] = sum(normalized.get("staffTargets", {}).values())
    goals["annual"][str(year_key)] = normalized
    save_goals_data(goals)
    return goals


def normalize_sales(rec):
    cleaned = {"store": 0, "staff": {}}
    if not isinstance(rec, dict):
        return cleaned
    staff = {}
    for name, val in (rec.get("staff") or {}).items():
        try:
            staff[name] = max(0, float(val))
        except (TypeError, ValueError):
            continue
    cleaned["staff"] = staff
    try:
        store_val = rec.get("store")
        store_num = float(store_val)
        if store_num < 0:
            store_num = 0.0
        cleaned["store"] = store_num
    except (TypeError, ValueError):
        cleaned["store"] = sum(staff.values())
    if cleaned["store"] == 0 and staff:
        cleaned["store"] = sum(staff.values())
    return cleaned


def get_sales_for_month(month_key, sales_data=None):
    sales = sales_data or load_sales_data()
    return sales.get("monthly", {}).get(month_key) or sales.get("default") or {"store": 0, "staff": {}}


def save_sales_for_month(month_key, body):
    sales = load_sales_data()
    sales.setdefault("monthly", {})
    sales["monthly"][month_key] = normalize_sales(body)
    save_sales_data(sales)
    return sales


def get_sales_for_year(year_key, sales_data=None, fallback_to_default=True):
    sales = sales_data or load_sales_data()
    found = sales.get("annual", {}).get(str(year_key))
    if found:
        return found
    if fallback_to_default:
        return sales.get("default") or {"store": 0, "staff": {}}
    return None


def save_sales_for_year(year_key, body):
    sales = load_sales_data()
    sales.setdefault("annual", {})
    sales["annual"][str(year_key)] = normalize_sales(body)
    save_sales_data(sales)
    return sales


def normalize_month_key(month_key):
    if not month_key:
        return current_month_key()
    if not isinstance(month_key, str):
        return None
    if re.match(r"^\d{4}-\d{2}$", month_key):
        return month_key
    return None


def month_key_from_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except (TypeError, ValueError):
        return None
    return f"{dt.year:04d}-{dt.month:02d}"


# ------------------------------------------------------------
# contracts テーブル操作
# ------------------------------------------------------------
def db_row_to_contract(row):
    """DBの行データをJSON形式の契約データに変換"""
    return {
        "id": row["id"],
        "source_file": row["source_file"] or "",
        "キーボックス番号": row["key_box_number"] or "",
        "ステータス日付": format_date(row["status_date"]),
        "レインズ変更日": format_date(row["reins_change_date"]),
        "レインズ変更済み": bool(row["reins_changed"]),
        "レインズ満了日": format_date(row["reins_expire_date"]),
        "レインズ登録フラグ": bool(row["reins_registered"]),
        "中止理由": row["cancel_reason"],
        "作成日時": format_datetime(row["created_at"]),
        "価格推移": row["price_history"] or [],
        "備考": row["notes"] or "",
        "反響媒体": row["media_source"] or "",
        "取引状況": row["deal_status"] or "",
        "売主": row["seller_name"] or "",
        "売主住所": row["seller_address"] or "",
        "売主連絡先": row["seller_contact"] or "",
        "変更履歴": row["change_history"] or [],
        "媒介期日": format_date(row["mediation_expire_date"]),
        "成約情報": row["deal_info"],
        "担当": row["staff_id"] or "",
        "新規媒介締結日": format_date(row["mediation_start_date"]),
        "更新日時": format_datetime(row["updated_at"]),
        "更新者": row["updated_by"] or "",
        "物件所在地": row["property_address"] or "",
        "物件種別": row["property_type"] or "",
        "現在の媒介価格": row["current_price"],
        "現況": row["occupancy_status"] or "",
        "申込日": format_date(row["application_date"]),
        "種別": row["contract_type"] or "",
        "買取情報": row["purchase_info"],
        "鍵の場所": row["key_location"] or "",
    }


def contract_to_db_params(contract, year_month=None):
    """JSON形式の契約データをDBパラメータに変換"""
    current_price = contract.get("現在の媒介価格")
    if current_price is not None:
        try:
            current_price = int(float(current_price))
        except (ValueError, TypeError):
            current_price = None

    cancel_reason = contract.get("中止理由")
    if isinstance(cancel_reason, dict):
        cancel_reason = json.dumps(cancel_reason, ensure_ascii=False)

    return {
        "id": contract.get("id"),
        "year_month": year_month or contract.get("year_month"),
        "source_file": contract.get("source_file") or "",
        "key_box_number": contract.get("キーボックス番号") or None,
        "status_date": parse_date(contract.get("ステータス日付")),
        "reins_change_date": parse_date(contract.get("レインズ変更日")),
        "reins_changed": bool(contract.get("レインズ変更済み")),
        "reins_expire_date": parse_date(contract.get("レインズ満了日")),
        "reins_registered": bool(contract.get("レインズ登録フラグ")),
        "cancel_reason": cancel_reason,
        "created_at": contract.get("作成日時"),
        "updated_at": contract.get("更新日時"),
        "updated_by": contract.get("更新者") or None,
        "notes": contract.get("備考") or None,
        "media_source": contract.get("反響媒体") or None,
        "deal_status": contract.get("取引状況") or None,
        "seller_name": contract.get("売主") or None,
        "seller_address": contract.get("売主住所") or None,
        "seller_contact": contract.get("売主連絡先") or None,
        "mediation_expire_date": parse_date(contract.get("媒介期日")),
        "mediation_start_date": parse_date(contract.get("新規媒介締結日")),
        "staff_id": contract.get("担当") or None,
        "property_address": contract.get("物件所在地") or None,
        "property_type": contract.get("物件種別") or None,
        "current_price": current_price,
        "occupancy_status": contract.get("現況") or None,
        "application_date": parse_date(contract.get("申込日")),
        "contract_type": contract.get("種別") or None,
        "key_location": contract.get("鍵の場所") or None,
        "price_history": json.dumps(contract.get("価格推移") or [], ensure_ascii=False),
        "change_history": json.dumps(contract.get("変更履歴") or [], ensure_ascii=False),
        "deal_info": json.dumps(contract.get("成約情報"), ensure_ascii=False) if contract.get("成約情報") else None,
        "purchase_info": json.dumps(contract.get("買取情報"), ensure_ascii=False) if contract.get("買取情報") else None,
    }


def load_all_contracts():
    """全ての契約を読み込み"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM contracts ORDER BY property_address")
            rows = cur.fetchall()
    return [db_row_to_contract(row) for row in rows]


def find_contract(contract_id):
    """契約IDで契約を検索"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM contracts WHERE id = %s", (contract_id,))
            row = cur.fetchone()
            if row is None:
                return None
            return db_row_to_contract(row)


def save_contract(contract, year_month=None):
    """契約を保存（upsert）"""
    params = contract_to_db_params(contract, year_month)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO contracts (
                    id, year_month, source_file,
                    key_box_number, status_date, reins_change_date,
                    reins_changed, reins_expire_date, reins_registered,
                    cancel_reason, created_at, updated_at, updated_by,
                    notes, media_source, deal_status,
                    seller_name, seller_address, seller_contact,
                    mediation_expire_date, mediation_start_date, staff_id,
                    property_address, property_type, current_price,
                    occupancy_status, application_date, contract_type,
                    key_location, price_history, change_history,
                    deal_info, purchase_info
                ) VALUES (
                    %(id)s, %(year_month)s, %(source_file)s,
                    %(key_box_number)s, %(status_date)s, %(reins_change_date)s,
                    %(reins_changed)s, %(reins_expire_date)s, %(reins_registered)s,
                    %(cancel_reason)s, %(created_at)s, %(updated_at)s, %(updated_by)s,
                    %(notes)s, %(media_source)s, %(deal_status)s,
                    %(seller_name)s, %(seller_address)s, %(seller_contact)s,
                    %(mediation_expire_date)s, %(mediation_start_date)s, %(staff_id)s,
                    %(property_address)s, %(property_type)s, %(current_price)s,
                    %(occupancy_status)s, %(application_date)s, %(contract_type)s,
                    %(key_location)s, %(price_history)s, %(change_history)s,
                    %(deal_info)s, %(purchase_info)s
                )
                ON CONFLICT (id) DO UPDATE SET
                    year_month = EXCLUDED.year_month,
                    source_file = EXCLUDED.source_file,
                    key_box_number = EXCLUDED.key_box_number,
                    status_date = EXCLUDED.status_date,
                    reins_change_date = EXCLUDED.reins_change_date,
                    reins_changed = EXCLUDED.reins_changed,
                    reins_expire_date = EXCLUDED.reins_expire_date,
                    reins_registered = EXCLUDED.reins_registered,
                    cancel_reason = EXCLUDED.cancel_reason,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by,
                    notes = EXCLUDED.notes,
                    media_source = EXCLUDED.media_source,
                    deal_status = EXCLUDED.deal_status,
                    seller_name = EXCLUDED.seller_name,
                    seller_address = EXCLUDED.seller_address,
                    seller_contact = EXCLUDED.seller_contact,
                    mediation_expire_date = EXCLUDED.mediation_expire_date,
                    mediation_start_date = EXCLUDED.mediation_start_date,
                    staff_id = EXCLUDED.staff_id,
                    property_address = EXCLUDED.property_address,
                    property_type = EXCLUDED.property_type,
                    current_price = EXCLUDED.current_price,
                    occupancy_status = EXCLUDED.occupancy_status,
                    application_date = EXCLUDED.application_date,
                    contract_type = EXCLUDED.contract_type,
                    key_location = EXCLUDED.key_location,
                    price_history = EXCLUDED.price_history,
                    change_history = EXCLUDED.change_history,
                    deal_info = EXCLUDED.deal_info,
                    purchase_info = EXCLUDED.purchase_info
                """,
                params
            )
        conn.commit()


def delete_contract(contract_id):
    """契約を削除"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM contracts WHERE id = %s", (contract_id,))
        conn.commit()


def duplicate_exists(contract_id, exclude_id=None):
    """重複チェック"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if exclude_id:
                cur.execute(
                    "SELECT 1 FROM contracts WHERE id = %s AND id != %s LIMIT 1",
                    (contract_id, exclude_id)
                )
            else:
                cur.execute("SELECT 1 FROM contracts WHERE id = %s LIMIT 1", (contract_id,))
            return cur.fetchone() is not None


# ------------------------------------------------------------
# customers テーブル操作
# ------------------------------------------------------------
def db_row_to_customer(row):
    """DBの行データをJSON形式の顧客データに変換"""
    return {
        "id": str(row["id"]),
        "case_number": row["case_number"] or "",
        "status": row["status"] or "",
        "staff_id": row["staff_id"] or "",
        "inquiry_date": format_date(row["inquiry_date"]),
        "inquiry_source": row["inquiry_source"] or "",
        "contact_method": row["contact_method"] or "",
        "property_type": row["property_type"] or "",
        "target_property": row["target_property"] or "",
        "assessment_address": row["assessment_address"] or "",
        "desired_property": row["desired_property"] or "",
        "customer_name": row["customer_name"] or "",
        "phone": row["phone"] or "",
        "current_address": row["current_address"] or "",
        "email": row["email"] or "",
        "first_call": row["first_call"] or "",
        "call_status": row["call_status"] or "",
        "mail_status": row["mail_status"] or "",
        "sms_status": row["sms_status"] or "",
        "showing_status": row["showing_status"] or "",
        "pre_assessment": row["pre_assessment"] or "",
        "visit_status": row["visit_status"] or "",
        "mediation": row["mediation_status"] or "",
        "contract": row["contract_status"] or "",
        "expected_yield": row["expected_yield"] or "",
        "yield_rate": row["expected_yield"] or "",
        "expected_rent": row["expected_rent"] or "",
        "own_funds": row["self_funds"] or "",
        "self_funds": row["self_funds"] or "",
        "loan_amount": row["desired_loan"] or "",
        "desired_loan": row["desired_loan"] or "",
        "desired_area": row["preferred_area"] or "",
        "preferred_area": row["preferred_area"] or "",
        "memo": row["memo"] or "",
        "year": row["year"],
        "created_at": format_datetime(row["created_at"]),
        "updated_at": format_datetime(row["updated_at"]),
    }


def load_customers(category, year):
    """顧客データを読み込み"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM customers WHERE category = %s AND year = %s ORDER BY case_number DESC",
                (category, year)
            )
            rows = cur.fetchall()
    customers = [db_row_to_customer(row) for row in rows]
    return {
        "meta": {
            "category": category,
            "year": year,
        },
        "customers": customers
    }


def save_customer(category, year, customer):
    """顧客を保存（upsert）"""
    customer_id = customer.get("id")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO customers (
                    id, category, year, case_number, status, staff_id,
                    inquiry_date, inquiry_source, contact_method,
                    property_type, target_property, assessment_address, desired_property,
                    customer_name, phone, current_address, email,
                    first_call, call_status, mail_status, sms_status,
                    showing_status, pre_assessment, visit_status,
                    mediation_status, contract_status,
                    expected_yield, expected_rent, self_funds, desired_loan, preferred_area,
                    memo, created_at, updated_at
                ) VALUES (
                    %(id)s::uuid, %(category)s, %(year)s, %(case_number)s, %(status)s, %(staff_id)s,
                    %(inquiry_date)s, %(inquiry_source)s, %(contact_method)s,
                    %(property_type)s, %(target_property)s, %(assessment_address)s, %(desired_property)s,
                    %(customer_name)s, %(phone)s, %(current_address)s, %(email)s,
                    %(first_call)s, %(call_status)s, %(mail_status)s, %(sms_status)s,
                    %(showing_status)s, %(pre_assessment)s, %(visit_status)s,
                    %(mediation_status)s, %(contract_status)s,
                    %(expected_yield)s, %(expected_rent)s, %(self_funds)s, %(desired_loan)s, %(preferred_area)s,
                    %(memo)s, %(created_at)s, %(updated_at)s
                )
                ON CONFLICT (id) DO UPDATE SET
                    category = EXCLUDED.category,
                    year = EXCLUDED.year,
                    case_number = EXCLUDED.case_number,
                    status = EXCLUDED.status,
                    staff_id = EXCLUDED.staff_id,
                    inquiry_date = EXCLUDED.inquiry_date,
                    inquiry_source = EXCLUDED.inquiry_source,
                    contact_method = EXCLUDED.contact_method,
                    property_type = EXCLUDED.property_type,
                    target_property = EXCLUDED.target_property,
                    assessment_address = EXCLUDED.assessment_address,
                    desired_property = EXCLUDED.desired_property,
                    customer_name = EXCLUDED.customer_name,
                    phone = EXCLUDED.phone,
                    current_address = EXCLUDED.current_address,
                    email = EXCLUDED.email,
                    first_call = EXCLUDED.first_call,
                    call_status = EXCLUDED.call_status,
                    mail_status = EXCLUDED.mail_status,
                    sms_status = EXCLUDED.sms_status,
                    showing_status = EXCLUDED.showing_status,
                    pre_assessment = EXCLUDED.pre_assessment,
                    visit_status = EXCLUDED.visit_status,
                    mediation_status = EXCLUDED.mediation_status,
                    contract_status = EXCLUDED.contract_status,
                    expected_yield = EXCLUDED.expected_yield,
                    expected_rent = EXCLUDED.expected_rent,
                    self_funds = EXCLUDED.self_funds,
                    desired_loan = EXCLUDED.desired_loan,
                    preferred_area = EXCLUDED.preferred_area,
                    memo = EXCLUDED.memo,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "id": customer_id,
                    "category": category,
                    "year": year,
                    "case_number": customer.get("case_number") or "",
                    "status": customer.get("status") or None,
                    "staff_id": customer.get("staff_id") or None,
                    "inquiry_date": parse_date(customer.get("inquiry_date")),
                    "inquiry_source": customer.get("inquiry_source") or None,
                    "contact_method": customer.get("contact_method") or None,
                    "property_type": customer.get("property_type") or None,
                    "target_property": customer.get("target_property") or None,
                    "assessment_address": customer.get("assessment_address") or None,
                    "desired_property": customer.get("desired_property") or None,
                    "customer_name": customer.get("customer_name") or None,
                    "phone": customer.get("phone") or None,
                    "current_address": customer.get("current_address") or None,
                    "email": customer.get("email") or None,
                    "first_call": customer.get("first_call") or None,
                    "call_status": customer.get("call_status") or None,
                    "mail_status": customer.get("mail_status") or None,
                    "sms_status": customer.get("sms_status") or None,
                    "showing_status": customer.get("showing_status") or None,
                    "pre_assessment": customer.get("pre_assessment") or None,
                    "visit_status": customer.get("visit_status") or None,
                    "mediation_status": customer.get("mediation") or customer.get("mediation_status") or None,
                    "contract_status": customer.get("contract") or customer.get("contract_status") or None,
                    "expected_yield": customer.get("yield_rate") or customer.get("expected_yield") or None,
                    "expected_rent": customer.get("expected_rent") or None,
                    "self_funds": customer.get("own_funds") or customer.get("self_funds") or None,
                    "desired_loan": customer.get("loan_amount") or customer.get("desired_loan") or None,
                    "preferred_area": customer.get("desired_area") or customer.get("preferred_area") or None,
                    "memo": customer.get("memo") or None,
                    "created_at": customer.get("created_at") or datetime.now().isoformat(),
                    "updated_at": customer.get("updated_at") or datetime.now().isoformat(),
                }
            )
        conn.commit()


def delete_customer(customer_id):
    """顧客を削除"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM customers WHERE id = %s::uuid", (customer_id,))
        conn.commit()


def get_customer_by_id(category, year, customer_id):
    """顧客IDで顧客を検索"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM customers WHERE id = %s::uuid AND category = %s AND year = %s",
                (customer_id, category, year)
            )
            row = cur.fetchone()
            if row is None:
                return None
            return db_row_to_customer(row)


def generate_case_number_for_date(category, year, inquiry_date, existing_customers):
    """反響日ベースで案件番号を採番"""
    prefix = {"sell": "S", "buy": "B", "investment": "R"}.get(category, "X")
    year_short = str(year)[-2:] if year >= 2000 else str(year)

    sorted_customers = sorted(
        [c for c in existing_customers if c.get("inquiry_date")],
        key=lambda c: c.get("inquiry_date") or ""
    )

    seq = 1
    for c in sorted_customers:
        c_date = c.get("inquiry_date") or ""
        if c_date <= inquiry_date:
            seq += 1
        else:
            break

    return f"{prefix}{year_short}{seq:04d}"


def reassign_case_numbers(category, year):
    """指定カテゴリ・年の全顧客の案件番号を反響日順に再採番"""
    data = load_customers(category, year)
    customers = data.get("customers", [])

    if not customers:
        return 0

    prefix = {"sell": "S", "buy": "B", "investment": "R"}.get(category, "X")
    year_short = str(year)[-2:] if year >= 2000 else str(year)

    def case_seq(value):
        if not value:
            return 9999
        match = re.search(r"(\d{4})$", str(value))
        return int(match.group(1)) if match else 9999

    sorted_customers = sorted(
        customers,
        key=lambda c: (c.get("inquiry_date") or "9999-99-99", case_seq(c.get("case_number")), c.get("case_number") or "", c.get("created_at") or "")
    )

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for idx, customer in enumerate(sorted_customers, 1):
                new_case_number = f"{prefix}{year_short}{idx:04d}"
                cur.execute(
                    "UPDATE customers SET case_number = %s WHERE id = %s::uuid",
                    (new_case_number, customer["id"])
                )
        conn.commit()

    return len(sorted_customers)


# ------------------------------------------------------------
# record_locks テーブル操作
# ------------------------------------------------------------
def cleanup_expired_locks():
    """期限切れのロックを削除"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM record_locks WHERE expires_at < NOW()")
        conn.commit()


def check_lock_available(resource_type, resource_id, user):
    """ロックを取得"""
    cleanup_expired_locks()
    now = datetime.utcnow()
    expires_at = now + timedelta(minutes=LOCK_DURATION_MINUTES)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 既存のロックをチェック
            cur.execute(
                "SELECT * FROM record_locks WHERE resource_type = %s AND resource_id = %s",
                (resource_type, resource_id)
            )
            existing = cur.fetchone()

            if existing:
                return False, {
                    "user": existing["locked_by"],
                    "locked_at": format_datetime(existing["locked_at"]),
                    "expires_at": format_datetime(existing["expires_at"]),
                }

            # 新しいロックを作成
            try:
                cur.execute(
                    """
                    INSERT INTO record_locks (resource_type, resource_id, locked_by, locked_at, expires_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (resource_type, resource_id, user, now, expires_at)
                )
                conn.commit()
                return True, {
                    "user": user,
                    "locked_at": now.isoformat(),
                    "expires_at": expires_at.isoformat(),
                }
            except psycopg2.IntegrityError:
                conn.rollback()
                return False, {"user": "unknown", "expires_at": ""}


def release_lock(resource_type, resource_id):
    """ロックを解放"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM record_locks WHERE resource_type = %s AND resource_id = %s",
                (resource_type, resource_id)
            )
        conn.commit()


def get_all_locks():
    """全ロックを取得"""
    cleanup_expired_locks()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM record_locks")
            rows = cur.fetchall()
    return [
        {
            "contract_id": row["resource_id"],
            "resource_type": row["resource_type"],
            "user": row["locked_by"],
            "locked_at": format_datetime(row["locked_at"]),
            "expires_at": format_datetime(row["expires_at"]),
        }
        for row in rows
    ]


# ------------------------------------------------------------
# 契約IDパース・ファイル名生成
# ------------------------------------------------------------
def parse_contract_id_components(contract_id):
    """媒介No.から(西暦, 月, 連番)のタプルを返す"""
    parts = contract_id.split("-")
    if len(parts) != 3:
        return None, "format"

    year_part, month_part, seq_part = parts
    try:
        if year_part.lower().startswith("r"):
            era_year = int(year_part[1:])
            if era_year <= 0:
                return None, "year"
            year = 2018 + era_year
        else:
            year_short = int(year_part)
            year = 2000 + year_short if year_short < 100 else year_short

        month = int(month_part)
        seq = int(seq_part)
    except ValueError:
        return None, "value"

    if month < 1 or month > 12:
        return None, "month"

    return (year, month, seq), None


def parse_contract_id(contract_id):
    """媒介No.から年月を抽出する"""
    if not contract_id:
        return None, "媒介No.が空です"

    components, parse_error = parse_contract_id_components(contract_id)
    if parse_error:
        if parse_error == "format":
            return None, f"媒介No.の形式が不正です（例: R7-1-1）: {contract_id}"
        if parse_error == "value":
            return None, f"媒介No.に数字以外が含まれています: {contract_id}"
        if parse_error == "month":
            return None, f"月が不正です（1-12）: {contract_id}"
        if parse_error == "year":
            return None, f"和暦の年が不正です: {contract_id}"
        return None, f"媒介No.の形式が不正です: {contract_id}"

    year, month, _ = components
    year_month = f"{year}_{month:02d}"
    return year_month, None


def get_file_for_purchase_date(date_str):
    """買取日から保存先ファイル名を決定する"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        dt = datetime.now()
    return f"{dt.year}_{dt.month:02d}.json"


# ------------------------------------------------------------
# 月次進捗計算
# ------------------------------------------------------------
def build_monthly_progress():
    monthly = {}

    def ensure_month(month_key):
        return monthly.setdefault(
            month_key,
            {"signed": 0, "canceled": 0, "net": 0, "staff": {}},
        )

    for contract in load_all_contracts():
        staff = contract.get("担当") or "未設定"

        signed_month = month_key_from_date(contract.get("新規媒介締結日") or contract.get("ステータス日付"))
        if signed_month:
            entry = ensure_month(signed_month)
            entry["signed"] += 1
            entry["net"] += 1
            staff_entry = entry["staff"].setdefault(staff, {"signed": 0, "canceled": 0, "net": 0})
            staff_entry["signed"] += 1
            staff_entry["net"] += 1

        cancel_month = None
        cancel_info = contract.get("中止理由") if isinstance(contract.get("中止理由"), dict) else None
        if isinstance(cancel_info, dict):
            cancel_month = month_key_from_date(cancel_info.get("中止日"))
        if not cancel_month and contract.get("取引状況") == "中止":
            cancel_month = month_key_from_date(contract.get("ステータス日付"))

        if cancel_month:
            entry = ensure_month(cancel_month)
            entry["canceled"] += 1
            entry["net"] -= 1
            staff_entry = entry["staff"].setdefault(staff, {"signed": 0, "canceled": 0, "net": 0})
            staff_entry["canceled"] += 1
            staff_entry["net"] -= 1

    return monthly


def build_yearly_progress(monthly_progress, goals):
    yearly = {}
    all_month_keys = set(monthly_progress.keys()) | set((goals.get("monthly") or {}).keys())
    for year_key in (goals.get("annual") or {}).keys():
        if year_key and "-" not in str(year_key):
            yearly.setdefault(
                str(year_key),
                {
                    "months": [],
                    "goal": {"storeTarget": 0, "staffTargets": {}, "includeStaff": []},
                    "monthlyTargetTotal": 0,
                    "progress": {"signed": 0, "canceled": 0, "net": 0, "staff": {}},
                },
            )

    for month_key in all_month_keys:
        if not month_key or "-" not in month_key:
            continue
        year = month_key.split("-", 1)[0]
        entry = yearly.setdefault(
            year,
            {
                "months": [],
                "goal": {"storeTarget": 0, "staffTargets": {}, "includeStaff": []},
                "monthlyTargetTotal": 0,
                "progress": {"signed": 0, "canceled": 0, "net": 0, "staff": {}},
            },
        )
        if month_key not in entry["months"]:
            entry["months"].append(month_key)

        month_goal = get_goal_for_month(month_key, goals)
        month_progress = monthly_progress.get(
            month_key, {"signed": 0, "canceled": 0, "net": 0, "staff": {}}
        )

        entry["monthlyTargetTotal"] += month_goal.get("storeTarget", 0)
        entry["goal"]["storeTarget"] += month_goal.get("storeTarget", 0)
        for name, target in (month_goal.get("staffTargets") or {}).items():
            entry["goal"]["staffTargets"][name] = entry["goal"]["staffTargets"].get(name, 0) + target
        entry["goal"]["includeStaff"] = sorted(
            set(entry["goal"].get("includeStaff", [])) | set(month_goal.get("includeStaff", []))
        )

        entry["progress"]["signed"] += month_progress.get("signed", 0)
        entry["progress"]["canceled"] += month_progress.get("canceled", 0)
        entry["progress"]["net"] += month_progress.get("net", 0)
        for name, staff_data in (month_progress.get("staff") or {}).items():
            staff_entry = entry["progress"]["staff"].setdefault(
                name, {"signed": 0, "canceled": 0, "net": 0}
            )
            staff_entry["signed"] += staff_data.get("signed", 0)
            staff_entry["canceled"] += staff_data.get("canceled", 0)
            staff_entry["net"] += staff_data.get("net", 0)

    for info in yearly.values():
        info["months"].sort()

    for year, info in yearly.items():
        annual_goal = get_goal_for_year(year, goals, fallback_to_default=False)
        if annual_goal:
            info["goal"] = annual_goal

    return yearly


# ------------------------------------------------------------
# 認証デコレータ
# ------------------------------------------------------------
def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return jsonify({"error": "認証が必要です"}), 401
        return func(*args, **kwargs)
    return wrapper


# ------------------------------------------------------------
# 認証系 API
# ------------------------------------------------------------
@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json() or {}
    user_id = data.get("id", "")
    password = data.get("password", "")
    if not user_id or not password:
        return jsonify({"error": "IDとパスワードを入力してください"}), 400

    user = USERS.get(user_id)
    if user and user["password"] == password:
        session.permanent = True
        session["logged_in"] = True
        session["user_id"] = user["display_name"]
        session["login_id"] = user_id
        session["is_admin"] = user.get("is_admin", False)
        session["login_at"] = datetime.utcnow().isoformat()
        return jsonify({
            "ok": True,
            "user_id": user["display_name"],
            "login_id": user_id,
            "is_admin": user.get("is_admin", False)
        })
    return jsonify({"error": "IDまたはパスワードが正しくありません"}), 401


@app.route("/api/logout", methods=["POST"])
@login_required
def api_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/check-auth", methods=["GET"])
def api_check_auth():
    if session.get("logged_in"):
        return jsonify({
            "authenticated": True,
            "user_id": session.get("user_id"),
            "login_id": session.get("login_id"),
            "is_admin": session.get("is_admin", False)
        })
    return jsonify({"authenticated": False}), 401


@app.route("/api/users", methods=["GET"])
@login_required
def api_get_users():
    users_list = []
    for login_id, info in USERS.items():
        users_list.append({
            "login_id": login_id,
            "display_name": info["display_name"],
            "is_admin": info.get("is_admin", False)
        })
    return jsonify(users_list)


# ------------------------------------------------------------
# マスター API
# ------------------------------------------------------------
@app.route("/api/masters", methods=["GET"])
@login_required
def api_get_masters():
    return jsonify(load_masters())


@app.route("/api/masters", methods=["PUT"])
@login_required
def api_update_masters():
    payload = request.get_json() or {}
    save_masters(payload)
    return jsonify({"ok": True})


@app.route("/api/status-colors", methods=["GET"])
@login_required
def api_get_status_colors():
    return jsonify(load_status_colors())


@app.route("/api/status-colors", methods=["PUT"])
@login_required
def api_update_status_colors():
    payload = request.get_json() or {}
    if not isinstance(payload, dict):
        return jsonify({"error": "不正な形式です"}), 400
    cleaned = {}
    for name, colors in payload.items():
        if not isinstance(colors, dict):
            continue
        bg = colors.get("bg")
        color = colors.get("color")
        if not isinstance(bg, str) or not isinstance(color, str):
            continue
        cleaned[name] = {"bg": bg, "color": color}
    save_status_colors(cleaned)
    return jsonify({"ok": True, "colors": cleaned})


# ------------------------------------------------------------
# ロック API
# ------------------------------------------------------------
@app.route("/api/locks", methods=["GET"])
@login_required
def api_list_locks():
    return jsonify(get_all_locks())


@app.route("/api/locks/<contract_id>", methods=["DELETE"])
@login_required
def api_delete_lock(contract_id):
    release_lock("contract", contract_id)
    return jsonify({"ok": True})


# ------------------------------------------------------------
# 契約データ API
# ------------------------------------------------------------
def filter_active_status(status):
    return status not in ("成約", "中止", "買取")


def sort_key_contract_id(contract):
    contract_id = contract.get("id", "")
    parsed, _ = parse_contract_id_components(contract_id) if contract_id else (None, None)
    if parsed:
        return parsed
    return (9999, 99, 9999)


@app.route("/api/contracts/active", methods=["GET"])
@login_required
def api_contracts_active():
    result = []
    for contract in load_all_contracts():
        status = contract.get("取引状況")
        if status is None or filter_active_status(status):
            result.append(contract)
    result.sort(key=sort_key_contract_id)
    return jsonify(result)


@app.route("/api/contracts/closed", methods=["GET"])
@login_required
def api_contracts_closed():
    closed = []
    for contract in load_all_contracts():
        if contract.get("取引状況") in ("成約", "中止", "買取"):
            closed.append(contract)
    closed.sort(key=sort_key_contract_id)
    return jsonify(closed)


@app.route("/api/contracts/<contract_id>", methods=["GET"])
@login_required
def api_get_contract(contract_id):
    contract = find_contract(contract_id)
    if not contract:
        return jsonify({"error": "契約が見つかりません"}), 404
    return jsonify(contract)


@app.route("/api/contracts", methods=["POST"])
@login_required
def api_create_contract():
    payload = request.get_json() or {}
    if not payload.get("id"):
        return jsonify({"error": "媒介No.を入力してください"}), 400
    if duplicate_exists(payload["id"]):
        return jsonify({"error": "この媒介No.は既に使用されています"}), 400

    year_month, error = parse_contract_id(payload["id"])
    if error:
        return jsonify({"error": error}), 400

    now_iso = datetime.now().isoformat()

    payload["source_file"] = f"{year_month}.json"
    payload.setdefault("作成日時", now_iso)
    payload["更新日時"] = now_iso
    payload.setdefault("ステータス日付", payload.get("新規媒介締結日") or now_iso.split("T")[0])

    save_contract(payload, year_month)
    return jsonify({"ok": True, "contract": payload}), 201


@app.route("/api/purchases", methods=["POST"])
@login_required
def api_create_purchase():
    body = request.get_json() or {}
    contract_id = body.get("id", "") or ""
    if contract_id and duplicate_exists(contract_id):
        return jsonify({"error": "この媒介No.は既に使用されています"}), 400

    purchase_date = body.get("purchaseDate") or datetime.now().strftime("%Y-%m-%d")
    source_file = get_file_for_purchase_date(purchase_date)
    year_month = source_file.replace(".json", "")
    now_iso = datetime.now().isoformat()

    price = body.get("price")
    try:
        price = None if price in (None, "", []) else float(price)
    except Exception:
        return jsonify({"error": "買取価格が不正です"}), 400

    record = {
        "id": contract_id,
        "担当": body.get("staff") or "",
        "種別": body.get("type") or "買取",
        "取引状況": "買取",
        "ステータス日付": purchase_date,
        "新規媒介締結日": "",
        "媒介期日": "",
        "物件種別": body.get("propertyType") or "",
        "物件所在地": body.get("address") or "",
        "売主": "",
        "売主住所": "",
        "売主連絡先": "",
        "現在の媒介価格": price,
        "価格推移": [],
        "反響媒体": "",
        "備考": body.get("memo") or "",
        "現況": "",
        "鍵の場所": "",
        "キーボックス番号": "",
        "申込日": "",
        "レインズ登録フラグ": False,
        "レインズ満了日": "",
        "レインズ変更済み": False,
        "レインズ変更日": "",
        "成約情報": None,
        "買取情報": {"買取日": purchase_date, "買取価格": price},
        "中止理由": None,
        "source_file": source_file,
        "作成日時": now_iso,
        "更新日時": now_iso,
    }

    save_contract(record, year_month)
    return jsonify({"ok": True, "contract": record}), 201


@app.route("/api/contracts/<contract_id>", methods=["PUT"])
@login_required
def api_update_contract(contract_id):
    payload = request.get_json() or {}
    new_id = payload.get("id", contract_id)

    if duplicate_exists(new_id, exclude_id=contract_id):
        return jsonify({"error": "この媒介No.は既に使用されています"}), 400

    contract = find_contract(contract_id)
    if not contract:
        return jsonify({"error": "契約が見つかりません"}), 404

    now_iso = datetime.now().isoformat()
    user = session.get("user_id") or "unknown"

    # 変更履歴を記録
    changes = []
    old_status = contract.get("取引状況")
    new_status = payload.get("取引状況")
    old_price = contract.get("現在の媒介価格")
    new_price = payload.get("現在の媒介価格")

    if old_status != new_status and new_status:
        changes.append({
            "type": "status",
            "from": old_status,
            "to": new_status,
            "date": now_iso,
            "user": user
        })

    if old_price != new_price and new_price is not None:
        changes.append({
            "type": "price",
            "from": old_price,
            "to": new_price,
            "date": now_iso,
            "user": user
        })

    change_history = contract.get("変更履歴") or []
    change_history.extend(changes)
    payload["変更履歴"] = change_history

    payload.setdefault("作成日時", contract.get("作成日時"))
    payload["更新日時"] = now_iso
    payload["更新者"] = user

    # year_month を取得
    year_month, _ = parse_contract_id(new_id)
    if not year_month:
        year_month, _ = parse_contract_id(contract_id)

    payload["source_file"] = f"{year_month}.json" if year_month else contract.get("source_file", "")

    # 古いIDと新しいIDが異なる場合、古いレコードを削除
    if contract_id != new_id:
        delete_contract(contract_id)

    save_contract(payload, year_month)
    return jsonify({"ok": True, "contract": payload})


@app.route("/api/contracts/<contract_id>", methods=["DELETE"])
@login_required
def api_delete_contract(contract_id):
    contract = find_contract(contract_id)
    if not contract:
        return jsonify({"error": "契約が見つかりません"}), 404

    delete_contract(contract_id)
    release_lock("contract", contract_id)
    return jsonify({"ok": True})


@app.route("/api/contracts/<contract_id>/lock", methods=["POST"])
@login_required
def api_lock_contract(contract_id):
    data = request.get_json() or {}
    user = data.get("user") or session.get("user_id") or "unknown"
    available, info = check_lock_available("contract", contract_id, user)
    if not available:
        return jsonify({"locked": True, "by": info["user"], "expires_at": info["expires_at"]}), 423
    return jsonify({"locked": True, "by": user, "expires_at": info["expires_at"]})


@app.route("/api/contracts/<contract_id>/unlock", methods=["POST"])
@login_required
def api_unlock_contract(contract_id):
    release_lock("contract", contract_id)
    return jsonify({"ok": True})


# ------------------------------------------------------------
# 通知 API
# ------------------------------------------------------------
@app.route("/api/notifications", methods=["GET"])
@login_required
def api_notifications():
    user = session.get("user_id") or "unknown"
    today = datetime.now().date()
    notifications = []

    for contract in load_all_contracts():
        contract_id = contract.get("id", "")
        address = contract.get("物件所在地", "")
        status = contract.get("取引状況", "")

        if status in ("成約", "中止", "買取"):
            continue

        expire_date_str = contract.get("媒介期日")
        if expire_date_str:
            try:
                expire_date = datetime.strptime(expire_date_str, "%Y-%m-%d").date()
                days_left = (expire_date - today).days
                if 0 <= days_left <= 20:
                    notifications.append({
                        "id": f"deadline_{contract_id}_{today.isoformat()}",
                        "type": "deadline",
                        "contract_id": contract_id,
                        "address": address,
                        "days_left": days_left,
                        "expire_date": expire_date_str,
                        "date": today.isoformat(),
                        "message": f"【期限】{contract_id} の媒介期限が{days_left}日後です" if days_left > 0 else f"【期限】{contract_id} の媒介期限は本日です"
                    })
                elif days_left < 0:
                    notifications.append({
                        "id": f"deadline_{contract_id}_{today.isoformat()}",
                        "type": "deadline_expired",
                        "contract_id": contract_id,
                        "address": address,
                        "days_left": days_left,
                        "expire_date": expire_date_str,
                        "date": today.isoformat(),
                        "message": f"【期限切れ】{contract_id} の媒介期限が{abs(days_left)}日過ぎています"
                    })
            except ValueError:
                pass

        change_history = contract.get("変更履歴") or []
        for change in change_history:
            change_user = change.get("user", "")
            change_date = change.get("date", "")[:10]
            change_type = change.get("type", "")

            if change_user != user:
                if change_type == "status":
                    notifications.append({
                        "id": f"status_{contract_id}_{change_date}_{change.get('to')}",
                        "type": "status_change",
                        "contract_id": contract_id,
                        "address": address,
                        "from": change.get("from"),
                        "to": change.get("to"),
                        "user": change_user,
                        "date": change_date,
                        "message": f"【ステータス変更】{contract_id} が「{change.get('to')}」に変更されました（{change_user}）"
                    })
                elif change_type == "price":
                    notifications.append({
                        "id": f"price_{contract_id}_{change_date}_{change.get('to')}",
                        "type": "price_change",
                        "contract_id": contract_id,
                        "address": address,
                        "from": change.get("from"),
                        "to": change.get("to"),
                        "user": change_user,
                        "date": change_date,
                        "message": f"【価格変更】{contract_id} の価格が {change.get('to')}万円 に変更されました（{change_user}）"
                    })

    notifications.sort(key=lambda x: x.get("date", ""), reverse=True)
    return jsonify(notifications)


# ------------------------------------------------------------
# サマリー API
# ------------------------------------------------------------
@app.route("/api/summary", methods=["GET"])
@login_required
def api_summary():
    summary = {}
    for contract in load_all_contracts():
        if contract.get("取引状況") in ("成約", "中止", "買取"):
            continue
        staff = contract.get("担当") or "未設定"
        type_name = (contract.get("種別") or "未設定").strip()
        if type_name in ("専属専任", "専属専任媒介"):
            type_name = "専属"
        summary.setdefault(staff, {"専属": 0, "専任": 0, "一般": 0, "total": 0})
        if type_name not in summary[staff]:
            summary[staff][type_name] = 0
        summary[staff][type_name] += 1
        summary[staff]["total"] += 1

    data = []
    for staff, counts in summary.items():
        data.append({
            "担当": staff,
            "専属": counts.get("専属", 0),
            "専任": counts.get("専任", 0),
            "一般": counts.get("一般", 0),
            "total": counts.get("total", 0),
        })
    return jsonify(data)


# ------------------------------------------------------------
# 目標 API
# ------------------------------------------------------------
@app.route("/api/goals", methods=["GET", "PUT"])
@login_required
def api_goals():
    month_key = normalize_month_key(request.args.get("month"))
    year_key = request.args.get("year")
    if year_key is not None:
        year_key = str(year_key)
        if not re.match(r"^\d{4}$", year_key):
            year_key = None

    if request.method == "GET":
        if not month_key:
            month_key = current_month_key()

        goals = load_goals_data()
        goal = get_goal_for_month(month_key, goals)
        annual_goal = get_goal_for_year(year_key or month_key.split("-", 1)[0], goals)
        response = {
            "month": month_key,
            "goal": goal,
            "monthly": goals.get("monthly", {}),
            "default": goals.get("default", DEFAULT_GOAL),
            "annual": goals.get("annual", {}),
            "annualGoal": annual_goal,
        }
        response.update(goal)
        return jsonify(response)

    body = request.get_json() or {}
    goal_body = {
        "storeTarget": body.get("storeTarget"),
        "staffTargets": body.get("staffTargets"),
        "includeStaff": body.get("includeStaff"),
    }
    if body.get("year") and not body.get("month"):
        year_key = str(body.get("year"))
        if not re.match(r"^\d{4}$", year_key):
            return jsonify({"error": "yearはYYYY形式で指定してください"}), 400
        goals = save_goal_for_year(year_key, goal_body)
        saved_goal = get_goal_for_year(year_key, goals)
        response = {
            "year": year_key,
            "annualGoal": saved_goal,
            "annual": goals.get("annual", {}),
            "monthly": goals.get("monthly", {}),
            "default": goals.get("default", DEFAULT_GOAL),
        }
        response.update(saved_goal)
        return jsonify(response)

    month_key = normalize_month_key(body.get("month")) or month_key or current_month_key()
    if not month_key:
        return jsonify({"error": "monthはYYYY-MM形式で指定してください"}), 400

    goals = save_goal_for_month(month_key, goal_body)
    saved_goal = get_goal_for_month(month_key, goals)
    response = {
        "month": month_key,
        "goal": saved_goal,
        "monthly": goals.get("monthly", {}),
        "annual": goals.get("annual", {}),
        "default": goals.get("default", DEFAULT_GOAL),
    }
    response.update(saved_goal)
    return jsonify(response)


# ------------------------------------------------------------
# 売上 API
# ------------------------------------------------------------
@app.route("/api/sales", methods=["GET", "PUT"])
@login_required
def api_sales():
    month_key = normalize_month_key(request.args.get("month"))
    year_key = request.args.get("year")
    if year_key is not None:
        year_key = str(year_key)
        if not re.match(r"^\d{4}$", year_key):
            year_key = None

    if request.method == "GET":
        if not month_key:
            month_key = current_month_key()
        sales = load_sales_data()
        month_sales = get_sales_for_month(month_key, sales)
        annual_sales = get_sales_for_year(year_key or month_key.split("-", 1)[0], sales)
        response = {
            "month": month_key,
            "monthSales": month_sales,
            "annualSales": annual_sales,
            "monthly": sales.get("monthly", {}),
            "annual": sales.get("annual", {}),
            "default": sales.get("default", {"store": 0, "staff": {}}),
        }
        return jsonify(response)

    body = request.get_json() or {}
    if body.get("year") and not body.get("month"):
        year_key = str(body.get("year"))
        if not re.match(r"^\d{4}$", year_key):
            return jsonify({"error": "yearはYYYY形式で指定してください"}), 400
        sales = save_sales_for_year(year_key, {"store": body.get("store"), "staff": body.get("staff")})
        saved = get_sales_for_year(year_key, sales)
        return jsonify({
            "year": year_key,
            "annualSales": saved,
            "annual": sales.get("annual", {}),
            "monthly": sales.get("monthly", {}),
            "default": sales.get("default", {"store": 0, "staff": {}}),
        })

    month_key = normalize_month_key(body.get("month")) or month_key or current_month_key()
    if not month_key:
        return jsonify({"error": "monthはYYYY-MM形式で指定してください"}), 400
    sales = save_sales_for_month(month_key, {"store": body.get("store"), "staff": body.get("staff")})
    saved = get_sales_for_month(month_key, sales)
    return jsonify({
        "month": month_key,
        "monthSales": saved,
        "monthly": sales.get("monthly", {}),
        "annual": sales.get("annual", {}),
        "default": sales.get("default", {"store": 0, "staff": {}}),
    })


# ------------------------------------------------------------
# 進捗 API
# ------------------------------------------------------------
@app.route("/api/goals/progress", methods=["GET"])
@login_required
def api_goal_progress():
    year_filter = request.args.get("year")
    if year_filter and not re.match(r"^\d{4}$", str(year_filter)):
        return jsonify({"error": "yearはYYYY形式で指定してください"}), 400

    month_filter = normalize_month_key(request.args.get("month"))

    goals = load_goals_data()
    monthly_progress = build_monthly_progress()
    all_month_keys = set(monthly_progress.keys()) | set((goals.get("monthly") or {}).keys())

    monthly_response = {}
    for month_key in sorted(all_month_keys):
        if month_filter and month_key != month_filter:
            continue
        if year_filter and not month_key.startswith(f"{year_filter}-"):
            continue
        monthly_response[month_key] = {
            "goal": get_goal_for_month(month_key, goals),
            "progress": monthly_progress.get(month_key, {"signed": 0, "canceled": 0, "net": 0, "staff": {}}),
        }

    yearly_response = build_yearly_progress(monthly_progress, goals)
    if year_filter:
        yearly_response = {year: data for year, data in yearly_response.items() if year == str(year_filter)}

    return jsonify({
        "currentMonth": current_month_key(),
        "monthly": monthly_response,
        "yearly": yearly_response,
        "annualGoals": goals.get("annual", {}),
    })


# ------------------------------------------------------------
# バックアップ API（Supabase版では空実装）
# ------------------------------------------------------------
@app.route("/api/backup/run", methods=["POST"])
@login_required
def api_backup_run():
    # Supabase版ではバックアップはSupabaseのバックアップ機能を使用
    return jsonify({"ok": True, "message": "Supabase版ではSupabaseのバックアップ機能を使用してください"})


@app.route("/api/backup/list", methods=["GET"])
@login_required
def api_backup_list():
    return jsonify([])


# ------------------------------------------------------------
# 顧客管理 API
# ------------------------------------------------------------
@app.route("/api/customer-masters", methods=["GET"])
@login_required
def api_get_customer_masters():
    return jsonify(load_customer_masters())


@app.route("/api/customer-masters", methods=["PUT"])
@login_required
def api_update_customer_masters():
    payload = request.get_json() or {}
    if not isinstance(payload, dict):
        return jsonify({"error": "不正な形式です"}), 400

    current = load_customer_masters()

    master_keys = ["inquiry_source_sell", "inquiry_source_buy", "property_type", "staff", "status_sell", "status_buy", "contact_method", "progress_status"]

    for key in master_keys:
        if key in payload:
            if isinstance(payload[key], list):
                current[key] = [str(item).strip() for item in payload[key] if str(item).strip()]
            elif isinstance(payload[key], str):
                current[key] = [item.strip() for item in payload[key].split(",") if item.strip()]

    if "status_colors" in payload and isinstance(payload["status_colors"], dict):
        current["status_colors"] = payload["status_colors"]

    save_customer_masters(current)
    return jsonify({"ok": True, "masters": current})


@app.route("/api/customers/years", methods=["GET"])
@login_required
def api_customer_years():
    years = set()
    current_year = datetime.now().year
    years.add(current_year)
    for i in range(3):
        years.add(current_year - i)

    # DBから年度を取得
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT year FROM customers ORDER BY year DESC")
            rows = cur.fetchall()
            for row in rows:
                years.add(row["year"])

    return jsonify(sorted(years, reverse=True))


@app.route("/api/customers/<category>/<int:year>", methods=["GET"])
@login_required
def api_get_customers(category, year):
    if category not in ("sell", "buy", "investment"):
        return jsonify({"error": "無効なカテゴリです"}), 400

    data = load_customers(category, year)
    customers = data.get("customers", [])

    # フィルター処理
    staff = request.args.get("staff")
    status = request.args.get("status")
    keyword = request.args.get("keyword")
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")

    if staff:
        customers = [c for c in customers if c.get("staff_id") == staff]
    if status:
        customers = [c for c in customers if c.get("status") == status]
    if keyword:
        kw = keyword.lower()
        customers = [c for c in customers if
            kw in (c.get("customer_name") or "").lower() or
            kw in (c.get("assessment_address") or "").lower() or
            kw in (c.get("target_property") or "").lower() or
            kw in (c.get("desired_property") or "").lower() or
            kw in (c.get("current_address") or "").lower() or
            kw in (c.get("case_number") or "").lower() or
            kw in (c.get("phone") or "").lower()
        ]
    if date_from:
        customers = [c for c in customers if (c.get("inquiry_date") or "") >= date_from]
    if date_to:
        customers = [c for c in customers if (c.get("inquiry_date") or "") <= date_to]

    def case_sort_key(c):
        cn = c.get("case_number") or ""
        m = re.search(r"(\d{4})$", cn)
        seq = int(m.group(1)) if m else 0
        return (seq, cn)

    customers.sort(key=case_sort_key, reverse=True)

    return jsonify({
        "meta": data.get("meta", {}),
        "customers": customers,
        "total": len(customers)
    })


@app.route("/api/customers/<category>/<int:year>/<customer_id>", methods=["GET"])
@login_required
def api_get_customer(category, year, customer_id):
    if category not in ("sell", "buy", "investment"):
        return jsonify({"error": "無効なカテゴリです"}), 400

    customer = get_customer_by_id(category, year, customer_id)
    if not customer:
        return jsonify({"error": "顧客が見つかりません"}), 404

    return jsonify(customer)


@app.route("/api/customers/<category>/<int:year>", methods=["POST"])
@login_required
def api_create_customer(category, year):
    if category not in ("sell", "buy", "investment"):
        return jsonify({"error": "無効なカテゴリです"}), 400

    payload = request.get_json() or {}

    required = ["inquiry_date", "customer_name"]
    for field in required:
        if not payload.get(field):
            return jsonify({"error": f"{field}は必須です"}), 400

    import uuid
    now_iso = datetime.now().isoformat()

    data = load_customers(category, year)
    existing_customers = data.get("customers", [])
    inquiry_date = payload.get("inquiry_date")
    case_number = payload.get("case_number") or generate_case_number_for_date(category, year, inquiry_date, existing_customers)

    customer = {
        "id": str(uuid.uuid4()),
        "case_number": case_number,
        "status": payload.get("status") or "未対応",
        "staff_id": payload.get("staff_id") or session.get("user_id") or "",
        "inquiry_date": payload.get("inquiry_date"),
        "inquiry_source": payload.get("inquiry_source"),
        "contact_method": payload.get("contact_method") or "",
        "property_type": payload.get("property_type"),
        "customer_name": payload.get("customer_name"),
        "phone": payload.get("phone") or "",
        "current_address": payload.get("current_address") or "",
        "email": payload.get("email") or "",
        "memo": payload.get("memo") or "",
        "first_call": payload.get("first_call") or "未",
        "year": year,
        "created_at": now_iso,
        "updated_at": now_iso
    }

    if category == "sell":
        customer["assessment_address"] = payload.get("assessment_address") or ""
        customer["call_status"] = payload.get("call_status") or "未"
        customer["mail_status"] = payload.get("mail_status") or "未"
        customer["sms_status"] = payload.get("sms_status") or "未"
        customer["pre_assessment"] = payload.get("pre_assessment") or "未"
        customer["visit_status"] = payload.get("visit_status") or "未"
        customer["mediation"] = payload.get("mediation") or "未"
        customer["contract"] = payload.get("contract") or "未"
    elif category == "buy":
        customer["target_property"] = payload.get("target_property") or ""
        customer["call_status"] = payload.get("call_status") or "未"
        customer["mail_status"] = payload.get("mail_status") or "未"
        customer["showing_status"] = payload.get("showing_status") or "未"
        customer["contract"] = payload.get("contract") or "未"
    elif category == "investment":
        customer["desired_property"] = payload.get("desired_property") or ""
        customer["call_status"] = payload.get("call_status") or "未"
        customer["mail_status"] = payload.get("mail_status") or "未"
        customer["showing_status"] = payload.get("showing_status") or "未"
        customer["contract"] = payload.get("contract") or "未"
        customer["yield_rate"] = payload.get("yield_rate")
        customer["expected_rent"] = payload.get("expected_rent")
        customer["own_funds"] = payload.get("own_funds")
        customer["loan_amount"] = payload.get("loan_amount")
        customer["desired_area"] = payload.get("desired_area") or ""

    save_customer(category, year, customer)
    return jsonify({"ok": True, "customer": customer}), 201


@app.route("/api/customers/<category>/<int:year>/<customer_id>", methods=["PUT"])
@login_required
def api_update_customer(category, year, customer_id):
    if category not in ("sell", "buy", "investment"):
        return jsonify({"error": "無効なカテゴリです"}), 400

    payload = request.get_json() or {}

    customer = get_customer_by_id(category, year, customer_id)
    if not customer:
        return jsonify({"error": "顧客が見つかりません"}), 404

    for key, value in payload.items():
        if key not in ("id", "created_at"):
            customer[key] = value
    customer["updated_at"] = datetime.now().isoformat()

    save_customer(category, year, customer)
    return jsonify({"ok": True, "customer": customer})


@app.route("/api/customers/<category>/<int:year>/<customer_id>", methods=["DELETE"])
@login_required
def api_delete_customer(category, year, customer_id):
    if category not in ("sell", "buy", "investment"):
        return jsonify({"error": "無効なカテゴリです"}), 400

    customer = get_customer_by_id(category, year, customer_id)
    if not customer:
        return jsonify({"error": "顧客が見つかりません"}), 404

    delete_customer(customer_id)
    reassign_case_numbers(category, year)
    return jsonify({"ok": True})


@app.route("/api/customers/reassign/<category>/<int:year>", methods=["POST"])
@login_required
def api_reassign_case_numbers(category, year):
    if category not in ("sell", "buy", "investment"):
        return jsonify({"error": "無効なカテゴリです"}), 400

    count = reassign_case_numbers(category, year)
    return jsonify({"ok": True, "reassigned_count": count})


@app.route("/api/customers/case-number/<category>/<int:year>", methods=["POST"])
@login_required
def api_generate_case_number(category, year):
    if category not in ("sell", "buy", "investment"):
        return jsonify({"error": "無効なカテゴリです"}), 400

    data = load_customers(category, year)
    existing_customers = data.get("customers", [])
    inquiry_date = datetime.now().strftime("%Y-%m-%d")
    case_number = generate_case_number_for_date(category, year, inquiry_date, existing_customers)
    return jsonify({"case_number": case_number})


@app.route("/api/customers/<category>/<int:year>/export", methods=["GET"])
@login_required
def api_export_customers(category, year):
    if category not in ("sell", "buy", "investment"):
        return jsonify({"error": "無効なカテゴリです"}), 400

    data = load_customers(category, year)
    customers = data.get("customers", [])

    if not customers:
        return jsonify({"error": "データがありません"}), 404

    output = io.StringIO()

    if category == "sell":
        headers = ["案件番号", "ステータス", "担当者", "反響日", "反響媒体", "連絡方法",
                   "物件種別", "査定住所", "氏名", "電話番号", "現住所", "メール",
                   "電話", "メール", "SMS", "査定前", "訪問", "媒介", "契約", "メモ"]
    elif category == "buy":
        headers = ["案件番号", "ステータス", "担当者", "反響日", "反響媒体", "連絡方法",
                   "物件種別", "反響物件", "氏名", "電話番号", "現住所", "メール",
                   "電話", "メール", "案内", "契約", "メモ"]
    else:
        headers = ["案件番号", "ステータス", "担当者", "反響日", "反響媒体", "連絡方法",
                   "物件種別", "希望物件", "氏名", "電話番号", "現住所", "メール",
                   "電話", "メール", "案内", "契約", "利回り希望", "想定家賃",
                   "自己資金", "融資希望額", "希望エリア", "メモ"]

    writer = csv.writer(output)
    writer.writerow(headers)

    for c in customers:
        if category == "sell":
            row = [
                c.get("case_number"), c.get("status"), c.get("staff_id"),
                c.get("inquiry_date"), c.get("inquiry_source"), c.get("contact_method"),
                c.get("property_type"), c.get("assessment_address"), c.get("customer_name"),
                c.get("phone"), c.get("current_address"), c.get("email"),
                c.get("call_status"), c.get("mail_status"), c.get("sms_status"),
                c.get("pre_assessment"), c.get("visit_status"), c.get("mediation"),
                c.get("contract"), c.get("memo")
            ]
        elif category == "buy":
            row = [
                c.get("case_number"), c.get("status"), c.get("staff_id"),
                c.get("inquiry_date"), c.get("inquiry_source"), c.get("contact_method"),
                c.get("property_type"), c.get("target_property"), c.get("customer_name"),
                c.get("phone"), c.get("current_address"), c.get("email"),
                c.get("call_status"), c.get("mail_status"), c.get("showing_status"),
                c.get("contract"), c.get("memo")
            ]
        else:
            row = [
                c.get("case_number"), c.get("status"), c.get("staff_id"),
                c.get("inquiry_date"), c.get("inquiry_source"), c.get("contact_method"),
                c.get("property_type"), c.get("desired_property"), c.get("customer_name"),
                c.get("phone"), c.get("current_address"), c.get("email"),
                c.get("call_status"), c.get("mail_status"), c.get("showing_status"),
                c.get("contract"), c.get("yield_rate"), c.get("expected_rent"),
                c.get("own_funds"), c.get("loan_amount"), c.get("desired_area"),
                c.get("memo")
            ]
        writer.writerow(row)

    output.seek(0)

    category_names = {"sell": "売り", "buy": "買い", "investment": "収益"}
    filename = f"customers_{category_names[category]}_{year}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ------------------------------------------------------------
# ルーティング（静的ファイル）
# ------------------------------------------------------------
@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_index(path):
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, "index.html")


# ------------------------------------------------------------
# アプリ起動
# ------------------------------------------------------------
if __name__ == "__main__":
    print("Supabase版媒介契約管理システム起動中...")
    app.run(host="0.0.0.0", port=5000, debug=False)
