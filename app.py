import pandas as pd
import streamlit as st
import altair as alt
from datetime import date, timedelta, datetime
import calendar
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, OperationalError

# ==========================================
# 1. 설정 및 DB 연결
# ==========================================
try:
    DB_URL = st.secrets["db_url"]
except:
    # 혹시 로컬에서 테스트할 때를 대비한 예비 코드 (필요하면 주석 처리)
    DB_URL = "postgresql://postgres.btlscfzrlwismefvyfea:Hakata190925@aws-1-ap-northeast-1.pooler.supabase.com:6543/postgres"
# 일본 공휴일 데이터
JAPAN_HOLIDAYS = {
    "2025-01-01": "元日", "2025-01-13": "成人の日", "2025-02-11": "建国記念の日",
    "2025-02-23": "天皇誕生日", "2025-02-24": "振替休日", "2025-03-20": "春分の日",
    "2025-04-29": "昭和の日", "2025-05-03": "憲法記念日", "2025-05-04": "みどりの日",
    "2025-05-05": "こどもの日", "2025-05-06": "振替休日", "2025-07-21": "海の日",
    "2025-08-11": "山の日", "2025-09-15": "敬老の日", "2025-09-23": "秋分の日",
    "2025-10-13": "スポーツの日", "2025-11-03": "文化の日", "2025-11-23": "勤労感謝の日",
    "2025-11-24": "振替休日",
    "2026-01-01": "元日", "2026-01-12": "成人の日", "2026-02-11": "建国記念の日",
}

# 언어 설정 (일본어 고정)
TEXTS = {
    "jp": {
        "title": "ホテル在庫予測システム", "menu_title": "メニュー", "menu_home": "🏠 ホーム・サマリー",
        "menu_items": "📦 1. 品目マスター", "menu_stock": "📝 2. 在庫記録", "menu_forecast": "📊 3. 予測＆発注",
        "menu_calendar": "📅 4. 発注カレンダー",
        "dashboard_alert": "発注推奨品目数", "dashboard_incoming": "入荷待ち件数", "dashboard_total_items": "登録品目数",
        "items_header": "品目マスター管理", "items_new": "新規登録", "items_list": "登録済み一覧",
        "item_name": "品目名", "item_cat": "使用エリア", "unit": "単位", "safety": "安全在庫", 
        "upr": "1室あたり使用数", # New!
        "cs_total": "1CS入数", "units_per_box": "1箱入数", "boxes_per_cs": "1CS箱数",
        "btn_register": "登録", "btn_update": "更新", "items_edit": "編集・削除", "select_item_edit": "品目選択",
        "err_itemname": "品目名は必須です。", "success_register": "登録しました。", "success_update": "更新しました。",
        "stock_header": "在庫記録管理", "stock_tab_input": "新規入力", "stock_tab_history": "履歴確認・削除",
        "stock_select_item": "品目選択", "stock_date": "日付", "stock_cs": "CS", "stock_box": "箱/袋", "stock_note": "備考",
        "btn_save_stock": "保存", "success_save_stock": "保存しました。", "recent_stock": "最新在庫状況", "history_list": "最近の入力履歴（削除可能）", 
        "btn_delete": "削除", "select_delete": "削除する記録を選択", "success_delete": "削除しました。", "warn_no_data": "データがありません。",
        "forecast_header": "ハイブリッド在庫予測 (実績 vs 理論)",
        "days_label": "過去実績算出期間(日)", "horizon_label": "予測期間(日)",
        "forecast_result": "発注推奨リスト", "info_forecast": "実際の消費量(Actual)と、稼働率ベースの理論値(Theory)を比較して多い方を採用します。",
        "cal_header": "入荷予定カレンダー", "cal_tab_new": "予定登録", "cal_tab_list": "カレンダー・検索・削除",
        "cal_item": "品目", "cal_order_date": "発注日", "cal_arrival_date": "入荷予定日", "cal_cs": "CS", "cal_box": "箱/袋", "cal_note": "備考",
        "btn_save_cal": "登録", "success_save_cal": "登録しました。", "cal_list": "入荷予定一覧", "cal_search_item": "品目検索",
        "weekdays": ["月", "火", "水", "木", "金", "土", "日"], "prev_month": "◀ 前月", "next_month": "翌月 ▶", "today": "今日",
        "lang": "Language",
        "cat_all": "全客室 (238室)", "cat_std": "Standard (225室)", "cat_hak": "Hakata (13室)"
    }
}

def t(key: str) -> str:
    return TEXTS["jp"].get(key, key)

# ==========================================
# 2. DB 엔진 및 초기화
# ==========================================
@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

def init_db():
    engine = get_engine()
    with engine.connect() as conn:
        # items 테이블 생성 (units_per_room 컬럼 추가)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                target_area TEXT DEFAULT 'ALL',
                unit TEXT,
                units_per_room FLOAT DEFAULT 0.0,
                cs_total_units INTEGER DEFAULT 0,
                units_per_box INTEGER DEFAULT 0,
                boxes_per_cs INTEGER DEFAULT 0,
                safety_stock INTEGER DEFAULT 0
            )
        """))
        
        # [자동 마이그레이션] 기존 테이블에 컬럼이 없으면 추가
        try:
            conn.execute(text("ALTER TABLE items ADD COLUMN IF NOT EXISTS target_area TEXT DEFAULT 'ALL'"))
            conn.execute(text("ALTER TABLE items ADD COLUMN IF NOT EXISTS units_per_room FLOAT DEFAULT 0.0"))
            conn.commit()
        except Exception:
            pass

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id SERIAL PRIMARY KEY,
                item_id INTEGER,
                snap_date TEXT,
                qty_cs INTEGER DEFAULT 0,
                qty_box INTEGER DEFAULT 0,
                total_units INTEGER DEFAULT 0,
                note TEXT,
                FOREIGN KEY(item_id) REFERENCES items(id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS deliveries (
                id SERIAL PRIMARY KEY,
                item_id INTEGER,
                order_date TEXT,
                arrival_date TEXT,
                qty_cs INTEGER DEFAULT 0,
                qty_box INTEGER DEFAULT 0,
                total_units INTEGER DEFAULT 0,
                note TEXT,
                FOREIGN KEY(item_id) REFERENCES items(id)
            )
        """))
        conn.commit()

# ==========================================
# 3. 데이터 쿼리 함수
# ==========================================
def run_query(query, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        query_str = query.strip().upper()
        if query_str.startswith("SELECT") or query_str.startswith("WITH"):
            return pd.read_sql(text(query), conn, params=params)
        else:
            conn.execute(text(query), params or {})
            conn.commit()

def force_numeric(df, cols):
    if df is None or df.empty: return df
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    return df

def safe_display(df):
    if df is None or df.empty: return pd.DataFrame()
    d = df.copy()
    for col in d.columns:
        d[col] = d[col].apply(lambda x: str(x) if x is not None else "")
    return d

@st.cache_data(ttl=60)
def get_items_df():
    df = run_query("SELECT * FROM items ORDER BY id")
    return force_numeric(df, ["cs_total_units", "units_per_box", "boxes_per_cs", "safety_stock", "units_per_room"])

def add_item(name, area, upr, unit, cs, upb, bpc, safe):
    sql = """
    INSERT INTO items (name, target_area, units_per_room, unit, cs_total_units, units_per_box, boxes_per_cs, safety_stock)
    VALUES (:name, :area, :upr, :unit, :cs, :upb, :bpc, :safe)
    """
    run_query(sql, {"name": name, "area": area, "upr": upr, "unit": unit, "cs": cs, "upb": upb, "bpc": bpc, "safe": safe})
    get_items_df.clear()

def update_item_logic(iid, name, area, upr, unit, cs, upb, bpc, safe):
    sql = """
    UPDATE items SET name=:name, target_area=:area, units_per_room=:upr, unit=:unit, cs_total_units=:cs, 
    units_per_box=:upb, boxes_per_cs=:bpc, safety_stock=:safe WHERE id=:id
    """
    run_query(sql, {"name": name, "area": area, "upr": upr, "unit": unit, "cs": cs, "upb": upb, "bpc": bpc, "safe": safe, "id": iid})
    get_items_df.clear()

def delete_item_logic(iid):
    s_cnt = int(pd.to_numeric(run_query("SELECT COUNT(*) as cnt FROM snapshots WHERE item_id=:id", {"id": iid})["cnt"]).iloc[0])
    d_cnt = int(pd.to_numeric(run_query("SELECT COUNT(*) as cnt FROM deliveries WHERE item_id=:id", {"id": iid})["cnt"]).iloc[0])
    
    if s_cnt == 0 and d_cnt == 0:
        run_query("DELETE FROM items WHERE id=:id", {"id": iid})
        get_items_df.clear()
        return True, 0, 0
    return False, s_cnt, d_cnt

def add_snapshot(iid, date, qc, qb, tot, note):
    sql = """
    INSERT INTO snapshots (item_id, snap_date, qty_cs, qty_box, total_units, note)
    VALUES (:iid, :dt, :qc, :qb, :tot, :note)
    """
    run_query(sql, {"iid": iid, "dt": date, "qc": qc, "qb": qb, "tot": tot, "note": note})

def delete_snapshot(sid):
    run_query("DELETE FROM snapshots WHERE id=:id", {"id": sid})

def add_delivery(iid, o_date, a_date, qc, qb, tot, note):
    sql = """
    INSERT INTO deliveries (item_id, order_date, arrival_date, qty_cs, qty_box, total_units, note)
    VALUES (:iid, :od, :ad, :qc, :qb, :tot, :note)
    """
    run_query(sql, {"iid": iid, "od": o_date, "ad": a_date, "qc": qc, "qb": qb, "tot": tot, "note": note})

def delete_delivery(did):
    run_query("DELETE FROM deliveries WHERE id=:id", {"id": did})

def get_latest_stock_df():
    sql = """
    WITH LatestSnaps AS (
        SELECT item_id, total_units as current_stock, snap_date as last_snap_date
        FROM snapshots s1
        WHERE snap_date = (SELECT MAX(snap_date) FROM snapshots s2 WHERE s2.item_id = s1.item_id)
    )
    SELECT i.*, COALESCE(ls.current_stock, 0) as current_stock, ls.last_snap_date 
    FROM items i
    LEFT JOIN LatestSnaps ls ON i.id = ls.item_id
    """
    df = run_query(sql)
    return force_numeric(df, ["current_stock", "safety_stock", "cs_total_units", "units_per_box", "boxes_per_cs", "units_per_room"])

def get_snapshot_history():
    sql = """
    SELECT s.*, i.name 
    FROM snapshots s 
    LEFT JOIN items i ON s.item_id = i.id 
    ORDER BY s.snap_date DESC, s.id DESC LIMIT 50
    """
    return run_query(sql)

def get_delivery_list():
    sql = """
    SELECT d.*, i.name as item 
    FROM deliveries d 
    LEFT JOIN items i ON d.item_id = i.id 
    ORDER BY d.arrival_date, d.order_date
    """
    return run_query(sql)

def get_usage_from_snapshots(days=60):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    sql = "SELECT item_id, snap_date, total_units FROM snapshots WHERE snap_date >= :cutoff ORDER BY item_id, snap_date"
    df = run_query(sql, {"cutoff": cutoff})
    if df.empty: return pd.DataFrame(columns=["id", "daily_avg_usage"])
    
    df["total_units"] = pd.to_numeric(df["total_units"], errors='coerce').fillna(0)
    df["snap_date"] = pd.to_datetime(df["snap_date"])
    
    records = []
    for item_id, group in df.groupby("item_id"):
        if len(group) < 2: continue
        daily_usages = []
        for i in range(1, len(group)):
            prev, curr = group.iloc[i-1], group.iloc[i]
            days_diff = (curr["snap_date"] - prev["snap_date"]).days
            if days_diff <= 0: continue
            usage = prev["total_units"] - curr["total_units"]
            if usage <= 0: continue
            daily_usages.append(usage / days_diff)
        if daily_usages:
            avg = sum(daily_usages) / len(daily_usages)
            records.append({"id": item_id, "daily_avg_usage": avg})
    return pd.DataFrame(records)

def get_future_deliveries(horizon_days):
    today = date.today().isoformat()
    end_date = (date.today() + timedelta(days=horizon_days)).isoformat()
    sql = """
    SELECT item_id, SUM(total_units) as incoming_units 
    FROM deliveries 
    WHERE arrival_date > :today AND arrival_date <= :end 
    GROUP BY item_id
    """
    df = run_query(sql, {"today": today, "end": end_date})
    if not df.empty:
        df["incoming_units"] = pd.to_numeric(df["incoming_units"], errors='coerce').fillna(0)
    return df

def get_jp_holiday_name(dt: date):
    return JAPAN_HOLIDAYS.get(dt.isoformat(), None)

# ==========================================
# 4. 페이지 UI
# ==========================================
def page_home():
    st.header(t("menu_home"))
    stock_df = get_latest_stock_df()
    if stock_df is None or stock_df.empty:
        st.info(t("warn_no_data"))
        return

    days, horizon = 60, 30
    usage_df = get_usage_from_snapshots(days)
    
    if not usage_df.empty:
        merged = stock_df.merge(usage_df, on="id", how="left")
    else:
        merged = stock_df.copy()
        merged["daily_avg_usage"] = 0.0
    
    merged["daily_avg_usage"] = pd.to_numeric(merged["daily_avg_usage"], errors='coerce').fillna(0)
    
    incoming_df = get_future_deliveries(horizon)
    if incoming_df is not None and not incoming_df.empty:
        merged = merged.merge(incoming_df, left_on="id", right_on="item_id", how="left")
        merged["incoming_units"] = merged["incoming_units"].fillna(0)
    else:
        merged["incoming_units"] = 0.0
    
    # ==========================================
    # [NEW] 예측 로직: 실제 사용량(Actual) vs 이론 사용량(Theory)
    # ==========================================
    
    # 1. 기본 가동률 설정 (태홍이네 호텔 기준)
    ROOMS_ALL = 238
    ROOMS_STD = 225
    ROOMS_HAK = 13
    
    OCC_ALL = 0.90 # 90%
    OCC_STD = 0.93 # 93%
    OCC_HAK = 0.70 # 70%

    def calculate_theory_daily(row):
        area = row.get("target_area", "ALL")
        upr = float(row.get("units_per_room", 0.0)) # 1방당 몇개?
        
        if upr <= 0: return 0.0 # 설정 안했으면 계산 불가
        
        rooms = ROOMS_ALL
        occ = OCC_ALL
        
        if area == "STD":
            rooms = ROOMS_STD
            occ = OCC_STD
        elif area == "HAK":
            rooms = ROOMS_HAK
            occ = OCC_HAK
            
        # 이론상 하루 소비량 = 방개수 * 가동률 * 방당개수
        return rooms * occ * upr

    merged["theory_daily_usage"] = merged.apply(calculate_theory_daily, axis=1)
    
    # 2. 최종 예측량 결정 (실제 vs 이론 중 더 큰 값 or 상황에 따라 선택)
    # 여기서는 안전하게 '둘 중 더 큰 값'을 기준으로 잡거나, 
    # 실제 데이터가 없으면 이론값을 쓰는 하이브리드 방식 채택
    def decide_forecast_usage(row):
        actual = row["daily_avg_usage"]
        theory = row["theory_daily_usage"]
        
        # 실제 기록이 있으면 그걸 우선하되, 이론값이 터무니없이 크면(도난 등) 이론값 참고 가능
        # 여기선 단순하게: 실제 데이터가 0이면 이론값, 아니면 실제값 사용 (또는 MAX 사용)
        if actual > 0:
            return actual
        else:
            return theory

    merged["final_daily_usage"] = merged.apply(decide_forecast_usage, axis=1)
    merged["forecast"] = merged["final_daily_usage"] * horizon
    
    merged["order_qty"] = (
        merged["forecast"] + merged["safety_stock"]
        - merged["current_stock"] - merged["incoming_units"]
    ).apply(lambda x: x if x > 0 else 0)
    
    urgent = merged[merged["order_qty"] > 0]
    
    c1, c2, c3 = st.columns(3)
    c1.metric(t("dashboard_alert"), f"{len(urgent)}", delta_color="inverse")
    c2.metric(t("dashboard_incoming"), f"{len(get_delivery_list())}")
    c3.metric(t("dashboard_total_items"), f"{len(stock_df)}")
    
    st.divider()
    if not urgent.empty:
        st.subheader("🚨 Urgent Orders (Recommended)")
        # 표시용 데이터 생성
        urgent_display = urgent[["name", "target_area", "current_stock", "daily_avg_usage", "theory_daily_usage", "order_qty", "unit"]].copy()
        
        # 컬럼명 보기 좋게 변경 (일본어)
        urgent_display = urgent_display.rename(columns={
            "daily_avg_usage": "実績消費/日",
            "theory_daily_usage": "理論消費/日"
        })
        
        # 소수점 정리
        urgent_display["実績消費/日"] = urgent_display["実績消費/日"].apply(lambda x: round(x, 1))
        urgent_display["理論消費/日"] = urgent_display["理論消費/日"].apply(lambda x: round(x, 1))
        urgent_display["order_qty"] = urgent_display["order_qty"].apply(lambda x: int(x))

        st.dataframe(safe_display(urgent_display).style.background_gradient(cmap="Reds", subset=["order_qty"]), use_container_width=True)
        st.caption("※ 実績消費: 過去の在庫記録に基づく平均 / 理論消費: 稼働率設定に基づく計算値")
    else:
        st.success("✅ All stocks are safe.")

def page_items():
    st.header(t("items_header"))
    tab1, tab2 = st.tabs([t("items_list"), t("items_new")])
    AREA_OPTS = {"ALL": t("cat_all"), "STD": t("cat_std"), "HAK": t("cat_hak")}
    
    with tab1:
        df = get_items_df()
        if df is not None and not df.empty:
            df_disp = df.copy()
            df_disp["target_area"] = df_disp["target_area"].map(AREA_OPTS).fillna(df_disp["target_area"])
            st.dataframe(safe_display(df_disp), use_container_width=True)
            
            st.divider()
            st.subheader(t("items_edit"))
            opts = [f"{row['name']} (ID:{row['id']})" for _, row in df.iterrows()]
            sel = st.selectbox(t("select_item_edit"), opts)
            if sel:
                iid = int(sel.split("ID:")[1].replace(")", ""))
                row = df[df["id"] == iid].iloc[0]
                with st.form("edit_item"):
                    c1, c2 = st.columns(2)
                    n = c1.text_input(t("item_name"), row["name"])
                    
                    curr_area = row["target_area"] if row["target_area"] in AREA_OPTS else "ALL"
                    area_key = c1.selectbox(t("item_cat"), list(AREA_OPTS.keys()), index=list(AREA_OPTS.keys()).index(curr_area), format_func=lambda x: AREA_OPTS[x])
                    
                    # [NEW] 1실당 사용수 입력
                    upr = c1.number_input(t("upr"), 0.0, value=float(row.get("units_per_room", 0.0)), step=0.1)
                    
                    u = c1.text_input(t("unit"), row["unit"])
                    s = c1.number_input(t("safety"), 0, value=int(row["safety_stock"]))
                    ct = c2.number_input(t("cs_total"), 0, value=int(row["cs_total_units"]))
                    up = c2.number_input(t("units_per_box"), 0, value=int(row["units_per_box"]))
                    bp = c2.number_input(t("boxes_per_cs"), 0, value=int(row["boxes_per_cs"]))
                    
                    if st.form_submit_button(t("btn_update")):
                        update_item_logic(iid, n, area_key, upr, u, ct, up, bp, s)
                        st.toast(t("success_update"), icon="✅")
                        st.rerun()
                
                if st.button(t("btn_delete"), type="primary"):
                    ok, sc, dc = delete_item_logic(iid)
                    if ok:
                        st.toast(t("success_delete"), icon="🗑️")
                        st.rerun()
                    else:
                        st.error(f"Cannot delete. Used in {sc} snapshots, {dc} deliveries.")
        else:
            st.info("No items.")
    with tab2:
        with st.form("new_item"):
            c1, c2 = st.columns(2)
            n = c1.text_input(t("item_name"))
            area_key = c1.selectbox(t("item_cat"), list(AREA_OPTS.keys()), format_func=lambda x: AREA_OPTS[x])
            
            # [NEW] 1실당 사용수 입력
            upr = c1.number_input(t("upr"), 0.0, step=0.1)
            
            u = c1.text_input(t("unit"), "本")
            s = c1.number_input(t("safety"), 0)
            ct = c2.number_input(t("cs_total"), 0)
            up = c2.number_input(t("units_per_box"), 0)
            bp = c2.number_input(t("boxes_per_cs"), 0)
            if st.form_submit_button(t("btn_register")):
                if n:
                    add_item(n, area_key, upr, u, ct, up, bp, s)
                    st.toast(t("success_register"), icon="🎉")
                    st.rerun()
                else:
                    st.error(t("err_itemname"))

def page_stock():
    st.header(t("stock_header"))
    t1, t2 = st.tabs([t("stock_tab_input"), t("stock_tab_history")])
    items = get_items_df()
    
    with t1:
        if items is not None and not items.empty:
            c1, c2 = st.columns([1, 1.5])
            with c1:
                imap = {r["name"]: r["id"] for _, r in items.iterrows()}
                sel = st.selectbox(t("stock_select_item"), list(imap.keys()))
                if sel:
                    iid = imap[sel]
                    row = items[items["id"] == iid].iloc[0]
                    st.caption(f"1CS={row['cs_total_units']}, 1Box={row['units_per_box']}")
                    with st.form("stock_in", clear_on_submit=True):
                        d = st.date_input(t("stock_date"), date.today())
                        cc1, cc2 = st.columns(2)
                        qc = cc1.number_input(t("stock_cs"), 0)
                        qb = cc2.number_input(t("stock_box"), 0)
                        nt = st.text_area(t("stock_note"), height=68)
                        if st.form_submit_button(t("btn_save_stock")):
                            qc = int(qc); qb = int(qb)
                            tot = int(qc * row["cs_total_units"] + qb * row["units_per_box"])
                            add_snapshot(iid, d.isoformat(), qc, qb, tot, nt)
                            st.toast(t("success_save_stock"), icon="💾")
                            st.rerun()
            with c2:
                st.subheader(t("recent_stock"))
                latest = get_latest_stock_df()
                if latest is not None and not latest.empty:
                    st.dataframe(safe_display(latest[["name", "current_stock", "last_snap_date"]]), use_container_width=True)
        else:
            st.info("No items loaded.")
    with t2:
        hist = get_snapshot_history()
        if hist is not None and not hist.empty:
            st.dataframe(safe_display(hist), use_container_width=True)
            st.divider()
            st.subheader(t("btn_delete"))
            opts = [f"ID {r['id']}: {r['snap_date']} - {r['name']}" for _, r in hist.iterrows()]
            s = st.selectbox(t("select_delete"), opts)
            if st.button(t("btn_delete"), key="del_snap", type="primary"):
                if s:
                    sid = int(s.split(":")[0].replace("ID", "").strip())
                    delete_snapshot(sid)
                    st.toast(t("success_delete"), icon="🗑️")
                    st.rerun()

def page_forecast_general():
    st.header(t("forecast_header"))
    stock = get_latest_stock_df()
    if stock is None or stock.empty: return

    with st.expander("⚙️ 稼働率設定 (Occupancy Settings)", expanded=True):
        c1, c2, c3 = st.columns(3)
        # [태홍이 호텔 기준 가동률]
        occ_all = c1.slider("全客室 (Default 90%)", 0, 100, 90) / 100.0
        occ_std = c2.slider("Standard (Default 93%)", 0, 100, 93) / 100.0
        occ_hak = c3.slider("Hakata (Default 70%)", 0, 100, 70) / 100.0
        
        cc1, cc2 = st.columns(2)
        days = cc1.slider(t("days_label"), 7, 120, 60)
        hor = cc2.slider(t("horizon_label"), 7, 120, 30)

    usage = get_usage_from_snapshots(days)
    
    if not usage.empty:
        merged = stock.merge(usage, on="id", how="left").fillna(0)
    else:
        merged = stock.copy()
        merged["daily_avg_usage"] = 0.0
        
    incoming = get_future_deliveries(hor)
    if incoming is not None and not incoming.empty:
        merged = merged.merge(incoming, left_on="id", right_on="item_id", how="left").fillna(0)
    else:
        merged["incoming_units"] = 0.0

    # [핵심 로직] 가동률 기반 시뮬레이션 (Theory Calculation)
    ROOMS_ALL = 238
    ROOMS_STD = 225
    ROOMS_HAK = 13

    def calculate_simulated_daily(row):
        area = row.get("target_area", "ALL")
        upr = float(row.get("units_per_room", 0.0)) # 방당 개수
        
        # 설정 안 했으면 그냥 0 리턴
        if upr <= 0: return 0.0
        
        # 이론 사용량 = 객실수 * 현재설정가동률 * 방당개수
        if area == "STD":
            return ROOMS_STD * occ_std * upr
        elif area == "HAK":
            return ROOMS_HAK * occ_hak * upr
        else:
            return ROOMS_ALL * occ_all * upr

    merged["theory_usage"] = merged.apply(calculate_simulated_daily, axis=1)
    
    # [하이브리드] 실제 vs 이론 중 무엇을 쓸 것인가?
    # -> 실제 데이터가 있으면 실제를 쓰고, 없으면 이론을 쓴다.
    # -> (원한다면 MAX(실제, 이론)으로 바꿀 수도 있음)
    def pick_usage(row):
        actual = float(row["daily_avg_usage"])
        theory = float(row["theory_usage"])
        
        # Case 1: 실제 데이터가 충분하면 실제 데이터가 짱임
        if actual > 0: return actual
        # Case 2: 실제 데이터 없으면(신상품 등) 이론치 사용
        return theory

    merged["final_daily_usage"] = merged.apply(pick_usage, axis=1)
    merged["forecast"] = merged["final_daily_usage"] * hor
    merged["order"] = (merged["forecast"] + merged["safety_stock"] - merged["current_stock"] - merged["incoming_units"]).apply(lambda x: x if x > 0 else 0)
    
    # 컬럼명 정리해서 보여주기
    res_display = merged[["name", "target_area", "current_stock", "daily_avg_usage", "theory_usage", "forecast", "order"]].copy()
    res_display = res_display.rename(columns={
        "daily_avg_usage": "実績/日",
        "theory_usage": "理論/日",
        "forecast": "予測需要"
    }).sort_values("order", ascending=False)
    
    # 소수점 깔끔하게
    res_display["実績/日"] = res_display["実績/日"].apply(lambda x: round(x, 1))
    res_display["理論/日"] = res_display["理論/日"].apply(lambda x: round(x, 1))
    res_display["予測需要"] = res_display["予測需要"].apply(lambda x: int(x))
    
    st.dataframe(safe_display(res_display), use_container_width=True)
    st.caption("💡 実績/日: 過去の在庫記録から計算 / 理論/日: 稼働率設定 × 1室あたり個数")

def page_calendar():
    st.header(t("cal_header"))
    t1, t2 = st.tabs([t("cal_tab_new"), t("cal_tab_list")])
    items = get_items_df()
    with t1:
        if items is not None and not items.empty:
            c1, c2 = st.columns([1, 2])
            with c1:
                imap = {r["name"]: r["id"] for _, r in items.iterrows()}
                sel = st.selectbox(t("cal_item"), list(imap.keys()))
                if sel:
                    iid = imap[sel]
                    row = items[items["id"] == iid].iloc[0]
                    with st.form("cal_in", clear_on_submit=True):
                        od = st.date_input(t("cal_order_date"))
                        ad = st.date_input(t("cal_arrival_date"))
                        cc1, cc2 = st.columns(2)
                        qc = cc1.number_input(t("cal_cs"), 0)
                        qb = cc2.number_input(t("cal_box"), 0)
                        nt = st.text_input(t("cal_note"))
                        if st.form_submit_button(t("btn_save_cal")):
                            qc = int(qc); qb = int(qb)
                            tot = int(qc * row["cs_total_units"] + qb * row["units_per_box"])
                            add_delivery(iid, od.isoformat(), ad.isoformat(), qc, qb, tot, nt)
                            st.toast(t("success_save_cal"), icon="🚚")
                            st.rerun()
    with t2:
        df = get_delivery_list()
        if df is not None and not df.empty:
            if "cy" not in st.session_state: st.session_state["cy"] = date.today().year
            if "cm" not in st.session_state: st.session_state["cm"] = date.today().month
            c_p, c_l, c_n = st.columns([1, 2, 1])
            if c_p.button(t("prev_month")): 
                if st.session_state["cm"] == 1: st.session_state["cm"]=12; st.session_state["cy"]-=1
                else: st.session_state["cm"]-=1
                st.rerun()
            if c_n.button(t("next_month")):
                if st.session_state["cm"] == 12: st.session_state["cm"]=1; st.session_state["cy"]+=1
                else: st.session_state["cm"]+=1
                st.rerun()
            c_l.markdown(f"<h3 style='text-align:center'>{st.session_state['cy']} / {st.session_state['cm']}</h3>", unsafe_allow_html=True)
            cols = st.columns(7)
            for i, d in enumerate(t("weekdays")):
                cols[i].markdown(f"<div style='text-align:center;font-weight:bold;color:{'blue' if i==5 else 'red' if i==6 else 'black'}'>{d}</div>", unsafe_allow_html=True)
            cal = calendar.monthcalendar(st.session_state["cy"], st.session_state["cm"])
            df["adt"] = pd.to_datetime(df["arrival_date"])
            m_df = df[(df["adt"].dt.year == st.session_state["cy"]) & (df["adt"].dt.month == st.session_state["cm"])]
            for week in cal:
                cols = st.columns(7)
                for i, day in enumerate(week):
                    with cols[i]:
                        if day != 0:
                            dt = date(st.session_state["cy"], st.session_state["cm"], day)
                            hol = get_jp_holiday_name(dt)
                            bg = "#e3f2fd" if dt == date.today() else "white"
                            clr = "blue" if i==5 else "red" if i==6 or hol else "black"
                            with st.container(border=True):
                                lbl = f"{day}" + (f" <small>({hol})</small>" if hol else "")
                                st.markdown(f"<div style='text-align:right;color:{clr};background:{bg}'>{lbl}</div>", unsafe_allow_html=True)
                                for _, r in m_df[m_df["adt"].dt.day == day].iterrows():
                                    q_txt = f"{r['qty_cs']} CS"
                                    if r['qty_box'] > 0: q_txt += f" + {r['qty_box']} B"
                                    st.markdown(f"<div style='background:#f0f0f0;font-size:0.8em;padding:2px'>📦 {r['item']}<br><b>{q_txt}</b></div>", unsafe_allow_html=True)
                        else: st.write("")
            st.divider()
            st.subheader(t("cal_list"))
            c1, c2 = st.columns(2)
            si = c1.selectbox(t("cal_search_item"), ["All"] + list(df["item"].unique()))
            if si != "All": df = df[df["item"] == si]
            st.dataframe(safe_display(df[["order_date", "arrival_date", "item", "qty_cs", "qty_box", "total_units", "note"]]), use_container_width=True)
            opts = [f"ID {r['id']}: {r['arrival_date']} - {r['item']} ({r['qty_cs']} CS)" for _, r in df.iterrows()]
            sd = st.selectbox(t("select_delete"), opts, key="del_cal")
            if st.button(t("btn_delete"), key="btn_del_cal", type="primary"):
                if sd:
                    did = int(sd.split(":")[0].replace("ID", "").strip())
                    delete_delivery(did)
                    st.toast(t("success_delete"), icon="🗑️")
                    st.rerun()

def main():
    st.set_page_config(page_title="Inventory SQL", layout="wide")
    init_db()
    with st.sidebar:
        st.title("🏨 Inventory SQL")
        st.divider()
        menu = ["menu_home", "menu_items", "menu_stock", "menu_forecast", "menu_calendar"]
        sel_label = st.radio(t("menu_title"), [t(k) for k in menu])
        try:
            sel_index = [t(k) for k in menu].index(sel_label)
            sel = menu[sel_index].replace("menu_", "")
        except:
            sel = "home"
        st.divider()
        st.caption("⚡ Powered by SQLAlchemy")
    if sel == "home": page_home()
    elif sel == "items": page_items()
    elif sel == "stock": page_stock()
    elif sel == "forecast": page_forecast_general()
    elif sel == "calendar": page_calendar()

if __name__ == "__main__":
    main()
