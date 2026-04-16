"""
ELS 우리신탁 모니터 — Streamlit 대시보드
"""

import streamlit as st
import pandas as pd
import io
from datetime import date, timedelta
from collections import Counter

# ── 페이지 설정 ──────────────────────────────────────────
st.set_page_config(
    page_title="ELS 우리신탁 모니터",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── els_core import ──────────────────────────────────────
import els_core

# ── API 키 로드 ──────────────────────────────────────────
API_KEY = st.secrets.get("DART_API_KEY", None)
if not API_KEY:
    st.error("⚠️ API 키가 설정되지 않았습니다. Streamlit Cloud → Settings → Secrets에 DART_API_KEY를 입력해주세요.")
    st.stop()


# ── 캐시: 같은 (발행사, 기간) 조합은 재조회하지 않음 ────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_issuer_data(api_key, issuer_full, date_from, date_to):
    """단일 증권사 데이터 수집 (캐시됨)"""
    return els_core.process_issuer(api_key, issuer_full, date_from, date_to)


def fetch_all(api_key, issuers, date_from, date_to, progress_bar=None):
    """전체 증권사 데이터 수집"""
    all_items = []
    for i, issuer in enumerate(issuers):
        if progress_bar:
            progress_bar.progress(
                (i + 1) / len(issuers),
                text=f"📡 {els_core.ISSUER_SHORT.get(issuer, issuer)} 조회 중... ({i+1}/{len(issuers)})"
            )
        items = fetch_issuer_data(api_key, issuer, date_from, date_to)
        all_items.extend(items)
    return all_items


def items_to_dataframe(items):
    """ELSItem 리스트를 DataFrame으로 변환"""
    rows = []
    for it in items:
        rows.append({
            "발행사": it.issuer_short,
            "호수": it.series_no,
            "비고": it.note,
            "Cur": it.currency,
            "Pricing date": it.pricing_date.strftime("%Y-%m-%d") if it.pricing_date else "",
            "Strike date": it.strike_date.strftime("%Y-%m-%d") if it.strike_date else "",
            "structure": it.structure,
            "und1": it.und1,
            "und2": it.und2,
            "und3": it.und3,
            "mat/freq": it.mat_freq,
            "ko/ki": it.ko_ki,
            "price": it.price_pct,
            "coupon": it.coupon_pct,
        })
    return pd.DataFrame(rows)


def to_excel_bytes(df):
    """DataFrame을 엑셀 바이트로 변환"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="은행신탁ELS", index=False)
        ws = writer.sheets["은행신탁ELS"]
        # 컬럼 너비 자동 조절
        for col_idx, col_name in enumerate(df.columns, 1):
            max_len = max(len(str(col_name)), df[col_name].astype(str).str.len().max())
            ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max_len + 3, 50)
    return output.getvalue()


# ══════════════════════════════════════════════════════════
#  사이드바
# ══════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ⚙️ 조회 설정")
    st.markdown("---")
    
    # 날짜 선택
    st.markdown("### 📅 조회 기간")
    today = date.today()
    # 이번 주 월요일 기본값
    default_from = today - timedelta(days=today.weekday())
    default_to = default_from + timedelta(days=4)  # 금요일
    
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input("시작일", value=default_from)
    with col2:
        date_to = st.date_input("종료일", value=default_to)
    
    st.markdown("---")
    
    # 발행사 선택
    st.markdown("### 🏢 발행사")
    all_issuers = els_core.ISSUERS
    short_names = [els_core.ISSUER_SHORT.get(i, i) for i in all_issuers]
    
    select_all = st.checkbox("전체 선택", value=True)
    
    if select_all:
        selected_issuers = all_issuers
    else:
        selected_shorts = st.multiselect(
            "증권사 선택",
            options=short_names,
            default=short_names,
        )
        # short → full 역매핑
        short_to_full = {v: k for k, v in els_core.ISSUER_SHORT.items()}
        selected_issuers = [short_to_full.get(s, s) for s in selected_shorts]
    
    st.markdown("---")
    
    # 통화 필터
    st.markdown("### 💱 통화")
    currency_filter = st.radio(
        "통화 필터",
        options=["전체", "KRW", "USD"],
        horizontal=True,
        label_visibility="collapsed",
    )
    
    st.markdown("---")
    
    # 조회 버튼
    run_button = st.button("🔍 데이터 조회", use_container_width=True, type="primary")


# ══════════════════════════════════════════════════════════
#  메인 영역
# ══════════════════════════════════════════════════════════

# 헤더
st.markdown("""
<h1 style='text-align: center; margin-bottom: 0;'>🏦 ELS 우리신탁 모니터</h1>
<p style='text-align: center; color: gray; font-size: 0.9em;'>
    DART 공시 기반 우리은행 신탁 ELS 주간 현황
</p>
""", unsafe_allow_html=True)

st.markdown("---")


# 세션 상태 관리
if "all_items" not in st.session_state:
    st.session_state.all_items = None
    st.session_state.query_info = None


# 조회 실행
if run_button:
    if not selected_issuers:
        st.warning("⚠️ 최소 1개 이상의 증권사를 선택해주세요.")
    else:
        date_from_str = date_from.strftime("%Y-%m-%d")
        date_to_str = date_to.strftime("%Y-%m-%d")
        
        progress = st.progress(0, text="📡 데이터 조회 시작...")
        
        all_items = fetch_all(API_KEY, selected_issuers, date_from_str, date_to_str, progress)
        all_items = els_core.dedup_amendments(all_items)
        
        progress.empty()
        
        st.session_state.all_items = all_items
        st.session_state.query_info = {
            "date_from": date_from_str,
            "date_to": date_to_str,
            "issuers": selected_issuers,
        }


# 결과 표시
if st.session_state.all_items is not None:
    all_items = st.session_state.all_items
    query = st.session_state.query_info
    
    # 우리은행 신탁 필터
    woori = [x for x in all_items if x.sold_via_woori]
    
    # 통화 필터
    if currency_filter == "KRW":
        woori = [x for x in woori if x.currency == "KRW"]
    elif currency_filter == "USD":
        woori = [x for x in woori if x.currency == "USD"]
    
    # 통화별 정렬 (KRW 먼저, USD 나중)
    woori.sort(key=lambda x: (
        0 if x.currency == "KRW" else 1,
        x.receipt_date or date.min,
        x.issuer_short,
        int(x.series_no or 0),
    ))
    
    # ── 상단 요약 지표 ────────────────────────────────
    woori_all = [x for x in all_items if x.sold_via_woori]
    total_all = len(woori_all)
    krw_cnt = len([x for x in woori_all if x.currency == "KRW"])
    usd_cnt = len([x for x in woori_all if x.currency == "USD"])
    issuer_cnt = len(set(x.issuer_short for x in woori_all))
    
    # 모집총액 합계 (억 단위)
    total_offering_krw = sum(
        (x.offering_total or 0) for x in woori_all if x.currency == "KRW"
    )
    total_offering_usd = sum(
        (x.offering_total or 0) for x in woori_all if x.currency == "USD"
    )
    
    st.caption(f"📅 조회 기간: {query['date_from']} ~ {query['date_to']}")
    
    # 1행: 핵심 지표
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("총 종목수", f"{total_all}종목")
    c2.metric("발행사", f"{issuer_cnt}개")
    c3.metric("KRW 종목", f"{krw_cnt}종목")
    c4.metric("USD 종목", f"{usd_cnt}종목")
    c5.metric("모집총액 (KRW)",
              f"{total_offering_krw / 100_000_000:,.0f}억원" if total_offering_krw else "-")
    
    # USD 모집총액이 있으면 추가 표시
    if total_offering_usd > 0:
        st.caption(f"💵 USD 모집총액: ${total_offering_usd / 10_000:,.0f}만")
    
    st.markdown("---")
    
    # ── 발행사별 요약 테이블 ──────────────────────────
    if woori:
        by_issuer = Counter(x.issuer_short for x in woori)
        offering_by_issuer = {}
        for x in woori:
            key = x.issuer_short
            if key not in offering_by_issuer:
                offering_by_issuer[key] = 0
            offering_by_issuer[key] += (x.offering_total or 0)
        
        summary_data = []
        for issuer, cnt in sorted(by_issuer.items(), key=lambda x: x[1], reverse=True):
            amt = offering_by_issuer.get(issuer, 0)
            summary_data.append({
                "발행사": issuer,
                "종목수": cnt,
                "모집총액(억)": f"{amt / 100_000_000:,.0f}" if amt else "-",
            })
        summary_df = pd.DataFrame(summary_data)
        
        # 차트 + 발행사 요약 나란히
        col_chart, col_summary = st.columns([3, 2])
        
        with col_chart:
            st.markdown("#### 📊 발행사별 종목수")
            chart_df = pd.DataFrame(
                sorted(by_issuer.items(), key=lambda x: x[1], reverse=True),
                columns=["발행사", "종목수"]
            )
            st.bar_chart(chart_df.set_index("발행사"), horizontal=True)
        
        with col_summary:
            st.markdown("#### 📋 발행사별 요약")
            st.dataframe(
                summary_df,
                use_container_width=True,
                hide_index=True,
                height=min(len(summary_data) * 35 + 40, 500),
            )
    
    st.markdown("---")
    
    # ── 종목 목록 (표) + 다운로드 ────────────────────
    if woori:
        st.markdown("#### 📋 종목 목록")
        
        # 검색 필터
        search = st.text_input("🔍 검색 (호수, 발행사, 기초자산 등)", "")
        
        df = items_to_dataframe(woori)
        if search:
            mask = df.astype(str).apply(lambda row: row.str.contains(search, case=False).any(), axis=1)
            df = df[mask]
        
        st.dataframe(
            df,
            use_container_width=True,
            height=600,
            column_config={
                "발행사": st.column_config.TextColumn(width="small"),
                "호수": st.column_config.TextColumn(width="small"),
                "Cur": st.column_config.TextColumn(width="small"),
                "ko/ki": st.column_config.TextColumn(width="large"),
                "price": st.column_config.NumberColumn(format="%.1f"),
                "coupon": st.column_config.NumberColumn(format="%.2f"),
            },
        )
        st.caption(f"총 {len(df)}종목 표시")
        
        # 엑셀 다운로드
        st.markdown("---")
        excel_bytes = to_excel_bytes(df)
        filename = f"els_woori_{query['date_from']}_{query['date_to']}.xlsx"
        
        col_dl, col_info = st.columns([1, 3])
        with col_dl:
            st.download_button(
                label="📥 엑셀 다운로드",
                data=excel_bytes,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
            )
        with col_info:
            st.caption(f"📁 {filename}")
            st.caption(f"총 {len(df)}종목 (KRW {krw_cnt} / USD {usd_cnt})")
    else:
        st.info("조회된 종목이 없습니다.")

else:
    # 초기 상태 — 안내 메시지
    st.markdown("""
    <div style='text-align: center; padding: 60px 20px; color: gray;'>
        <h3>👈 왼쪽 사이드바에서 조회 기간과 발행사를 설정한 후<br>
        <span style='color: #FF4B4B;'>🔍 데이터 조회</span> 버튼을 클릭하세요.</h3>
        <p style='margin-top: 20px;'>
            DART 공시에서 우리은행 신탁 판매 ELS를 자동으로 수집합니다.<br>
            최초 조회는 1~2분 소요되며, 같은 기간 재조회 시 캐시로 즉시 표시됩니다.
        </p>
    </div>
    """, unsafe_allow_html=True)


# 푸터
st.markdown("---")
st.caption("📌 데이터 출처: DART 전자공시시스템 | 매주 자동 업데이트는 아니며, 조회 시점 기준으로 수집됩니다.")
