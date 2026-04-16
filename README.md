# 🏦 ELS 우리신탁 모니터

DART 전자공시시스템에서 우리은행 신탁 채널로 판매되는 ELS 정보를 자동 수집하여 대시보드로 보여주는 웹앱입니다.

## 기능
- 📅 조회 기간 자유 설정
- 🏢 16개 증권사 중 개별/전체 선택
- 💱 통화별 필터 (KRW/USD)
- 📊 발행사별 종목수 차트
- 📋 종목 목록 (검색/필터 가능)
- 📥 엑셀 다운로드

## 대상 증권사 (16개)
한화, 신한, IBK, 하나, 교보, 메리츠, 신영, 한국투자, KB, NH, 삼성, 미래에셋, 대신, 키움, 유안타, 현대차

## 로컬 실행
```bash
pip install -r requirements.txt
streamlit run app.py
```

## 배포 (Streamlit Cloud)
1. 이 저장소를 GitHub에 push
2. https://share.streamlit.io 에서 Deploy
3. Secrets에 `DART_API_KEY = "본인_키"` 입력
