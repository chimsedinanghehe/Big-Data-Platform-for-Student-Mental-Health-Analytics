from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import html
import hashlib
import json
import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.dashboard_core import (
    CATEGORY_LABELS,
    DATA_SOURCE_COLUMN,
    HMS_NATIVE_FEATURE_DEFINITIONS,
    HMS_NATIVE_FEATURES,
    HMS_SCOPED_COLUMNS,
    HMS_POPULATION_LABEL,
    INTERNAL_SOURCE_FILE_COLUMN,
    INTERNAL_SOURCE_TYPE_COLUMN,
    MENTAL_SCHOOL_POPULATION_LABEL,
    POPULATION_COLUMN,
    QNUM_TO_ENGLISH,
    RESEARCH_FEATURE_DEFINITIONS,
    RESEARCH_FEATURES,
    TARGET_DEFINITION,
    apply_description_filters,
    cluster_gap_summary,
    cluster_overview_table,
    compact_data_quality_summary,
    derived_score_summary,
    explain_target_counts,
    extract_qnum,
    find_q_col,
    hms_data_quality_summary,
    preprocess_yrbs_data,
    question_frequency_table,
    raw_question_catalog,
    target_by_response_table,
    target_prevalence_by_group,
    target_prevalence_by_score_bins,
    top_missing_questions,
    top_target_gap_questions,
    value_to_label,
)

APP_TITLE = "Student Mental Health Analytics Dashboard"
PIPELINE_VERSION = "research_construct_dashboard_v12_correct_hms_family_direction"
DEFAULT_DATA_PATHS = [Path("data/Mental School.csv"), Path("Mental School.csv")]
DEFAULT_HMS_PATHS = [
    Path("data/HMS_2022-2023_PUBLIC_instchars.csv"),
    Path("data/HMS_2023-2024_PUBLIC_instchars.csv"),
    Path("data/HMS_2024-2025_PUBLIC_instchars.csv"),
]
LOCAL_CACHE_DIR = Path("data/.dashboard_cache")
GCS_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "student-mental-health-496205")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "student-mental-health-lake-nhom1-2026")
GOLD_SURVEY_FEATURE_PREFIX = "gold/dashboard_tables/survey_analytic_features/"
GOLD_SURVEY_TABLE_PREFIXES = {
    "survey_overview_summary": "gold/dashboard_tables/survey_overview_summary/",
    "survey_response_by_date": "gold/dashboard_tables/survey_response_by_date/",
    "survey_demographic_summary": "gold/dashboard_tables/survey_demographic_summary/",
    "survey_question_distribution": "gold/dashboard_tables/survey_question_distribution/",
    "survey_numeric_summary": "gold/dashboard_tables/survey_numeric_summary/",
    "survey_analytic_features": GOLD_SURVEY_FEATURE_PREFIX,
}
REQUIRED_GOLD_SURVEY_TABLES = {"survey_analytic_features"}
GOLD_SURVEY_CURRENT_MANIFEST = "gold/dashboard_tables/_manifests/survey_current.json"


def create_gcs_storage_client():
    import google.auth
    from google.cloud import storage

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if hasattr(credentials, "with_quota_project"):
        credentials = credentials.with_quota_project(None)
    return storage.Client(project=GCS_PROJECT_ID, credentials=credentials)


KEY_RAW_QNUMS = sorted(
    {26, 84}.union(
        qnum
        for definition in RESEARCH_FEATURE_DEFINITIONS.values()
        for qnum in definition["qnums"]
    )
)


@dataclass
class BoardPreparedData:
    raw_analysis: pd.DataFrame
    cleaned: pd.DataFrame
    gold_tables: Optional[Dict[str, pd.DataFrame]] = None

THEME = {
    "background": "#F4F6F9",
    "surface": "#FFFFFF",
    "surface_alt": "#F8FAFC",
    "border": "#D9E2EC",
    "text": "#2F3B45",
    "muted": "#7B8794",
    "primary": "#2D9CDB",
    "primary_soft": "#E8F5FD",
    "secondary": "#27AE60",
    "teal": "#00A8C6",
    "teal_soft": "#E6F8FB",
    "coral": "#E74C3C",
    "coral_soft": "#FDEDEC",
    "gold": "#F39C12",
    "lavender": "#8E7CC3",
    "success": "#2ECC71",
    "danger": "#E74C3C",
    "grid": "#E5EAF0",
}

PALETTE = [
    THEME["primary"],
    THEME["teal"],
    THEME["coral"],
    THEME["gold"],
    "#FF5C7A",
    THEME["secondary"],
    "#7F8C8D",
    THEME["lavender"],
]

PLOT_CONFIG = {"displayModeBar": False, "responsive": True}

BOARD_NAV_PAGES = {
    "Tổng quan": "Toàn bộ mẫu và các nhóm yếu tố có thể so sánh chung",
    "Học sinh": "Cụm khảo sát chi tiết riêng của học sinh",
    "Sinh viên": "Cụm khảo sát chi tiết riêng của sinh viên",
}

RISK_LABELS = {
    "At Risk": "Có dấu hiệu nguy cơ",
    "Lower Risk": "Chưa thấy dấu hiệu rõ",
    1: "Có dấu hiệu nguy cơ",
    0: "Chưa thấy dấu hiệu rõ",
}

POPULATION_LABELS = {
    HMS_POPULATION_LABEL: "Sinh viên đại học/cao đẳng",
    MENTAL_SCHOOL_POPULATION_LABEL: "Học sinh trung học/THPT",
}

CONSTRUCT_LABELS = {
    "Family Pressure Index": "Gia đình & an toàn tại nhà",
    "Academic Pressure Index": "Học tập & kết nối trường",
    "Peer & Safety Stress Index": "Bạn bè, bắt nạt & an toàn",
    "Trauma Exposure Index": "Sang chấn & bạo lực quan hệ",
    "Substance Coping Risk Index": "Dùng chất như cách đối phó",
    "Lifestyle Recovery Deficit": "Thiếu phục hồi hằng ngày",
}

CONSTRUCT_SHORT_LABELS = {
    "Family Pressure Index": "Gia đình",
    "Academic Pressure Index": "Học tập",
    "Peer & Safety Stress Index": "Bạn bè/an toàn",
    "Trauma Exposure Index": "Sang chấn",
    "Substance Coping Risk Index": "Dùng chất",
    "Lifestyle Recovery Deficit": "Phục hồi",
}

CONSTRUCT_ANALYST_MEANING = {
    "Family Pressure Index": "Bất lợi trong gia đình, thiếu an toàn/cơ bản và mức giám sát hỗ trợ từ người lớn.",
    "Academic Pressure Index": "Khó khăn học tập, cảm giác thuộc về trường và năng lực tập trung trong học đường.",
    "Peer & Safety Stress Index": "Quan hệ bạn bè, bắt nạt và cảm giác không an toàn ở trường/khu vực sống.",
    "Trauma Exposure Index": "Trải nghiệm bị ép buộc, bạo lực tình dục hoặc bạo lực trong quan hệ.",
    "Substance Coping Risk Index": "Hành vi dùng thuốc lá, vape, rượu, cần sa hoặc thuốc giảm đau như tín hiệu đối phó rủi ro.",
    "Lifestyle Recovery Deficit": "Thiếu ngủ, ít vận động, ăn sáng/nước uống chưa đủ và thời gian mạng xã hội cao.",
}

CLUSTER_LABELS = {
    "Family Pressure": "Gia đình & chăm sóc",
    "Academic Pressure": "Học tập & nhà trường",
    "Peer & Safety Stress": "Bạn bè & an toàn",
    "Trauma Exposure": "Sang chấn & bạo lực",
    "Substance Coping": "Dùng chất & đối phó",
    "Recovery Lifestyle": "Phục hồi & lối sống",
    "Demographics": "Nhân khẩu học",
    "Data Quality": "Chất lượng dữ liệu",
    "Other": "Khác",
}

QUESTION_LABELS = {
    12: "Mang vũ khí đến trường",
    13: "Mang súng trong 12 tháng qua",
    14: "Nghỉ học vì cảm thấy không an toàn",
    15: "Bị đe dọa hoặc bị thương bằng vũ khí",
    16: "Tham gia đánh nhau",
    17: "Tham gia đánh nhau tại trường",
    18: "Chứng kiến bạo lực nơi sinh sống",
    19: "Bị ép quan hệ tình dục",
    20: "Bị ép thực hiện hành vi tình dục khác",
    21: "Bạo lực tình dục trong hẹn hò",
    22: "Bạo lực thể chất trong hẹn hò",
    23: "Bị đối xử bất công vì chủng tộc/dân tộc",
    24: "Bị bắt nạt tại trường",
    25: "Bị bắt nạt trực tuyến",
    26: "Buồn bã hoặc tuyệt vọng kéo dài",
    31: "Từng hút thuốc lá",
    32: "Bắt đầu hút thuốc trước 13 tuổi",
    33: "Hút thuốc lá hiện tại",
    34: "Số điếu thuốc mỗi ngày",
    35: "Từng dùng vape",
    36: "Dùng vape hiện tại",
    37: "Nguồn có được vape",
    38: "Dùng thuốc lá không khói",
    39: "Hút xì gà",
    40: "Cố gắng cai thuốc lá",
    41: "Uống rượu lần đầu trước 13 tuổi",
    42: "Uống rượu hiện tại",
    43: "Uống rượu quá mức",
    44: "Số ly rượu lớn nhất một lần",
    45: "Nguồn có được rượu",
    46: "Từng dùng cần sa",
    47: "Dùng cần sa lần đầu trước 13 tuổi",
    48: "Dùng cần sa hiện tại",
    49: "Từng dùng sai thuốc giảm đau kê đơn",
    50: "Từng dùng cocaine",
    51: "Từng sử dụng chất hít",
    52: "Từng dùng heroin",
    53: "Từng dùng ma túy đá",
    54: "Từng dùng thuốc lắc",
    55: "Từng tiêm chích ma túy",
    56: "Từng quan hệ tình dục",
    57: "Quan hệ tình dục lần đầu trước 13 tuổi",
    58: "Có từ 4 bạn tình trở lên",
    59: "Có hoạt động tình dục gần đây",
    60: "Dùng rượu/ma túy trước lần quan hệ cuối",
    61: "Dùng bao cao su trong lần quan hệ cuối",
    62: "Dùng biện pháp tránh thai",
    63: "Giới tính của bạn tình",
    64: "Xu hướng tình dục",
    65: "Bản dạng chuyển giới",
    75: "Ăn sáng thường xuyên",
    76: "Hoạt động thể chất hằng ngày",
    77: "Tham gia lớp giáo dục thể chất",
    78: "Tham gia đội thể thao",
    79: "Chấn động não do thể thao/vận động",
    80: "Thời gian dùng mạng xã hội",
    81: "Từng xét nghiệm HIV",
    82: "Xét nghiệm bệnh lây truyền tình dục",
    83: "Lần cuối đi khám nha sĩ",
    84: "Số ngày sức khỏe tinh thần kém",
    85: "Thời lượng ngủ",
    87: "Kết quả học tập tự đánh giá",
    88: "Bị người lớn tuổi hơn ép tình dục",
    89: "Bị người lớn trong nhà xúc phạm/làm nhục",
    90: "Bị người lớn trong nhà bạo hành thể chất",
    91: "Chứng kiến bạo lực trong gia đình",
    92: "Lạm dụng thuốc giảm đau kê đơn",
    93: "Từng dùng chất gây ảo giác",
    94: "Hỏi xin đồng thuận trước quan hệ",
    96: "Uống nước hằng ngày",
    99: "Nhu cầu cơ bản được người lớn đáp ứng",
    100: "Sống cùng người lạm dụng chất",
    101: "Sống cùng người có vấn đề sức khỏe tinh thần/tự sát",
    102: "Cha hoặc mẹ từng bị giam giữ",
    103: "Cảm giác kết nối với trường học",
    104: "Phụ huynh biết con đang ở đâu",
    105: "Bị kỷ luật không công bằng ở trường",
    106: "Khó tập trung, ghi nhớ hoặc ra quyết định",
    107: "Khả năng nói tiếng Anh",
}

RESPONSE_LABEL_OVERRIDES = {
    "Yes": "Có",
    "No": "Không",
    "Never": "Không bao giờ",
    "Rarely": "Hiếm khi",
    "Sometimes": "Thỉnh thoảng",
    "Most of the time": "Hầu hết thời gian",
    "Always": "Luôn luôn",
    "Strongly agree": "Rất đồng ý",
    "Agree": "Đồng ý",
    "Not sure": "Không chắc",
    "Disagree": "Không đồng ý",
    "Strongly disagree": "Rất không đồng ý",
    "Missing": "Thiếu dữ liệu",
    "Female": "Nữ",
    "Male": "Nam",
    "Other/Unspecified": "Khác/không xác định",
    "Nonbinary/Genderqueer": "Phi nhị nguyên/genderqueer",
    "Transgender": "Chuyển giới",
    "Self-described": "Tự mô tả",
    "Prefer no response/Other": "Không trả lời/khác",
    "9th": "Lớp 9",
    "10th": "Lớp 10",
    "11th": "Lớp 11",
    "12th": "Lớp 12",
    "College 1st year": "Sinh viên năm 1",
    "College 2nd year": "Sinh viên năm 2",
    "College 3rd year": "Sinh viên năm 3",
    "College 4th+ year": "Sinh viên năm 4+",
    "Graduate/Professional": "Sau đại học/chuyên nghiệp",
    "Ungraded/Other": "Khác/chưa phân lớp",
    "Other college": "Bậc đại học khác",
}

TABLE_COLUMN_LABELS = {
    "qnum": "Mã nguồn",
    "Q": "Mã nguồn",
    "question": "Yếu tố dữ liệu",
    "Question": "Yếu tố dữ liệu",
    "Metric": "Chỉ số dữ liệu",
    "Value": "Giá trị",
    "Data Source": "Nhóm dữ liệu",
    "Rows": "Số bản ghi",
    "Rows loaded": "Số bản ghi được nạp",
    "Analysable rows after target validation": "Số bản ghi đủ nhãn nguy cơ",
    "Raw columns loaded": "Số cột dữ liệu gốc",
    "Columns after preprocessing": "Số trường sau chuẩn hóa",
    "Research-scope survey questions": "Số yếu tố khảo sát trong phạm vi phân tích",
    "Research constructs": "Số khía cạnh phân tích",
    "Missing cells after preprocessing": "Số ô còn thiếu sau chuẩn hóa",
    "Data sources": "Số nhóm dữ liệu được hợp nhất",
    "HMS age outliers set to missing": "Số ngoại lệ tuổi sinh viên đã chuyển thành thiếu",
    "Average HMS scoped missing (%)": "Thiếu dữ liệu trung bình ở nhóm sinh viên (%)",
    "At Risk Rate (%)": "Tỷ lệ nguy cơ (%)",
    "Average Scoped Missing (%)": "Thiếu dữ liệu trung bình (%)",
    "Age Coverage (%)": "Độ phủ dữ liệu tuổi (%)",
    "Age Outliers Set Missing": "Ngoại lệ tuổi đã chuyển thành thiếu",
    "Construct Coverage (%)": "Độ phủ khía cạnh phân tích (%)",
    "cluster": "Nhóm chủ đề",
    "Construct": "Khía cạnh",
    "Response": "Phản hồi",
    "Count": "Số bản ghi",
    "At Risk Count": "Số bản ghi có nguy cơ",
    "At Risk Rate": "Tỷ lệ nguy cơ (%)",
    "At Risk %": "Tỷ lệ nguy cơ (%)",
    "At Risk Gap (%)": "Chênh lệch nguy cơ (%)",
    "At Risk ở mức thấp (%)": "Nguy cơ ở mức thấp (%)",
    "At Risk ở mức cao (%)": "Nguy cơ ở mức cao (%)",
    "Thay đổi khi construct tăng (%)": "Chênh lệch khi điểm tăng (%)",
    "Highest-risk response": "Phản hồi nguy cơ cao nhất",
    "Highest-risk rate (%)": "Tỷ lệ cao nhất (%)",
    "Lowest-risk response": "Phản hồi nguy cơ thấp nhất",
    "Lowest-risk rate (%)": "Tỷ lệ thấp nhất (%)",
    "missing": "Số ô thiếu",
    "missing_pct": "Thiếu dữ liệu (%)",
    "unique_valid_values": "Số phản hồi hợp lệ",
    "n": "Số bản ghi",
    "Available n": "Số bản ghi có dữ liệu",
    "Lower Risk mean": "TB nhóm chưa thấy nguy cơ",
    "At Risk mean": "TB nhóm có nguy cơ",
    "Difference": "Chênh lệch",
    "Mean score": "Điểm trung bình",
    "Mức construct": "Mức điểm",
    "Top pressure construct": "Khía cạnh cao nhất",
    "Top construct score": "Điểm khía cạnh cao nhất",
    "Most elevated construct": "Khía cạnh nổi bật hơn toàn mẫu",
    "Elevation vs overall": "Chênh lệch so với toàn mẫu",
    "Dimension": "Chiều phân tầng",
    "Group": "Nhóm",
    "Selected group": "Nhóm đang chọn",
    "Overall": "Toàn mẫu",
    "Selected group %": "Tỷ lệ trong nhóm (%)",
    "Overall %": "Tỷ lệ toàn mẫu (%)",
    "Selected group n": "Số bản ghi nhóm đang chọn",
    "Overall n": "Số bản ghi toàn mẫu",
    "Selected group At Risk %": "Nguy cơ trong nhóm (%)",
    "Overall At Risk %": "Nguy cơ toàn mẫu (%)",
    "Gender": "Giới tính",
    "Grade": "Bậc/lớp",
    "Age": "Độ tuổi",
}

HMS_CONSTRUCT_SOURCE_MEANINGS = {
    "Family Pressure Index": "Lo lắng về nhà ở, thực phẩm, tài chính, khả năng chi trả chi phí học tập - sinh hoạt và mức hỗ trợ từ gia đình/giảng viên.",
    "Academic Pressure Index": "Căng thẳng học tập, ảnh hưởng của học tập tới đời sống, cảm giác cạnh tranh, impostor feeling, trượt môn, quản lý thời gian và nghi ngờ việc học.",
    "Peer & Safety Stress Index": "Cảm giác thuộc về, trải nghiệm phân biệt đối xử, mức an toàn trong/ngoài campus và căng thẳng từ môi trường thù địch.",
    "Trauma Exposure Index": "Bạo hành, stalking, assault tình dục, bạo lực thân mật và các hành vi đe dọa/xúc phạm từ bạn đời.",
    "Substance Coping Risk Index": "Rượu, binge drinking, thuốc lá/vape, cần sa và các chỉ báo dùng chất liên quan tới đối phó rủi ro.",
    "Lifestyle Recovery Deficit": "Giấc ngủ, vận động và lo lắng thực phẩm như các tín hiệu thiếu phục hồi hằng ngày.",
}


def _safe_int(value) -> Optional[int]:
    try:
        if pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def construct_label(name: object, short: bool = False) -> str:
    key = str(name)
    labels = CONSTRUCT_SHORT_LABELS if short else CONSTRUCT_LABELS
    return labels.get(key, key)


def cluster_label(name: object) -> str:
    return CLUSTER_LABELS.get(str(name), str(name))


def question_label(qnum: object, fallback: object = "") -> str:
    q = _safe_int(qnum)
    if q is not None and q in QUESTION_LABELS:
        return QUESTION_LABELS[q]
    return str(fallback) if fallback not in (None, "") else f"Chỉ báo nguồn {q}" if q is not None else ""


def source_code(qnum: object) -> str:
    q = _safe_int(qnum)
    return f"Q{q}" if q is not None else str(qnum)


def response_label(value: object) -> str:
    return RESPONSE_LABEL_OVERRIDES.get(str(value), RISK_LABELS.get(value, str(value)))


def population_label(value: object) -> str:
    return POPULATION_LABELS.get(str(value), str(value))


def risk_label(value: object) -> str:
    return RISK_LABELS.get(value, RISK_LABELS.get(str(value), str(value)))


def add_semantic_construct_column(df: pd.DataFrame, source_col: str = "Construct", target_col: str = "Khía cạnh") -> pd.DataFrame:
    out = df.copy()
    if source_col in out.columns:
        out[target_col] = out[source_col].map(construct_label)
    return out


def add_semantic_question_column(df: pd.DataFrame, target_col: str = "Yếu tố dữ liệu") -> pd.DataFrame:
    out = df.copy()
    if "qnum" in out.columns:
        fallback_col = "question" if "question" in out.columns else "Question" if "Question" in out.columns else None
        if fallback_col:
            out[target_col] = [
                question_label(qnum, fallback)
                for qnum, fallback in zip(out["qnum"], out[fallback_col])
            ]
        else:
            out[target_col] = out["qnum"].map(question_label)
    return out


def localize_response_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [
        "Response",
        "Target",
        "Group",
        "Highest-risk response",
        "Lowest-risk response",
        "Mức construct",
        "Score Bin",
    ]:
        if col in out.columns:
            out[col] = out[col].map(response_label)
    return out

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def inject_css() -> None:
    st.markdown(
        f"""
        <style>
        :root {{
            --bg: {THEME["background"]};
            --surface: {THEME["surface"]};
            --surface-alt: {THEME["surface_alt"]};
            --border: {THEME["border"]};
            --text: {THEME["text"]};
            --muted: {THEME["muted"]};
            --primary: {THEME["primary"]};
            --primary-soft: {THEME["primary_soft"]};
            --secondary: {THEME["secondary"]};
            --teal: {THEME["teal"]};
            --teal-soft: {THEME["teal_soft"]};
            --coral: {THEME["coral"]};
            --coral-soft: {THEME["coral_soft"]};
            --gold: {THEME["gold"]};
        }}

        html, body, [class*="css"] {{
            font-family: "Inter", "Segoe UI", Arial, sans-serif;
        }}

        .stApp {{
            background:
                radial-gradient(circle at 7% 6%, rgba(46, 134, 171, 0.11), transparent 26rem),
                radial-gradient(circle at 92% 8%, rgba(42, 157, 143, 0.10), transparent 24rem),
                linear-gradient(180deg, #F8FBFF 0%, var(--bg) 100%);
            color: var(--text);
        }}

        .block-container {{
            max-width: 1480px;
            padding-top: 1.25rem;
            padding-bottom: 3rem;
            padding-left: 2rem;
            padding-right: 2rem;
        }}

        [data-testid="stSidebar"],
        [data-testid="collapsedControl"] {{
            display: none !important;
        }}

        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #F8FBFF 0%, #EFF5FC 100%);
            border-right: 1px solid var(--border);
        }}

        [data-testid="stSidebar"] > div:first-child {{
            padding-top: 1.25rem;
            padding-left: 1.1rem;
            padding-right: 1.1rem;
        }}

        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {{
            color: var(--primary);
            font-weight: 800;
            letter-spacing: -0.01em;
        }}

        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] div {{
            color: var(--text);
        }}

        [data-testid="stSidebar"] .stFileUploader {{
            background: rgba(255,255,255,0.94);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 0.45rem;
        }}

        [data-testid="stSidebar"] [data-baseweb="select"] > div {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            min-height: 42px;
        }}

        [data-testid="stSidebar"] [data-baseweb="tag"] {{
            background: var(--primary-soft);
            border: 1px solid rgba(31,78,121,0.18);
        }}

        [data-testid="stSidebar"] hr {{
            margin-top: 1rem;
            margin-bottom: 1rem;
        }}

        .nav-brand {{
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 0.85rem 0.9rem;
            margin-bottom: 0.8rem;
        }}

        .nav-brand-title {{
            color: #162a38 !important;
            font-size: 1.08rem;
            line-height: 1.25;
            font-weight: 800;
        }}

        .nav-brand-subtitle {{
            color: #526575 !important;
            font-size: 0.73rem;
            margin-top: 0.32rem;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }}

        .nav-caption {{
            color: #3f5364 !important;
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            margin: 0.55rem 0 0.35rem;
        }}

        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] {{
            gap: 0.5rem;
        }}

        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label {{
            display: flex !important;
            align-items: center !important;
            width: 100% !important;
            min-height: 48px !important;
            background: #FFFFFF !important;
            border: 1px solid #CDD8E3 !important;
            border-radius: 9px !important;
            padding: 0.65rem 0.75rem !important;
            box-shadow: 0 1px 2px rgba(25, 45, 60, 0.04);
            transition: border-color 0.15s ease, background 0.15s ease, box-shadow 0.15s ease;
            cursor: pointer;
        }}

        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label > div:first-child {{
            display: none !important;
        }}

        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label p,
        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label span,
        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label div {{
            color: #243746 !important;
            opacity: 1 !important;
            font-size: 0.96rem !important;
            font-weight: 650 !important;
        }}

        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) {{
            background: #E8F7F6 !important;
            border-color: #10A99D !important;
            box-shadow: inset 4px 0 0 #10A99D, 0 2px 5px rgba(16, 169, 157, 0.12);
        }}

        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) p,
        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) span,
        [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] label:has(input:checked) div {{
            color: #083B38 !important;
            font-weight: 780 !important;
        }}

        h1, h2, h3, h4 {{
            color: var(--primary);
            letter-spacing: -0.01em;
        }}

        .hero {{
            background:
                linear-gradient(135deg, rgba(255,255,255,0.98), rgba(240,247,255,0.96));
            border: 1px solid var(--border);
            border-radius: 30px;
            padding: 1.9rem 2rem;
            box-shadow: 0 18px 46px rgba(31,78,121,0.10);
            margin-bottom: 1.15rem;
        }}

        .hero-pill {{
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            width: fit-content;
            background: var(--primary-soft);
            color: var(--primary);
            border: 1px solid rgba(31,78,121,0.18);
            border-radius: 999px;
            padding: 0.42rem 0.85rem;
            font-size: 0.84rem;
            font-weight: 800;
            margin-bottom: 0.9rem;
        }}

        .hero-title {{
            color: var(--primary);
            font-size: 2.2rem;
            line-height: 1.18;
            font-weight: 880;
            max-width: 1180px;
            margin-bottom: 0.65rem;
        }}

        .hero-copy {{
            color: var(--muted);
            font-size: 1.02rem;
            line-height: 1.62;
            max-width: 1200px;
        }}

        .method-box {{
            background: linear-gradient(135deg, var(--primary-soft), var(--teal-soft));
            border: 1px solid rgba(31,78,121,0.16);
            border-left: 6px solid var(--secondary);
            border-radius: 20px;
            padding: 1rem 1.15rem;
            color: var(--text);
            line-height: 1.6;
            margin-bottom: 1.15rem;
            box-shadow: 0 12px 28px rgba(31,78,121,0.06);
        }}

        .kpi-card {{
            background: rgba(255,255,255,0.96);
            border: 1px solid var(--border);
            border-radius: 22px;
            padding: 1.05rem 1.1rem;
            min-height: 132px;
            box-shadow: 0 14px 30px rgba(31,78,121,0.07);
        }}

        .kpi-label {{
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.055em;
            font-size: 0.78rem;
            font-weight: 850;
            margin-bottom: 0.55rem;
        }}

        .kpi-value {{
            color: var(--primary);
            font-size: 2rem;
            font-weight: 900;
            line-height: 1.12;
        }}

        .kpi-note {{
            color: var(--muted);
            font-size: 0.88rem;
            line-height: 1.42;
            margin-top: 0.62rem;
        }}

        .section-head {{
            background: rgba(255,255,255,0.90);
            border: 1px solid var(--border);
            border-radius: 22px;
            padding: 1.05rem 1.2rem;
            box-shadow: 0 12px 28px rgba(31,78,121,0.05);
            margin-top: 1.15rem;
            margin-bottom: 0.9rem;
        }}

        .section-title {{
            color: var(--primary);
            font-size: 1.35rem;
            font-weight: 880;
            line-height: 1.28;
            margin-bottom: 0.25rem;
        }}

        .section-subtitle {{
            color: var(--muted);
            font-size: 0.96rem;
            line-height: 1.55;
        }}

        .note-box {{
            background: var(--coral-soft);
            border: 1px solid rgba(231,111,81,0.20);
            border-radius: 18px;
            padding: 0.95rem 1.05rem;
            color: var(--text);
            line-height: 1.58;
        }}

        .soft-box {{
            background: rgba(255,255,255,0.95);
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 1rem 1.1rem;
            box-shadow: 0 12px 28px rgba(31,78,121,0.05);
            line-height: 1.62;
            color: var(--text);
        }}

        div[data-testid="stMetric"] {{
            background: rgba(255,255,255,0.96);
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 0.95rem 1rem;
            box-shadow: 0 12px 26px rgba(31,78,121,0.06);
        }}

        div[data-testid="stMetricValue"] {{
            color: var(--primary);
            font-weight: 900;
        }}

        [data-testid="stDataFrame"] {{
            border: 1px solid var(--border);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0 12px 26px rgba(31,78,121,0.05);
        }}

        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.4rem;
            padding: 0.42rem;
            border: 1px solid var(--border);
            border-radius: 20px;
            background: rgba(255,255,255,0.93);
            margin-top: 0.9rem;
            margin-bottom: 1rem;
        }}

        .stTabs [data-baseweb="tab"] {{
            height: 2.9rem;
            padding-left: 1rem;
            padding-right: 1rem;
            border-radius: 15px;
            color: var(--primary);
            font-weight: 800;
        }}

        .stTabs [aria-selected="true"] {{
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            color: white !important;
            box-shadow: 0 10px 22px rgba(31,78,121,0.18);
        }}

        button[kind="primary"],
        .stDownloadButton button {{
            border-radius: 14px !important;
            border: none !important;
            background: linear-gradient(135deg, var(--primary), var(--secondary)) !important;
            color: white !important;
            font-weight: 800 !important;
        }}

        hr {{
            border: none;
            height: 1px;
            background: linear-gradient(90deg, transparent, var(--border), transparent);
            margin-top: 1.25rem;
            margin-bottom: 1.25rem;
        }}

        .stApp {{
            background:
                linear-gradient(180deg, #F8FAFC 0%, #F4F6F9 100%);
            color: var(--text);
        }}

        [data-testid="stSidebar"] {{
            background: #243746;
            border-right: 1px solid var(--border);
            box-shadow: 10px 0 38px rgba(0,0,0,0.24);
        }}

        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3,
        h1, h2, h3, h4 {{
            color: var(--primary);
        }}

        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] div,
        .stMarkdown,
        .stMarkdown p,
        label,
        p,
        span {{
            color: var(--text);
        }}

        [data-testid="stSidebar"] .stFileUploader,
        [data-testid="stSidebar"] [data-baseweb="select"] > div,
        [data-baseweb="select"] > div,
        [data-baseweb="input"] > div,
        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input,
        textarea {{
            background: rgba(20,16,13,0.96) !important;
            border: 1px solid var(--border) !important;
            color: var(--text) !important;
            border-radius: 12px;
        }}

        [data-baseweb="popover"],
        [data-baseweb="menu"],
        [role="listbox"] {{
            background: #16100C !important;
            border: 1px solid var(--border) !important;
            color: var(--text) !important;
        }}

        .hero,
        .kpi-card,
        .section-head,
        .soft-box,
        div[data-testid="stMetric"],
        .stTabs [data-baseweb="tab-list"] {{
            background: linear-gradient(180deg, rgba(29,22,17,0.97), rgba(16,11,8,0.96)) !important;
            border: 1px solid var(--border) !important;
            box-shadow: 0 16px 42px rgba(0,0,0,0.34), inset 0 1px 0 rgba(255,179,71,0.08) !important;
        }}

        .hero {{
            border-radius: 24px;
        }}

        .hero-pill,
        [data-testid="stSidebar"] [data-baseweb="tag"] {{
            background: var(--primary-soft) !important;
            border: 1px solid #BFE3F7 !important;
            color: var(--secondary) !important;
        }}

        .hero-title,
        .kpi-value,
        .section-title,
        div[data-testid="stMetricValue"] {{
            color: var(--primary) !important;
        }}

        .hero-copy,
        .kpi-label,
        .kpi-note,
        .section-subtitle {{
            color: var(--muted) !important;
        }}

        .method-box {{
            background: #FFFFFF !important;
            border: 1px solid var(--border) !important;
            border-left: 6px solid var(--primary) !important;
            color: var(--text) !important;
        }}

        .note-box {{
            background: var(--coral-soft) !important;
            border: 1px solid #F5B7B1 !important;
            color: var(--text) !important;
        }}

        .stTabs [data-baseweb="tab"] {{
            color: var(--muted) !important;
        }}

        .stTabs [aria-selected="true"],
        button[kind="primary"],
        .stDownloadButton button {{
            background: var(--primary) !important;
            color: #FFFFFF !important;
            box-shadow: none !important;
        }}

        [data-testid="stDataFrame"] {{
            border: 1px solid var(--border);
            background: var(--surface);
            box-shadow: 0 16px 34px rgba(0,0,0,0.28);
        }}

        div[data-testid="stAlert"] {{
            background: var(--primary-soft);
            border: 1px solid #BFE3F7;
            color: var(--text);
        }}

        /* AdminCAST-inspired cool dashboard theme */
        .stApp {{
            background: #F4F6F9 !important;
            color: var(--text) !important;
        }}

        .block-container {{
            max-width: 1480px;
            padding-top: 1rem;
        }}

        [data-testid="stSidebar"] {{
            background: #243746 !important;
            border-right: 1px solid #1D2B38 !important;
            box-shadow: none !important;
        }}

        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] div {{
            color: #DDE7EF !important;
        }}

        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {{
            color: #FFFFFF !important;
        }}

        [data-testid="stSidebar"] .stFileUploader,
        [data-testid="stSidebar"] [data-baseweb="select"] > div,
        [data-testid="stSidebar"] [data-baseweb="input"] > div,
        [data-testid="stSidebar"] [data-testid="stTextInput"] input,
        [data-testid="stSidebar"] [data-testid="stNumberInput"] input {{
            background: #2F4556 !important;
            border: 1px solid #3F596C !important;
            color: #FFFFFF !important;
            border-radius: 6px !important;
        }}

        [data-baseweb="select"] > div,
        [data-baseweb="input"] > div,
        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input,
        textarea {{
            background: #FFFFFF !important;
            border: 1px solid var(--border) !important;
            color: var(--text) !important;
            border-radius: 6px !important;
        }}

        [data-baseweb="popover"],
        [data-baseweb="menu"],
        [role="listbox"] {{
            background: #FFFFFF !important;
            border: 1px solid var(--border) !important;
            color: var(--text) !important;
        }}

        .hero,
        .kpi-card,
        .section-head,
        .soft-box,
        div[data-testid="stMetric"],
        .stTabs [data-baseweb="tab-list"],
        [data-testid="stDataFrame"] {{
            background: #FFFFFF !important;
            border: 1px solid var(--border) !important;
            border-radius: 4px !important;
            box-shadow: 0 1px 2px rgba(16, 24, 40, 0.06) !important;
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }}

        .kpi-card:hover,
        .soft-box:hover,
        div[data-testid="stMetric"]:hover,
        [data-testid="stDataFrame"]:hover {{
            transform: translateY(-1px);
            border-color: #B8CDD9 !important;
            box-shadow: 0 8px 22px rgba(16, 24, 40, 0.09) !important;
        }}

        .block-container > div {{
            animation: dashboardFadeIn 220ms ease both;
        }}

        @keyframes dashboardFadeIn {{
            from {{
                opacity: 0;
                transform: translateY(4px);
            }}
            to {{
                opacity: 1;
                transform: translateY(0);
            }}
        }}

        .hero {{
            padding: 1.25rem 1.45rem !important;
            margin-bottom: 1rem !important;
        }}

        h1, h2, h3, h4,
        .hero-title,
        .section-title,
        .kpi-value,
        div[data-testid="stMetricValue"] {{
            color: #2F3B45 !important;
        }}

        .hero-copy,
        .kpi-label,
        .kpi-note,
        .section-subtitle,
        .stMarkdown,
        .stMarkdown p,
        label,
        p,
        span {{
            color: var(--muted) !important;
        }}

        .hero-pill,
        [data-testid="stSidebar"] [data-baseweb="tag"] {{
            background: #E8F5FD !important;
            border: 1px solid #BFE3F7 !important;
            color: #2D9CDB !important;
            border-radius: 4px !important;
        }}

        .method-box {{
            background: #FFFFFF !important;
            border: 1px solid var(--border) !important;
            border-left: 5px solid #2D9CDB !important;
            color: var(--text) !important;
            border-radius: 4px !important;
            box-shadow: 0 1px 2px rgba(16, 24, 40, 0.06) !important;
        }}

        .note-box {{
            background: #FDEDEC !important;
            border: 1px solid #F5B7B1 !important;
            color: var(--text) !important;
            border-radius: 4px !important;
        }}

        .stTabs [data-baseweb="tab-list"] {{
            gap: 0 !important;
            padding: 0 !important;
            border-radius: 4px !important;
        }}

        .stTabs [data-baseweb="tab"] {{
            color: #637381 !important;
            border-radius: 0 !important;
        }}

        .stTabs [aria-selected="true"] {{
            background: #2D9CDB !important;
            color: #FFFFFF !important;
            box-shadow: none !important;
        }}

        button[kind="primary"],
        .stDownloadButton button {{
            background: #2D9CDB !important;
            color: #FFFFFF !important;
            border-radius: 4px !important;
            box-shadow: none !important;
        }}

        div[data-testid="stAlert"] {{
            background: #E8F5FD !important;
            border: 1px solid #BFE3F7 !important;
            color: var(--text) !important;
            border-radius: 4px !important;
        }}

        /* Final layout cleanup */
        [data-testid="stHeader"],
        header[data-testid="stHeader"] {{
            background: #FFFFFF !important;
            border-bottom: 1px solid var(--border) !important;
            box-shadow: 0 1px 2px rgba(16, 24, 40, 0.06) !important;
        }}

        [data-testid="stToolbar"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"],
        header [data-testid="baseButton-header"] {{
            background: #FFFFFF !important;
            color: #2F3B45 !important;
        }}

        header button,
        header svg,
        [data-testid="stToolbar"] button,
        [data-testid="stToolbar"] svg {{
            color: #2F3B45 !important;
            fill: #2F3B45 !important;
        }}

        .block-container {{
            padding-top: 1.15rem !important;
            padding-left: 1.5rem !important;
            padding-right: 1.5rem !important;
        }}

        .hero {{
            border-radius: 4px !important;
            padding: 1.05rem 1.3rem !important;
        }}

        .hero-title {{
            font-size: 1.9rem !important;
            line-height: 1.22 !important;
            max-width: 1120px !important;
        }}

        .hero-copy {{
            font-size: 0.96rem !important;
            color: #52616F !important;
        }}

        .section-head {{
            padding: 0.9rem 1rem !important;
            margin-top: 1rem !important;
            margin-bottom: 0.75rem !important;
        }}

        .section-title {{
            font-size: 1.16rem !important;
        }}

        .stTabs [data-baseweb="tab-list"] {{
            background: #FFFFFF !important;
            border: 1px solid var(--border) !important;
            border-radius: 4px !important;
            box-shadow: 0 1px 2px rgba(16, 24, 40, 0.06) !important;
        }}

        .stTabs [data-baseweb="tab"] {{
            background: #FFFFFF !important;
            color: #2F3B45 !important;
            font-weight: 700 !important;
            border-right: 1px solid #EEF2F6 !important;
        }}

        .stTabs [data-baseweb="tab"] p,
        .stTabs [data-baseweb="tab"] span {{
            color: #2F3B45 !important;
            opacity: 1 !important;
        }}

        .stTabs [aria-selected="true"] {{
            background: #2D9CDB !important;
            border-bottom: 1px solid #2D9CDB !important;
        }}

        .stTabs [aria-selected="true"] p,
        .stTabs [aria-selected="true"] span {{
            color: #FFFFFF !important;
        }}

        [data-testid="stDataFrame"],
        [data-testid="stTable"],
        [data-testid="stCodeBlock"],
        .stCodeBlock,
        pre,
        code {{
            background: #FFFFFF !important;
            color: #2F3B45 !important;
            border-color: var(--border) !important;
        }}

        .stCodeBlock pre,
        .stCodeBlock code,
        pre code {{
            background: #FFFFFF !important;
            color: #2F3B45 !important;
        }}

        [data-testid="stSidebar"] [data-testid="stBaseButton-header"],
        [data-testid="stSidebar"] button,
        [data-testid="stSidebar"] svg {{
            color: #FFFFFF !important;
            fill: #FFFFFF !important;
        }}

        [data-testid="stSidebar"] .stFileUploader label,
        [data-testid="stSidebar"] .stFileUploader span,
        [data-testid="stSidebar"] .stFileUploader p {{
            color: #FFFFFF !important;
            opacity: 1 !important;
        }}

        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzone"] {{
            background: #F8FAFC !important;
            border: 1px solid #D9E2EC !important;
            border-radius: 8px !important;
        }}

        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzone"] label,
        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzone"] span,
        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzone"] p,
        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzone"] small,
        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzoneInstructions"],
        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzoneInstructions"] * {{
            color: #415466 !important;
            opacity: 1 !important;
        }}

        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzone"] button,
        [data-testid="stSidebar"] .stFileUploader [data-testid="stFileUploaderDropzone"] button * {{
            background: #FFFFFF !important;
            border-color: #CBD5E1 !important;
            color: #243746 !important;
            fill: #243746 !important;
        }}

        [data-testid="stSidebar"] [data-testid="stAlert"] {{
            background: #DDF7E8 !important;
            border: 1px solid #8FE0B0 !important;
            color: #145A32 !important;
            border-radius: 6px !important;
        }}

        [data-testid="stSidebar"] [data-testid="stAlert"] div,
        [data-testid="stSidebar"] [data-testid="stAlert"] p,
        [data-testid="stSidebar"] [data-testid="stAlert"] span {{
            color: #145A32 !important;
            opacity: 1 !important;
            font-weight: 750 !important;
        }}

        [data-testid="stSidebar"] [data-testid="stCaptionContainer"],
        [data-testid="stSidebar"] [data-testid="stCaptionContainer"] p,
        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] .stMarkdown p,
        [data-testid="stSidebar"] .stMarkdown strong {{
            color: #E8F1F7 !important;
            opacity: 1 !important;
        }}

        [data-testid="stSidebar"] .stMarkdown strong {{
            font-weight: 800 !important;
        }}

        /* User-requested nav polish */
        [data-testid="collapsedControl"],
        [data-testid="collapsedControl"] button,
        [data-testid="collapsedControl"] svg {{
            color: #2F3B45 !important;
            fill: #2F3B45 !important;
        }}

        [data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"],
        [data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] button,
        [data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] svg,
        [data-testid="stSidebar"] [data-testid="baseButton-headerNoPadding"],
        [data-testid="stSidebar"] [data-testid="baseButton-headerNoPadding"] svg {{
            color: #FFFFFF !important;
            fill: #FFFFFF !important;
        }}

        .block-container {{
            padding-top: 2.25rem !important;
        }}

        .hero {{
            margin-top: 0.75rem !important;
            margin-bottom: 1.35rem !important;
        }}

        .stTabs [data-baseweb="tab-list"] {{
            display: grid !important;
            grid-template-columns: repeat(auto-fit, minmax(12rem, 1fr)) !important;
            gap: 0.5rem !important;
            width: 100% !important;
            padding: 0 !important;
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            margin-top: 1.15rem !important;
            margin-bottom: 1.25rem !important;
        }}

        .stTabs [data-baseweb="tab"] {{
            width: 100% !important;
            min-height: 3.1rem !important;
            height: auto !important;
            justify-content: center !important;
            align-items: center !important;
            background: #FFFFFF !important;
            border: 1px solid var(--border) !important;
            border-radius: 999px !important;
            box-shadow: 0 1px 2px rgba(16, 24, 40, 0.06) !important;
            padding-left: 0.75rem !important;
            padding-right: 0.75rem !important;
            white-space: normal !important;
        }}

        .stTabs [data-baseweb="tab"] p,
        .stTabs [data-baseweb="tab"] span {{
            width: 100% !important;
            text-align: center !important;
            color: #2F3B45 !important;
            font-weight: 750 !important;
            opacity: 1 !important;
        }}

        .stTabs [aria-selected="true"] {{
            background: #2D9CDB !important;
            border: 1px solid #2D9CDB !important;
            border-bottom: 1px solid #2D9CDB !important;
        }}

        .stTabs [aria-selected="true"] p,
        .stTabs [aria-selected="true"] span {{
            color: #FFFFFF !important;
        }}

        /* Harmonized light palette and readable controls */
        :root {{
            color-scheme: light;
        }}

        html,
        body,
        .stApp {{
            background: #F6F8FB !important;
            color: #24313D !important;
        }}

        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.75rem !important;
        }}

        .stTabs [data-baseweb="tab"] {{
            border: 1px solid #DDE6EF !important;
            box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04) !important;
        }}

        .stTabs [aria-selected="true"] {{
            background: #2F9DD8 !important;
            border: 1px solid #2F9DD8 !important;
            border-bottom: 1px solid #2F9DD8 !important;
            box-shadow: 0 4px 10px rgba(47, 157, 216, 0.18) !important;
        }}

        .stTabs [aria-selected="true"]::after,
        .stTabs [data-baseweb="tab-highlight"],
        .stTabs [data-baseweb="tab-border"] {{
            display: none !important;
            background: transparent !important;
            height: 0 !important;
        }}

        [data-baseweb="tag"] {{
            background: #E6F4FB !important;
            border: 1px solid #B9DDF0 !important;
            color: #1D6F99 !important;
            border-radius: 6px !important;
            font-weight: 650 !important;
        }}

        [data-baseweb="tag"] span,
        [data-baseweb="tag"] svg {{
            color: #1D6F99 !important;
            fill: #1D6F99 !important;
        }}

        [data-baseweb="select"] > div {{
            min-height: 46px !important;
            border: 1px solid #D5E0EA !important;
            box-shadow: none !important;
        }}

        [data-baseweb="select"] input,
        [data-baseweb="select"] span,
        [data-baseweb="select"] svg {{
            color: #24313D !important;
            fill: #24313D !important;
        }}

        [data-baseweb="select"] [aria-disabled="true"],
        [data-baseweb="select"] [disabled] {{
            color: #6B7785 !important;
            opacity: 1 !important;
        }}

        .stMarkdown,
        .stMarkdown p,
        label,
        p,
        span,
        div[data-testid="stCaptionContainer"] {{
            color: #3D4A57 !important;
            opacity: 1 !important;
        }}

        .hero-copy,
        .section-subtitle,
        .kpi-note,
        .kpi-label {{
            color: #52616F !important;
            opacity: 1 !important;
        }}

        [data-testid="stDataFrame"],
        [data-testid="stTable"],
        [data-testid="stDataFrame"] div,
        [data-testid="stTable"] div {{
            background: #FFFFFF !important;
            color: #24313D !important;
        }}

        [data-testid="stDataFrame"] canvas {{
            filter: none !important;
        }}

        [data-testid="stDataFrame"] {{
            background: #FFFFFF !important;
            border: 1px solid #DDE6EF !important;
            border-radius: 6px !important;
            overflow: hidden !important;
        }}

        [data-testid="stDataFrame"] div[role="grid"],
        [data-testid="stDataFrame"] div[role="row"],
        [data-testid="stDataFrame"] div[role="gridcell"],
        [data-testid="stDataFrame"] div[role="columnheader"] {{
            background: #FFFFFF !important;
            color: #24313D !important;
        }}

        [data-testid="stCodeBlock"],
        .stCodeBlock,
        pre,
        code {{
            background: #FFFFFF !important;
            color: #24313D !important;
            text-shadow: none !important;
        }}

        .plain-table-wrap {{
            width: 100%;
            overflow-x: auto;
            overflow-y: auto;
            background: #FFFFFF;
            border: 1px solid #DDE6EF;
            border-radius: 6px;
            box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
            margin-bottom: 1.35rem;
        }}

        .plain-table {{
            width: max-content;
            min-width: 100%;
            border-collapse: collapse;
            background: #FFFFFF;
            color: #111827;
            font-size: 0.92rem;
        }}

        .plain-table th {{
            position: sticky;
            top: 0;
            z-index: 1;
            background: #F3F6FA;
            color: #111827;
            font-weight: 750;
            text-align: left;
            padding: 0.68rem 0.78rem;
            border-bottom: 1px solid #DDE6EF;
            white-space: nowrap;
        }}

        .plain-table td {{
            background: #FFFFFF;
            color: #111827;
            padding: 0.62rem 0.78rem;
            border-bottom: 1px solid #E8EEF5;
            vertical-align: top;
            white-space: nowrap;
        }}

        .plain-table tr:nth-child(even) td {{
            background: #F8FAFC;
        }}

        .plain-table tr:hover td {{
            background: #EEF7FD;
        }}

        .plain-table-wrap::-webkit-scrollbar {{
            width: 12px;
            height: 12px;
        }}

        .plain-table-wrap::-webkit-scrollbar-track {{
            background: #DDE6EF;
            border-radius: 999px;
        }}

        .plain-table-wrap::-webkit-scrollbar-thumb {{
            background: #7B8EA3;
            border: 2px solid #DDE6EF;
            border-radius: 999px;
        }}

        .plain-table-wrap::-webkit-scrollbar-thumb:hover {{
            background: #526B82;
        }}

        .plain-table-wrap {{
            scrollbar-color: #7B8EA3 #DDE6EF;
            scrollbar-width: thin;
        }}

        </style>
        """,
        unsafe_allow_html=True,
    )


def style_figure(fig, height: int | None = None):
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FFFFFF",
        font=dict(color=THEME["text"], family="Inter, Segoe UI, Arial, sans-serif", size=13),
        title=dict(font=dict(size=19, color=THEME["primary"]), x=0.02, xanchor="left"),
        margin=dict(l=34, r=28, t=72, b=48),
        legend=dict(
            font=dict(color=THEME["text"], size=12),
            bgcolor="rgba(255,255,255,0.94)",
            bordercolor=THEME["border"],
            borderwidth=1,
        ),
        xaxis=dict(
            color=THEME["text"],
            title_font=dict(color=THEME["muted"]),
            tickfont=dict(color=THEME["text"]),
            gridcolor=THEME["grid"],
            zerolinecolor=THEME["grid"],
        ),
        yaxis=dict(
            color=THEME["text"],
            title_font=dict(color=THEME["muted"]),
            tickfont=dict(color=THEME["text"]),
            gridcolor=THEME["grid"],
            zerolinecolor=THEME["grid"],
        ),
        coloraxis=dict(
            colorbar=dict(
                title_font=dict(color=THEME["text"]),
                tickfont=dict(color=THEME["text"]),
                outlinecolor=THEME["border"],
            )
        ),
    )
    fig.update_coloraxes(
        colorbar_title_font_color=THEME["text"],
        colorbar_tickfont_color=THEME["text"],
        colorbar_outlinecolor=THEME["border"],
    )
    fig.update_xaxes(linecolor=THEME["border"], mirror=False)
    fig.update_yaxes(linecolor=THEME["border"], mirror=False)
    if height is not None:
        fig.update_layout(height=height)
    return fig


def find_default_dataset() -> Optional[Path]:
    for path in DEFAULT_DATA_PATHS:
        if path.exists():
            return path
    return None


def find_default_hms_datasets() -> List[Path]:
    return [path for path in DEFAULT_HMS_PATHS if path.exists()]


def is_hms_header(columns: List[str]) -> bool:
    normalized = {str(col).strip().lower() for col in columns}
    return bool({"deprawsc", "anx_score", "sui_idea", "yr_sch", "inst_hmsyear"} & normalized)


def mark_source(df: pd.DataFrame, source_type: str, source_name: str) -> pd.DataFrame:
    out = df.copy()
    out[INTERNAL_SOURCE_TYPE_COLUMN] = source_type
    out[INTERNAL_SOURCE_FILE_COLUMN] = source_name
    return out


def hms_usecols(path_or_buffer) -> List[str]:
    header = pd.read_csv(path_or_buffer, nrows=0).columns.tolist()
    return [col for col in header if str(col).strip() in HMS_SCOPED_COLUMNS]


@st.cache_data(show_spinner=False)
def load_csv_from_bytes(file_bytes: bytes) -> pd.DataFrame:
    header = pd.read_csv(BytesIO(file_bytes), nrows=0).columns.tolist()
    if is_hms_header(header):
        usecols = [col for col in header if str(col).strip() in HMS_SCOPED_COLUMNS]
        df = pd.read_csv(BytesIO(file_bytes), usecols=usecols, low_memory=False)
        return mark_source(df, "hms", "Uploaded HMS")
    df = pd.read_csv(BytesIO(file_bytes), low_memory=False)
    return mark_source(df, "mental_school", "Uploaded Mental School")


def read_csv_from_path_uncached(path: str) -> pd.DataFrame:
    header = pd.read_csv(path, nrows=0).columns.tolist()
    if is_hms_header(header):
        usecols = [col for col in header if str(col).strip() in HMS_SCOPED_COLUMNS]
        df = pd.read_csv(path, usecols=usecols, low_memory=False)
        return mark_source(df, "hms", Path(path).name)
    df = pd.read_csv(path, low_memory=False)
    return mark_source(df, "mental_school", Path(path).name)


@st.cache_data(show_spinner=False)
def load_csv_from_path(path: str) -> pd.DataFrame:
    return read_csv_from_path_uncached(path)


@st.cache_data(show_spinner=False)
def load_default_combined_data(default_path: str, hms_paths: Tuple[str, ...]) -> pd.DataFrame:
    frames = [load_csv_from_path(default_path)]
    for hms_path in hms_paths:
        frames.append(load_csv_from_path(hms_path))
    return pd.concat(frames, ignore_index=True, sort=False)


def prepare_board_data(df: pd.DataFrame) -> BoardPreparedData:
    processed = preprocess_yrbs_data(df)
    needed_columns = [
        column
        for column in ["Target", *RESEARCH_FEATURES]
        if column in processed.cleaned.columns
    ]
    return BoardPreparedData(
        raw_analysis=processed.raw_analysis,
        cleaned=processed.cleaned[needed_columns].copy(),
    )


@st.cache_resource(show_spinner="Đang tiền xử lý dữ liệu tải lên...")
def cached_preprocess(df: pd.DataFrame, pipeline_version: str) -> BoardPreparedData:
    return prepare_board_data(df)


def source_signature(paths: List[Path]) -> str:
    return "||".join(
        f"{path.as_posix()}::{path.stat().st_size}::{path.stat().st_mtime_ns}"
        for path in paths
    )


def processed_cache_path(signature: str, pipeline_version: str) -> Path:
    token = hashlib.sha256(f"{pipeline_version}::{signature}".encode("utf-8")).hexdigest()[:24]
    return LOCAL_CACHE_DIR / f"board_processed_{token}.pkl"


def gold_survey_cache_path(manifest: Tuple[Tuple[str, str, int, int], ...]) -> Path:
    signature = json.dumps(manifest, ensure_ascii=True, sort_keys=True)
    token = hashlib.sha256(f"{PIPELINE_VERSION}::gold::{signature}".encode("utf-8")).hexdigest()[:24]
    return LOCAL_CACHE_DIR / f"gold_survey_{token}.pkl"


@st.cache_resource(show_spinner="Đang nạp dữ liệu dashboard...")
def load_default_prepared_data(
    default_path: Optional[str],
    hms_paths: Tuple[str, ...],
    signature: str,
    pipeline_version: str,
) -> Tuple[BoardPreparedData, Tuple[int, int], bool]:
    cache_path = processed_cache_path(signature, pipeline_version)
    if cache_path.exists():
        try:
            payload = pd.read_pickle(cache_path)
            if (
                payload.get("signature") == signature
                and payload.get("pipeline_version") == pipeline_version
            ):
                board_data = BoardPreparedData(
                    raw_analysis=payload["raw_analysis"],
                    cleaned=payload["cleaned"],
                )
                return board_data, tuple(payload["source_shape"]), True
        except (OSError, ValueError, EOFError, KeyError, AttributeError):
            pass

    frames = []
    if default_path is not None:
        frames.append(read_csv_from_path_uncached(default_path))
    frames.extend(read_csv_from_path_uncached(path) for path in hms_paths)
    raw_df = pd.concat(frames, ignore_index=True, sort=False)
    source_shape = tuple(raw_df.shape)
    board_data = prepare_board_data(raw_df)

    LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pd.to_pickle(
        {
            "signature": signature,
            "pipeline_version": pipeline_version,
            "source_shape": source_shape,
            "raw_analysis": board_data.raw_analysis,
            "cleaned": board_data.cleaned,
        },
        cache_path,
    )
    return board_data, source_shape, False


def blob_partition_value(blob_name: str, key: str) -> str:
    token = f"{key}="
    for segment in blob_name.split("/"):
        if segment.startswith(token):
            return segment[len(token) :]
    return ""


@st.cache_data(ttl=60, show_spinner=False)
def survey_gold_tables_manifest() -> Tuple[Tuple[Tuple[str, str, int, int], ...], str]:
    try:
        client = create_gcs_storage_client()
        try:
            manifest_blob = client.bucket(GCS_BUCKET_NAME).blob(GOLD_SURVEY_CURRENT_MANIFEST)
            if manifest_blob.exists():
                manifest = json.loads(manifest_blob.download_as_text())
                entries = []
                for table_name, table_entries in manifest.get("tables", {}).items():
                    for entry in table_entries:
                        entries.append(
                            (
                                table_name,
                                str(entry["name"]),
                                int(entry.get("generation") or 0),
                                int(entry.get("size") or 0),
                            )
                        )
                if entries and REQUIRED_GOLD_SURVEY_TABLES.issubset({entry[0] for entry in entries}):
                    return tuple(sorted(entries)), ""
        except Exception:
            # Fall back to scanning versioned table prefixes if the pointer is absent or stale.
            pass
        candidates_by_table: Dict[str, List[Tuple[str, str, str, int, int]]] = {}
        for table_name, prefix in GOLD_SURVEY_TABLE_PREFIXES.items():
            candidates = []
            for blob in client.list_blobs(GCS_BUCKET_NAME, prefix=prefix):
                if not blob.name.lower().endswith(".parquet") or int(blob.size or 0) <= 0:
                    continue
                candidates.append(
                    (
                        blob_partition_value(blob.name, "run_id"),
                        table_name,
                        blob.name,
                        int(blob.generation or 0),
                        int(blob.size or 0),
                    )
                )
            candidates_by_table[table_name] = candidates

        def latest_table_entries(
            candidates: List[Tuple[str, str, str, int, int]]
        ) -> List[Tuple[str, str, int, int]]:
            if not candidates:
                return []
            versioned: Dict[str, List[Tuple[str, str, int, int]]] = {}
            legacy_entries: List[Tuple[str, str, int, int]] = []
            for run_id, table, name, generation, size in candidates:
                entry = (table, name, generation, size)
                if run_id:
                    versioned.setdefault(run_id, []).append(entry)
                else:
                    legacy_entries.append(entry)
            if versioned:
                latest_run_id = sorted(
                    (
                        max(generation for _table, _name, generation, _size in entries),
                        run_id,
                    )
                    for run_id, entries in versioned.items()
                )[-1][1]
                return versioned[latest_run_id]
            return legacy_entries

        versioned_runs: Dict[str, Dict[str, List[Tuple[str, str, int, int]]]] = {}
        for table_name, candidates in candidates_by_table.items():
            for run_id, table, name, generation, size in candidates:
                if not run_id:
                    continue
                versioned_runs.setdefault(run_id, {}).setdefault(table_name, []).append(
                    (table, name, generation, size)
                )

        complete_runs = []
        required_tables = REQUIRED_GOLD_SURVEY_TABLES
        for run_id, table_entries in versioned_runs.items():
            if required_tables.issubset(table_entries):
                newest_generation = max(
                    generation
                    for table_name, entries_for_table in table_entries.items()
                    if table_name in required_tables
                    for _table, _name, generation, _size in entries_for_table
                )
                complete_runs.append((newest_generation, run_id))

        entries: List[Tuple[str, str, int, int]] = []
        if complete_runs:
            _generation, latest_run_id = sorted(complete_runs)[-1]
            for table_name in GOLD_SURVEY_TABLE_PREFIXES:
                same_run_entries = versioned_runs[latest_run_id].get(table_name)
                if same_run_entries:
                    entries.extend(same_run_entries)
                elif table_name not in required_tables:
                    entries.extend(latest_table_entries(candidates_by_table.get(table_name, [])))
            return tuple(sorted(entries)), ""

        for table_name, candidates in candidates_by_table.items():
            table_entries = latest_table_entries(candidates)
            if table_name in required_tables and not table_entries:
                return tuple(), f"Gold survey thiếu bảng bắt buộc: {table_name}"
            entries.extend(table_entries)
        return tuple(sorted(entries)), ""
    except Exception as exc:
        return tuple(), str(exc)


def normalize_gold_analytic_for_dashboard(gold: pd.DataFrame) -> pd.DataFrame:
    out = gold.copy()
    if "source_group" in out.columns:
        source_group = out["source_group"].astype(str).str.lower()
    else:
        source_group = pd.Series("unknown", index=out.index)
        out["source_group"] = source_group

    if POPULATION_COLUMN not in out.columns:
        out[POPULATION_COLUMN] = np.select(
            [source_group.eq("school"), source_group.eq("university")],
            [MENTAL_SCHOOL_POPULATION_LABEL, HMS_POPULATION_LABEL],
            default="Unknown",
        )
    if DATA_SOURCE_COLUMN not in out.columns:
        if "source_dataset" in out.columns:
            out[DATA_SOURCE_COLUMN] = out["source_dataset"].astype(str)
        elif "source_file" in out.columns:
            out[DATA_SOURCE_COLUMN] = out["source_file"].astype(str).str.replace(r"\.csv$", "", regex=True)
        else:
            out[DATA_SOURCE_COLUMN] = out["source_group"].astype(str)
    if "q1" not in out.columns and "age" in out.columns:
        age = pd.to_numeric(out["age"], errors="coerce")
        out["q1"] = np.select(
            [
                age < 13,
                age.eq(13),
                age.eq(14),
                age.eq(15),
                age.eq(16),
                age.eq(17),
                age.eq(18),
                age.between(19, 20),
                age.between(21, 24),
                age.between(25, 34),
                age >= 35,
            ],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            default=np.nan,
        )
    if "q2" not in out.columns:
        gender_col = "gender" if "gender" in out.columns else "sex" if "sex" in out.columns else None
        if gender_col:
            gender = out[gender_col].astype(str).str.lower()
            out["q2"] = np.select(
                [gender.eq("female"), gender.eq("male"), gender.isin(["other", "unknown"])],
                [1, 2, 3],
                default=np.nan,
            )
    if "q3" not in out.columns and "grade" in out.columns:
        out["q3"] = pd.to_numeric(out["grade"], errors="coerce")

    if "Target" not in out.columns:
        risk = out.get("risk_related_flag", pd.Series(False, index=out.index)).fillna(False).astype(bool)
        high_score = out.get("high_score_flag", pd.Series(False, index=out.index)).fillna(False).astype(bool)
        out["Target"] = (risk | high_score).astype(int)

    for feature in RESEARCH_FEATURES:
        if feature not in out.columns:
            out[feature] = np.nan
        out[feature] = pd.to_numeric(out[feature], errors="coerce")

    out["Target"] = pd.to_numeric(out["Target"], errors="coerce").fillna(0).astype(int)
    return out


def gold_table_status(gold_tables: Dict[str, pd.DataFrame]) -> str:
    parts = [f"{name}={len(frame):,}" for name, frame in sorted(gold_tables.items())]
    return "Gold tables loaded: " + ", ".join(parts)


def partition_values_from_blob_name(blob_name: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for segment in blob_name.split("/"):
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        if key and value:
            values[key] = value
    return values


@st.cache_resource(show_spinner="Đang nạp Gold survey dashboard tables từ Cloud Storage...")
def load_gold_survey_prepared_data(
    manifest: Tuple[Tuple[str, str, int, int], ...],
) -> Tuple[BoardPreparedData, Tuple[int, int], str]:
    cache_path = gold_survey_cache_path(manifest)
    if cache_path.exists():
        try:
            payload = pd.read_pickle(cache_path)
            if payload.get("manifest") == manifest and payload.get("pipeline_version") == PIPELINE_VERSION:
                board_data = BoardPreparedData(
                    raw_analysis=payload["raw_analysis"],
                    cleaned=payload["cleaned"],
                    gold_tables=payload["gold_tables"],
                )
                return board_data, tuple(payload["input_shape"]), str(payload["gold_status"])
        except (OSError, ValueError, EOFError, KeyError, AttributeError):
            pass

    client = create_gcs_storage_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    grouped: Dict[str, List[str]] = {table_name: [] for table_name in GOLD_SURVEY_TABLE_PREFIXES}
    for table_name, blob_name, _generation, _size in manifest:
        grouped.setdefault(table_name, []).append(blob_name)

    gold_tables: Dict[str, pd.DataFrame] = {}
    for table_name, blob_names in grouped.items():
        frames = []
        for name in sorted(blob_names):
            frame = pd.read_parquet(BytesIO(bucket.blob(name).download_as_bytes()))
            for column, value in partition_values_from_blob_name(name).items():
                if column not in frame.columns:
                    frame[column] = value
            frames.append(frame)
        if not frames:
            if table_name in REQUIRED_GOLD_SURVEY_TABLES:
                raise ValueError(f"Gold survey thiếu dữ liệu bảng {table_name}")
            continue
        gold_tables[table_name] = pd.concat(frames, ignore_index=True, sort=False)

    if "survey_analytic_features" not in gold_tables:
        raise ValueError("Gold survey thiếu bảng survey_analytic_features")
    gold = normalize_gold_analytic_for_dashboard(gold_tables["survey_analytic_features"])
    required = ["Target", *RESEARCH_FEATURES]
    missing = [column for column in required if column not in gold.columns]
    if missing:
        raise ValueError(f"Gold analytic features thiếu cột bắt buộc: {missing}")

    board_data = BoardPreparedData(
        raw_analysis=gold,
        cleaned=gold[required].copy(),
        gold_tables=gold_tables,
    )
    input_shape = tuple(gold.shape)
    status_text = gold_table_status(gold_tables)

    LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pd.to_pickle(
        {
            "manifest": manifest,
            "pipeline_version": PIPELINE_VERSION,
            "input_shape": input_shape,
            "gold_status": status_text,
            "raw_analysis": board_data.raw_analysis,
            "cleaned": board_data.cleaned,
            "gold_tables": board_data.gold_tables,
        },
        cache_path,
    )
    return board_data, input_shape, status_text


def load_data_ui() -> Tuple[Optional[BoardPreparedData], str, Tuple[int, int], str]:
    manifest, gold_error = survey_gold_tables_manifest()
    if manifest:
        try:
            board_data, input_shape, gold_status = load_gold_survey_prepared_data(manifest)
            return (
                board_data,
                "Gold survey dashboard tables từ Cloud Storage",
                input_shape,
                gold_status,
            )
        except Exception as exc:
            st.error(f"Không đọc được dữ liệu khảo sát đã xử lý: {exc}")
            return None, "", (0, 0), ""

    if gold_error:
        st.error(f"Không đọc được dữ liệu khảo sát đã xử lý: {gold_error}")
    else:
        st.error("Không tìm thấy dữ liệu khảo sát đã xử lý để hiển thị.")
    return None, "", (0, 0), ""


def options_from_q(df: pd.DataFrame, qnum: int) -> List[str]:
    col = find_q_col(df, qnum)
    if not col:
        return []
    values = df[col].apply(lambda value: value_to_label(value, qnum)).drop_duplicates().tolist()
    values = [value for value in values if value != "Missing"]
    return sorted(values, key=str)


def main_navigation() -> str:
    selected_page = st.segmented_control(
        "Trang phân tích",
        list(BOARD_NAV_PAGES),
        default="Tổng quan",
        selection_mode="single",
        label_visibility="collapsed",
        key="board_main_page",
    )
    return selected_page or "Tổng quan"


def sidebar_description_filters(processed) -> Tuple[List[str], List[str], List[str], List[str], List[str], List[str]]:
    return [], [], [], [], [], []


def sidebar_status(
    source_text: str,
    source_shape: Tuple[int, int],
    filtered_df: pd.DataFrame,
    cache_status: str,
) -> None:
    return None


def hero() -> None:
    st.markdown(
        """
        <div class="hero">
            <div class="hero-pill">Học sinh & sinh viên • Dashboard phân tích</div>
            <div class="hero-title">Student Mental Health Analytics</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def method_box() -> None:
    st.markdown(
        f"""
        <div class="method-box">
            <b>Biến kết quả đang phân tích:</b> {TARGET_DEFINITION}
            <br>
            Diễn giải các kết quả như mối liên hệ trong dữ liệu khảo sát, không phải kết luận nhân quả hay chẩn đoán cá nhân.
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi_card(label: str, value: str, note: str) -> None:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_head(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="section-head">
            <div class="section-title">{title}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def insight_box(question: str, takeaway: str, caution: str | None = None) -> None:
    return None


def research_question_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                "1",
                "Quy mô nhóm có dấu hiệu nguy cơ là bao nhiêu?",
                "KPI, donut tỷ trọng nguy cơ, phân bố nhóm học sinh/sinh viên",
                "Ước lượng quy mô vấn đề trong nhóm đang lọc.",
            ],
            [
                "2",
                "Bối cảnh tâm lý - giáo dục nào nổi bật ở nhóm có nguy cơ?",
                "So sánh điểm trung bình giữa nhóm có nguy cơ và nhóm còn lại",
                "Hiểu dữ liệu đang nói về bối cảnh tâm lý - giáo dục nào.",
            ],
            [
                "3",
                "Khi một khía cạnh xấu đi, nguy cơ tăng bao nhiêu?",
                "Tỷ lệ nguy cơ theo mức thấp/cao của từng khía cạnh",
                "Đọc mối liên hệ mô tả: điểm bối cảnh cao hơn đi cùng nguy cơ cao hơn hay thấp hơn.",
            ],
            [
                "4",
                "Nhóm học sinh/sinh viên nào cần chú ý trước?",
                "Phân tầng theo tuổi, giới tính, bậc/lớp",
                "Ưu tiên nhóm cần phân tích sâu hoặc cần hỗ trợ trong bối cảnh giáo dục.",
            ],
            [
                "5",
                "Nhóm nào có nhu cầu hỗ trợ cao nhưng bối cảnh hỗ trợ còn thiếu?",
                "Ma trận At Risk Rate so với điểm thiếu hụt hỗ trợ theo tuổi, giới tính, bậc/lớp",
                "Ưu tiên nguồn lực theo khoảng cách nhu cầu - hỗ trợ thay vì chỉ nhìn nhóm đông nhất.",
            ],
            [
                "6",
                "Kết quả này có đáng tin để đưa vào báo cáo không?",
                "Thiếu dữ liệu, ngoại lệ tuổi sinh viên, độ phủ khía cạnh",
                "Biết phần nào là tín hiệu mạnh, phần nào chỉ nên xem như gợi ý mô tả.",
            ],
        ],
        columns=["Thứ tự", "Câu hỏi phân tích", "Biểu đồ/Số liệu trả lời", "Ý nghĩa khi đọc"],
    )


def construct_definition_table() -> pd.DataFrame:
    rows = []
    for feature, definition in RESEARCH_FEATURE_DEFINITIONS.items():
        rows.append(
            {
                "Khía cạnh": construct_label(feature),
                "Nó nói gì về dữ liệu?": CONSTRUCT_ANALYST_MEANING.get(feature, definition["meaning"]),
                "Cách đọc trên dashboard": "Điểm càng cao nghĩa là bất lợi, rủi ro hoặc thiếu hụt trong bối cảnh đó càng lớn.",
                "Dữ liệu thành phần": f"{len(definition['qnums'])} tín hiệu khảo sát học sinh + tín hiệu sinh viên tương thích",
            }
        )
    return pd.DataFrame(rows)


QUALITY_METRIC_LABELS = {
    "Rows loaded": "Số bản ghi được nạp",
    "Analysable rows after target validation": "Số bản ghi đủ nhãn nguy cơ",
    "Raw columns loaded": "Số cột dữ liệu gốc",
    "Columns after preprocessing": "Số trường sau chuẩn hóa",
    "Research-scope survey questions": "Số yếu tố khảo sát trong phạm vi phân tích",
    "Research constructs": "Số khía cạnh phân tích",
    "Missing cells after preprocessing": "Số ô còn thiếu sau chuẩn hóa",
    "Dropped metadata/survey design columns": "Trường hệ thống đã loại khỏi dashboard",
    "Data sources": "Số nguồn dữ liệu",
    "HMS age outliers set to missing": "Số ngoại lệ tuổi sinh viên đã chuyển thành thiếu",
    "Average HMS scoped missing (%)": "Thiếu dữ liệu trung bình ở nhóm sinh viên (%)",
}


def semantic_quality_summary(raw_df: pd.DataFrame, processed) -> pd.DataFrame:
    quality = compact_data_quality_summary(raw_df, processed)
    if quality.empty:
        return quality
    quality = quality[quality["Metric"] != "Model features"].copy()
    quality["Metric"] = quality["Metric"].replace(QUALITY_METRIC_LABELS)
    quality["Metric"] = quality["Metric"].str.replace("Rows - ", "Số bản ghi - ", regex=False)
    quality["Value"] = quality["Value"].replace({"None": "Không có"})
    return quality


HMS_CONSTRUCT_SOURCE_COLUMNS = {
    "Family Pressure Index": [
        "housing_worry",
        "food_worry",
        "fincur",
        "finpast",
        "afford_school",
        "afford_food",
        "afford_transp",
        "afford_hc",
        "afford_books",
        "afford_house",
        "pay_worry",
        "pay_worry1",
        "pay_worry2",
        "pay_worry3",
        "fam_support_aca",
        "prof_support_aca",
    ],
    "Academic Pressure Index": [
        "aca_impa",
        "stress1",
        "stress2",
        "stress3",
        "stress4",
        "compet_sch",
        "grade_curv",
        "imposter_1",
        "imposter_2",
        "imposter_3",
        "imposter_4",
        "imposter_5",
        "failed",
        "adjust_aca_1",
        "adjust_aca_2",
        "time_manage",
        "doubt_school_1",
    ],
    "Peer & Safety Stress Index": [
        "belong1",
        "belong2",
        "belong8",
        "belong9",
        "discrim_race",
        "discrim_culture",
        "discrim_gender",
        "discrim_sexual",
        "discrim_other",
        "safe_on_day",
        "safe_on_night",
        "safe_off_day",
        "safe_off_night",
        "hostcli_distress",
    ],
    "Trauma Exposure Index": [
        "abuse_life",
        "abuse_recent",
        "stalk_exp",
        "assault_sex",
        "sa_exp",
        "IPV_1",
        "IPV_2",
        "IPV_3",
        "IPV_4",
        "IPV_5",
        "partner_phys",
        "partner_insult",
        "partner_threat",
        "partner_curse",
    ],
    "Substance Coping Risk Index": [
        "alc_any",
        "binge_fr",
        "sub_any",
        "sub_cig",
        "smok_freq",
        "smok_vape",
        "drug_mar",
        "mar_freq",
    ],
    "Lifestyle Recovery Deficit": [
        "sleep_wknight",
        "sleep_wkend",
        "exerc",
        "exerc_range5",
        "exerc_range4",
        "food_worry",
    ],
}


def hms_construct_source_table(construct: str) -> pd.DataFrame:
    columns = HMS_CONSTRUCT_SOURCE_COLUMNS.get(construct, [])
    if not columns:
        return pd.DataFrame(columns=["Khía cạnh", "Ý nghĩa dữ liệu ở nhóm sinh viên", "Cách quy đổi"])
    return pd.DataFrame(
        [
            {
                "Khía cạnh": construct_label(construct),
                "Ý nghĩa dữ liệu ở nhóm sinh viên": HMS_CONSTRUCT_SOURCE_MEANINGS.get(
                    construct,
                    CONSTRUCT_ANALYST_MEANING.get(construct, construct),
                ),
                "Cách quy đổi": "Các tín hiệu ở nhóm sinh viên được đưa về thang 0-100 cùng chiều với nhóm học sinh để so sánh được giữa hai nhóm.",
            }
        ]
    )


def construct_impact_table(df: pd.DataFrame, score_cols: List[str]) -> pd.DataFrame:
    columns = [
        "Construct",
        "At Risk ở mức thấp (%)",
        "At Risk ở mức cao (%)",
        "Thay đổi khi construct tăng (%)",
        "Diễn giải",
    ]
    rows = []
    for score in score_cols:
        if score not in df.columns:
            continue
        valid = df[[score, "Target"]].dropna()
        if valid.empty or valid[score].nunique() < 2:
            continue

        low_threshold = float(valid[score].quantile(0.25))
        high_threshold = float(valid[score].quantile(0.75))
        if low_threshold < high_threshold:
            low_group = valid[valid[score] <= low_threshold]
            high_group = valid[valid[score] >= high_threshold]
        else:
            # Sparse indicators can have the same Q1/Q3 baseline; compare
            # baseline records against every record above that baseline.
            low_group = valid[valid[score] <= low_threshold]
            high_group = valid[valid[score] > high_threshold]
            if high_group.empty:
                low_group = valid[valid[score] < low_threshold]
                high_group = valid[valid[score] >= high_threshold]

        if low_group.empty or high_group.empty:
            continue
        low_rate = float(low_group["Target"].mean() * 100)
        high_rate = float(high_group["Target"].mean() * 100)
        gap = high_rate - low_rate
        rows.append(
            {
                "Construct": score,
                "At Risk ở mức thấp (%)": round(low_rate, 2),
                "At Risk ở mức cao (%)": round(high_rate, 2),
                "Thay đổi khi construct tăng (%)": round(gap, 2),
                "Diễn giải": "Tăng" if gap > 0 else "Giảm" if gap < 0 else "Gần như không đổi",
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns).sort_values("Thay đổi khi construct tăng (%)", ascending=False).reset_index(drop=True)


def construct_profile_table(df: pd.DataFrame, score_cols: List[str]) -> pd.DataFrame:
    columns = ["Construct", "Lower Risk mean", "At Risk mean", "Difference"]
    if "Target" not in df.columns:
        return pd.DataFrame(columns=columns)
    rows = []
    for score in score_cols:
        if score not in df.columns:
            continue
        lower = df.loc[df["Target"] == 0, score].mean()
        risk = df.loc[df["Target"] == 1, score].mean()
        if pd.isna(lower) or pd.isna(risk):
            continue
        rows.append(
            {
                "Construct": score,
                "Lower Risk mean": round(float(lower), 2),
                "At Risk mean": round(float(risk), 2),
                "Difference": round(float(risk - lower), 2),
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns).sort_values("Difference", ascending=False).reset_index(drop=True)


def construct_trend_table(df: pd.DataFrame, score_col: str, bins: int = 4) -> pd.DataFrame:
    if score_col not in df.columns or "Target" not in df.columns:
        return pd.DataFrame(columns=["Mức construct", "At Risk Rate", "n", "Mean score"])
    tmp = df[[score_col, "Target"]].dropna().copy()
    if tmp.empty:
        return pd.DataFrame(columns=["Mức construct", "At Risk Rate", "n", "Mean score"])

    labels = ["Thấp", "Trung bình thấp", "Trung bình cao", "Cao"][:bins]
    try:
        tmp["Mức construct"] = pd.qcut(tmp[score_col], q=bins, labels=labels, duplicates="drop")
    except ValueError:
        tmp["Mức construct"] = pd.cut(tmp[score_col], bins=bins, labels=labels, duplicates="drop")

    out = (
        tmp.groupby("Mức construct", observed=False, as_index=False)
        .agg(
            **{
                "At Risk Rate": ("Target", "mean"),
                "n": ("Target", "size"),
                "Mean score": (score_col, "mean"),
            }
        )
        .dropna(subset=["Mức construct"])
    )
    out["At Risk Rate"] = (out["At Risk Rate"] * 100).round(2)
    out["Mean score"] = out["Mean score"].round(2)
    return out


def demographic_construct_table(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    qnum: int,
    group_name: str,
    score_cols: List[str],
) -> pd.DataFrame:
    col = find_q_col(raw_df, qnum)
    if col is None:
        return pd.DataFrame(columns=[group_name, "Construct", "Mean score", "At Risk Rate", "n"])

    tmp = cleaned_df[score_cols + ["Target"]].copy()
    tmp[group_name] = raw_df[col].apply(lambda value: value_to_label(value, qnum))
    tmp = tmp[tmp[group_name] != "Missing"].copy()
    long_df = tmp.melt(
        id_vars=[group_name, "Target"],
        value_vars=score_cols,
        var_name="Construct",
        value_name="Score",
    ).dropna(subset=["Score"])

    result = (
        long_df.groupby([group_name, "Construct"], as_index=False)
        .agg(
            **{
                "Mean score": ("Score", "mean"),
                "At Risk Rate": ("Target", "mean"),
                "n": ("Target", "size"),
            }
        )
    )
    result["Mean score"] = result["Mean score"].round(2)
    result["At Risk Rate"] = (result["At Risk Rate"] * 100).round(2)
    return result


def demographic_mental_summary(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    qnum: int,
    group_name: str,
    score_cols: List[str],
) -> pd.DataFrame:
    columns = [
        group_name,
        "n",
        "At Risk Rate",
        "Top pressure construct",
        "Top construct score",
        "Most elevated construct",
        "Elevation vs overall",
    ]
    col = find_q_col(raw_df, qnum)
    if col is None or "Target" not in cleaned_df.columns:
        return pd.DataFrame(columns=columns)

    tmp = cleaned_df[["Target"] + score_cols].copy()
    tmp[group_name] = raw_df[col].apply(lambda value: value_to_label(value, qnum))
    tmp = tmp[tmp[group_name] != "Missing"].copy()
    if tmp.empty:
        return pd.DataFrame(columns=columns)
    overall_means = tmp[score_cols].mean(numeric_only=True)
    rows = []
    for group_value, group_df in tmp.groupby(group_name, dropna=False):
        if group_df.empty:
            continue
        construct_means = group_df[score_cols].mean(numeric_only=True)
        if construct_means.empty or construct_means.dropna().empty:
            top_construct = ""
            top_score = np.nan
            elevated_construct = ""
            elevation = np.nan
        else:
            top_construct = str(construct_means.idxmax())
            top_score = float(construct_means.loc[top_construct])
            diff = construct_means - overall_means
            elevated_construct = str(diff.idxmax())
            elevation = float(diff.loc[elevated_construct])
        rows.append(
            {
                group_name: str(group_value),
                "n": int(len(group_df)),
                "At Risk Rate": round(float(group_df["Target"].mean() * 100), 2),
                "Top pressure construct": top_construct,
                "Top construct score": round(top_score, 2) if pd.notna(top_score) else np.nan,
                "Most elevated construct": elevated_construct,
                "Elevation vs overall": round(elevation, 2) if pd.notna(elevation) else np.nan,
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns).sort_values("At Risk Rate", ascending=False).reset_index(drop=True)


def all_demographic_mental_summary(raw_df: pd.DataFrame, cleaned_df: pd.DataFrame, score_cols: List[str]) -> pd.DataFrame:
    frames = []
    for label, qnum, col_name in [("Giới tính", 2, "Gender"), ("Bậc/lớp", 3, "Grade"), ("Độ tuổi", 1, "Age")]:
        summary = demographic_mental_summary(raw_df, cleaned_df, qnum, col_name, score_cols)
        if summary.empty:
            continue
        summary = summary.rename(columns={col_name: "Group"})
        summary.insert(0, "Dimension", label)
        frames.append(summary)
    if not frames:
        return pd.DataFrame(
            columns=[
                "Dimension",
                "Group",
                "n",
                "At Risk Rate",
                "Top pressure construct",
                "Top construct score",
                "Most elevated construct",
                "Elevation vs overall",
            ]
        )
    return pd.concat(frames, ignore_index=True).sort_values("At Risk Rate", ascending=False).reset_index(drop=True)


def group_construct_profile(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    qnum: int,
    group_name: str,
    selected_group: str,
    score_cols: List[str],
) -> pd.DataFrame:
    col = find_q_col(raw_df, qnum)
    if col is None:
        return pd.DataFrame(columns=["Construct", "Selected group", "Overall", "Difference"])
    labels = raw_df[col].apply(lambda value: value_to_label(value, qnum))
    selected_mask = labels == selected_group
    if not selected_mask.any():
        return pd.DataFrame(columns=["Construct", "Selected group", "Overall", "Difference"])
    rows = []
    for score in score_cols:
        group_mean = cleaned_df.loc[selected_mask, score].mean()
        overall_mean = cleaned_df[score].mean()
        if pd.isna(group_mean) or pd.isna(overall_mean):
            continue
        rows.append(
            {
                "Construct": score,
                "Selected group": round(float(group_mean), 2),
                "Overall": round(float(overall_mean), 2),
                "Difference": round(float(group_mean - overall_mean), 2),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["Construct", "Selected group", "Overall", "Difference"])
    return pd.DataFrame(rows).sort_values("Difference", ascending=False).reset_index(drop=True)


def construct_source_question_table(processed, construct: str) -> pd.DataFrame:
    definition = RESEARCH_FEATURE_DEFINITIONS.get(construct, {})
    rows = []
    for qnum in definition.get("qnums", []):
        col = find_q_col(processed.raw_analysis, qnum)
        if col is None:
            continue
        rows.append(
            {
                "qnum": qnum,
                "question": processed.raw_col_to_display.get(col, col),
                "Nó góp nghĩa gì?": "Được quy đổi về cùng chiều: mức cao hơn phản ánh nhiều bất lợi/rủi ro hơn trong khía cạnh này.",
                "Khía cạnh": construct_label(construct),
            }
        )
    return pd.DataFrame(rows)


def group_source_response_patterns(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    group_qnum: int,
    selected_group: str,
    construct: str,
) -> pd.DataFrame:
    columns = [
        "qnum",
        "Question",
        "Response",
        "Selected group %",
        "Overall %",
        "Difference",
        "Selected group n",
        "Overall n",
        "Selected group At Risk %",
        "Overall At Risk %",
    ]
    group_col = find_q_col(raw_df, group_qnum)
    if group_col is None or "Target" not in cleaned_df.columns:
        return pd.DataFrame(columns=columns)

    group_labels = raw_df[group_col].apply(lambda value: value_to_label(value, group_qnum))
    selected_mask = group_labels == selected_group
    if not selected_mask.any():
        return pd.DataFrame(columns=columns)

    rows = []
    for qnum in RESEARCH_FEATURE_DEFINITIONS.get(construct, {}).get("qnums", []):
        q_col = find_q_col(raw_df, qnum)
        if q_col is None:
            continue
        question_labels = raw_df[q_col].apply(lambda value: value_to_label(value, qnum))
        tmp = pd.DataFrame(
            {
                "Response": question_labels,
                "Target": cleaned_df["Target"],
                "Selected": selected_mask,
            }
        )
        tmp = tmp[tmp["Response"] != "Missing"].copy()
        selected_total = int(tmp["Selected"].sum())
        overall_total = int(len(tmp))
        if selected_total == 0 or overall_total == 0:
            continue

        for response in sorted(tmp["Response"].dropna().unique().tolist(), key=str):
            selected_response = tmp[tmp["Selected"] & (tmp["Response"] == response)]
            overall_response = tmp[tmp["Response"] == response]
            selected_n = int(len(selected_response))
            overall_n = int(len(overall_response))
            selected_pct = selected_n / selected_total * 100 if selected_total else 0.0
            overall_pct = overall_n / overall_total * 100 if overall_total else 0.0
            selected_risk = selected_response["Target"].mean() * 100 if selected_n else np.nan
            overall_risk = overall_response["Target"].mean() * 100 if overall_n else np.nan
            rows.append(
                {
                    "qnum": qnum,
                    "Question": question_label(qnum, QNUM_TO_ENGLISH.get(qnum, q_col)),
                    "Response": response,
                    "Selected group %": round(float(selected_pct), 2),
                    "Overall %": round(float(overall_pct), 2),
                    "Difference": round(float(selected_pct - overall_pct), 2),
                    "Selected group n": selected_n,
                    "Overall n": overall_n,
                    "Selected group At Risk %": round(float(selected_risk), 2) if pd.notna(selected_risk) else np.nan,
                    "Overall At Risk %": round(float(overall_risk), 2) if pd.notna(overall_risk) else np.nan,
                }
            )
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns).sort_values("Difference", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)


def source_question_response_comparison(patterns: pd.DataFrame, qnum: int) -> pd.DataFrame:
    if patterns.empty:
        return pd.DataFrame(columns=["Response", "Group", "Percentage", "At Risk %", "n"])
    subset = patterns[patterns["qnum"] == qnum].copy()
    if subset.empty:
        return pd.DataFrame(columns=["Response", "Group", "Percentage", "At Risk %", "n"])
    selected = subset[["Response", "Selected group %", "Selected group At Risk %", "Selected group n"]].rename(
        columns={"Selected group %": "Percentage", "Selected group At Risk %": "At Risk %", "Selected group n": "n"}
    )
    selected["Group"] = "Nhóm đang chọn"
    overall = subset[["Response", "Overall %", "Overall At Risk %", "Overall n"]].rename(
        columns={"Overall %": "Percentage", "Overall At Risk %": "At Risk %", "Overall n": "n"}
    )
    overall["Group"] = "Toàn mẫu"
    return pd.concat([selected, overall], ignore_index=True)


def render_table(df: pd.DataFrame, height: int | None = None) -> None:
    table_df = df.copy()
    if "qnum" in table_df.columns:
        table_df = add_semantic_question_column(table_df)
        table_df = table_df.drop(columns=["qnum"], errors="ignore")
    if "Construct" in table_df.columns:
        table_df["Construct"] = table_df["Construct"].map(construct_label)
    for col in ["Top pressure construct", "Most elevated construct"]:
        if col in table_df.columns:
            table_df[col] = table_df[col].map(construct_label)
    for col in ["cluster"]:
        if col in table_df.columns:
            table_df[col] = table_df[col].map(cluster_label)
    for col in ["Target", "Group", "Response", "Highest-risk response", "Lowest-risk response", "Gender", "Grade", "Age"]:
        if col in table_df.columns:
            table_df[col] = table_df[col].map(response_label)
    drop_cols = [
        col
        for col in ["question", "Question"]
        if col in table_df.columns and "Yếu tố dữ liệu" in table_df.columns
    ]
    table_df = table_df.drop(columns=drop_cols, errors="ignore")
    table_df = table_df.drop(
        columns=["Mã nguồn", "Cột kỹ thuật", "Cột kỹ thuật HMS", "Cột dữ liệu", "Q", "column"],
        errors="ignore",
    )

    table_df = table_df.rename(columns=TABLE_COLUMN_LABELS)
    table_df.columns = [str(col) for col in table_df.columns]

    header_html = "".join(f"<th>{html.escape(str(col))}</th>" for col in table_df.columns)
    rows_html = []
    for row in table_df.itertuples(index=False, name=None):
        row_cells = []
        for value in row:
            display = "" if pd.isna(value) else html.escape(str(value))
            row_cells.append(f"<td>{display}</td>")
        rows_html.append(f"<tr>{''.join(row_cells)}</tr>")

    max_height = f"max-height: {height}px;" if height else ""
    st.markdown(
        f"""
        <div class="plain-table-wrap" style="{max_height}">
            <table class="plain-table">
                <thead><tr>{header_html}</tr></thead>
                <tbody>{''.join(rows_html)}</tbody>
            </table>
        </div>
        """,
        unsafe_allow_html=True,
    )


def make_target_donut(df: pd.DataFrame):
    target_df = explain_target_counts(df)
    target_df["Nhóm nguy cơ"] = target_df["Target"].map(risk_label)
    fig = px.pie(
        target_df,
        names="Nhóm nguy cơ",
        values="Count",
        hole=0.62,
        title="Trong mẫu hiện tại, tỷ lệ At Risk là bao nhiêu?",
        color="Nhóm nguy cơ",
        color_discrete_map={
            risk_label("Lower Risk"): THEME["primary"],
            risk_label("At Risk"): THEME["coral"],
        },
    )
    fig.update_traces(textposition="inside", textinfo="percent+label", marker=dict(line=dict(color=THEME["surface"], width=2)))
    fig.update_layout(legend=dict(orientation="h", y=-0.16, x=0.5, xanchor="center"))
    return style_figure(fig, 390)


def make_simple_donut(df: pd.DataFrame, qnum: int, title: str):
    col = find_q_col(df, qnum)
    if col is None:
        return None
    freq = question_frequency_table(df, col, qnum)
    freq = localize_response_df(freq)
    fig = px.pie(
        freq,
        names="Response",
        values="Count",
        hole=0.62,
        title=title,
        color_discrete_sequence=PALETTE,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label", marker=dict(line=dict(color=THEME["surface"], width=2)))
    fig.update_layout(legend=dict(orientation="h", y=-0.16, x=0.5, xanchor="center"))
    return style_figure(fig, 390)


def make_population_distribution_bar(df: pd.DataFrame, height: int = 390):
    if POPULATION_COLUMN not in df.columns:
        return None
    chart_df = (
        df[POPULATION_COLUMN]
        .fillna("Thiếu dữ liệu")
        .astype(str)
        .value_counts(dropna=False)
        .rename_axis("Population")
        .reset_index(name="Count")
    )
    if chart_df.empty:
        return None
    chart_df["Nhóm người tham gia"] = chart_df["Population"].map(population_label)
    chart_df["Tỷ trọng (%)"] = chart_df["Count"] / chart_df["Count"].sum() * 100
    chart_df = chart_df.sort_values("Count", ascending=True)
    fig = px.bar(
        chart_df,
        x="Count",
        y="Nhóm người tham gia",
        orientation="h",
        text="Tỷ trọng (%)",
        color="Nhóm người tham gia",
        color_discrete_sequence=PALETTE,
        hover_data={"Count": True, "Tỷ trọng (%)": ":.2f"},
        title="Mẫu dữ liệu đang đại diện cho nhóm nào?",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Số bản ghi", yaxis_title="", showlegend=False)
    return style_figure(fig, height)


def make_frequency_chart(freq_df: pd.DataFrame, title: str, height: int = 390):
    freq_df = localize_response_df(freq_df)
    if len(freq_df) <= 4:
        fig = px.pie(
            freq_df,
            names="Response",
            values="Count",
            hole=0.62,
            title=title,
            color_discrete_sequence=PALETTE,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label", marker=dict(line=dict(color=THEME["surface"], width=2)))
        fig.update_layout(legend=dict(orientation="h", y=-0.16, x=0.5, xanchor="center"))
        return style_figure(fig, height)

    chart_df = freq_df.sort_values("Count", ascending=True)
    fig = px.bar(
        chart_df,
        x="Count",
        y="Response",
        orientation="h",
        text="Percentage",
        color="Percentage",
        color_continuous_scale=[[0, THEME["primary_soft"]], [0.5, THEME["secondary"]], [1, THEME["primary"]]],
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Số bản ghi", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_group_count_bar(df: pd.DataFrame, qnum: int, title: str, height: int = 390):
    col = find_q_col(df, qnum)
    if col is None:
        return None
    freq = question_frequency_table(df, col, qnum)
    freq = localize_response_df(freq)
    chart_df = freq.sort_values("Count", ascending=False)
    fig = px.bar(
        chart_df,
        x="Response",
        y="Count",
        text="Percentage",
        color="Response",
        color_discrete_sequence=PALETTE,
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="", yaxis_title="Số bản ghi", showlegend=False)
    return style_figure(fig, height)


def make_prevalence_bar(group_df: pd.DataFrame, x_col: str, title: str, height: int = 390):
    group_df = group_df.copy()
    if x_col in group_df.columns:
        group_df[x_col] = group_df[x_col].map(response_label)
    fig = px.bar(
        group_df,
        x=x_col,
        y="At Risk Rate",
        text="At Risk Rate",
        color="At Risk Rate",
        color_continuous_scale=[[0, THEME["coral_soft"]], [0.5, THEME["coral"]], [1, THEME["danger"]]],
        hover_data={"n": True, "At Risk Rate": ":.2f"},
        labels={"n": "Số bản ghi", "At Risk Rate": "Tỷ lệ nguy cơ (%)"},
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="", yaxis_title="Tỷ lệ có dấu hiệu nguy cơ (%)", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_gap_bar(gap_df: pd.DataFrame, height: int = 610):
    if gap_df.empty:
        return None
    chart_df = add_semantic_question_column(localize_response_df(gap_df)).sort_values("At Risk Gap (%)", ascending=True)
    chart_df["Nhóm chủ đề"] = chart_df["cluster"].map(cluster_label)
    fig = px.bar(
        chart_df,
        x="At Risk Gap (%)",
        y="Yếu tố dữ liệu",
        orientation="h",
        color="At Risk Gap (%)",
        color_continuous_scale=[[0, THEME["teal_soft"]], [0.5, THEME["teal"]], [1, THEME["primary"]]],
        hover_data=["Nhóm chủ đề", "Highest-risk response", "Lowest-risk response"],
        labels={
            "Highest-risk response": "Phản hồi nguy cơ cao nhất",
            "Lowest-risk response": "Phản hồi nguy cơ thấp nhất",
        },
        title="Yếu tố nào tách nhóm có nguy cơ rõ nhất?",
    )
    fig.update_layout(yaxis_title="", xaxis_title="Chênh lệch tỷ lệ nguy cơ (%)", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_target_response_bar(target_table: pd.DataFrame, title: str, height: int = 440):
    chart_df = localize_response_df(target_table)
    chart_df["Lower Risk Count"] = chart_df["Count"] - chart_df["At Risk Count"]
    long_df = chart_df.melt(
        id_vars=["Response", "At Risk Rate"],
        value_vars=["At Risk Count", "Lower Risk Count"],
        var_name="Group",
        value_name="Records",
    )
    long_df["Group"] = long_df["Group"].replace({
        "At Risk Count": risk_label("At Risk"),
        "Lower Risk Count": risk_label("Lower Risk"),
    })
    fig = px.bar(
        long_df,
        x="Response",
        y="Records",
        color="Group",
        barmode="stack",
        title=title,
        color_discrete_map={risk_label("At Risk"): THEME["coral"], risk_label("Lower Risk"): THEME["primary"]},
        hover_data={"At Risk Rate": ":.2f"},
    )
    fig.update_layout(xaxis_title="Phản hồi", yaxis_title="Số bản ghi", legend_title_text="Nhóm nguy cơ")
    return style_figure(fig, height)


def make_target_rate_bar(target_table: pd.DataFrame, title: str, height: int = 440):
    chart_df = localize_response_df(target_table).sort_values("At Risk Rate", ascending=True)
    fig = px.bar(
        chart_df,
        x="At Risk Rate",
        y="Response",
        orientation="h",
        text="At Risk Rate",
        color="At Risk Rate",
        color_continuous_scale=[[0, THEME["coral_soft"]], [0.5, THEME["coral"]], [1, THEME["danger"]]],
        hover_data={"Count": True, "At Risk Count": True},
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Tỷ lệ có dấu hiệu nguy cơ (%)", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_missing_bar(missing_df: pd.DataFrame, height: int = 540):
    chart_df = add_semantic_question_column(missing_df).sort_values("missing_pct", ascending=True)
    chart_df["Nhóm chủ đề"] = chart_df["cluster"].map(cluster_label)
    fig = px.bar(
        chart_df,
        x="missing_pct",
        y="Yếu tố dữ liệu",
        orientation="h",
        text="missing_pct",
        color="missing_pct",
        color_continuous_scale=[[0, THEME["primary_soft"]], [0.5, THEME["gold"]], [1, THEME["coral"]]],
        hover_data=["Nhóm chủ đề", "missing"],
        labels={"missing": "Số ô thiếu", "missing_pct": "Thiếu dữ liệu (%)"},
        title="Yếu tố nào thiếu dữ liệu nhiều nhất?",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Missing (%)", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_cluster_question_bar(cluster_df: pd.DataFrame, height: int = 470):
    chart_df = cluster_df.copy()
    chart_df["Nhóm chủ đề"] = chart_df["cluster"].map(cluster_label)
    chart_df = chart_df.sort_values("Questions", ascending=True)
    fig = px.bar(
        chart_df,
        x="Questions",
        y="Nhóm chủ đề",
        orientation="h",
        text="Questions",
        color="Questions",
        color_continuous_scale=[[0, THEME["primary_soft"]], [0.5, THEME["secondary"]], [1, THEME["primary"]]],
        title="Độ rộng dữ liệu theo từng nhóm chủ đề",
    )
    fig.update_traces(textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Số câu hỏi", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_cluster_coverage_bar(cluster_df: pd.DataFrame, height: int = 470):
    chart_df = cluster_df.copy()
    chart_df["Nhóm chủ đề"] = chart_df["cluster"].map(cluster_label)
    chart_df = chart_df.sort_values("Average Coverage (%)", ascending=True)
    fig = px.bar(
        chart_df,
        x="Average Coverage (%)",
        y="Nhóm chủ đề",
        orientation="h",
        text="Average Coverage (%)",
        color="Average Coverage (%)",
        color_continuous_scale=[[0, THEME["coral_soft"]], [0.5, THEME["teal"]], [1, THEME["success"]]],
        title="Nhóm chủ đề nào có dữ liệu đầy đủ hơn?",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Coverage (%)", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_cluster_gap_bar(gap_df: pd.DataFrame, height: int = 470):
    chart_df = gap_df.copy()
    chart_df["Nhóm chủ đề"] = chart_df["cluster"].map(cluster_label)
    chart_df = chart_df.sort_values("Average Gap (%)", ascending=True)
    fig = px.bar(
        chart_df,
        x="Average Gap (%)",
        y="Nhóm chủ đề",
        orientation="h",
        text="Average Gap (%)",
        color="Max Gap (%)",
        color_continuous_scale=[[0, THEME["coral_soft"]], [0.5, THEME["coral"]], [1, THEME["danger"]]],
        hover_data=["Questions Analysed", "Max Gap (%)"],
        title="Nhóm chủ đề nào tạo khác biệt nguy cơ lớn hơn?",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Average gap (%)", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_cluster_treemap(catalog: pd.DataFrame, height: int = 620):
    chart_df = catalog.copy()
    chart_df["size"] = 1
    chart_df["Nhóm chủ đề"] = chart_df["cluster"].map(cluster_label)
    chart_df = add_semantic_question_column(chart_df)
    fig = px.treemap(
        chart_df,
        path=["Nhóm chủ đề", "Yếu tố dữ liệu"],
        values="size",
        color="missing_pct",
        color_continuous_scale=[[0, THEME["teal_soft"]], [0.5, THEME["gold"]], [1, THEME["coral"]]],
        hover_data=["qnum", "missing_pct", "unique_valid_values"],
        title="Bản đồ các yếu tố thành phần theo nhóm chủ đề",
    )
    return style_figure(fig, height)


def make_derived_boxplot(df: pd.DataFrame, selected_scores: List[str], height: int = 540):
    long_df = df[selected_scores + ["Target"]].copy()
    long_df["Nhóm nguy cơ"] = long_df["Target"].map(risk_label).fillna(long_df["Target"].astype(str))
    long_df = long_df.melt(
        id_vars=["Target", "Nhóm nguy cơ"],
        value_vars=selected_scores,
        var_name="Construct",
        value_name="Value",
    )
    long_df["Khía cạnh"] = long_df["Construct"].map(construct_label)
    fig = px.box(
        long_df,
        x="Khía cạnh",
        y="Value",
        color="Nhóm nguy cơ",
        points=False,
        color_discrete_map={risk_label("Lower Risk"): THEME["primary"], risk_label("At Risk"): THEME["coral"]},
        title="Nhóm có nguy cơ đang cao hơn ở khía cạnh nào?",
    )
    fig.update_layout(xaxis_title="", yaxis_title="Điểm khía cạnh (0-100)", legend_title_text="")
    return style_figure(fig, height)


def make_score_bin_rate(df: pd.DataFrame, score_col: str, height: int = 430):
    bin_df = localize_response_df(target_prevalence_by_score_bins(df, score_col, bins=5))
    fig = px.bar(
        bin_df,
        x="Score Bin",
        y="At Risk Rate",
        text="At Risk Rate",
        color="At Risk Rate",
        color_continuous_scale=[[0, THEME["coral_soft"]], [0.5, THEME["coral"]], [1, THEME["danger"]]],
        hover_data={"n": True},
        labels={"n": "Số bản ghi", "At Risk Rate": "Tỷ lệ nguy cơ (%)"},
        title=f"{construct_label(score_col)}: mức cao hơn đi cùng nguy cơ ra sao?",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Mức điểm của khía cạnh", yaxis_title="Tỷ lệ có dấu hiệu nguy cơ (%)", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_score_corr(df: pd.DataFrame, score_cols: List[str], height: int = 520):
    corr = df[score_cols].corr()
    corr = corr.rename(index=CONSTRUCT_LABELS, columns=CONSTRUCT_LABELS)
    fig = px.imshow(
        corr,
        text_auto=".2f",
        color_continuous_scale=[[0, THEME["danger"]], [0.5, THEME["surface_alt"]], [1, THEME["primary"]]],
        zmin=-1,
        zmax=1,
        title="Các khía cạnh bối cảnh thường đi cùng nhau như thế nào?",
    )
    return style_figure(fig, height)


def make_construct_profile_chart(profile_df: pd.DataFrame, height: int = 460):
    if profile_df.empty:
        return None
    profile_df = add_semantic_construct_column(profile_df)
    long_df = profile_df.melt(
        id_vars=["Construct", "Khía cạnh", "Difference"],
        value_vars=["Lower Risk mean", "At Risk mean"],
        var_name="Group",
        value_name="Mean score",
    )
    long_df["Group"] = long_df["Group"].replace(
        {"Lower Risk mean": risk_label("Lower Risk"), "At Risk mean": risk_label("At Risk")}
    )
    fig = px.bar(
        long_df,
        x="Mean score",
        y="Khía cạnh",
        color="Group",
        barmode="group",
        orientation="h",
        color_discrete_map={risk_label("Lower Risk"): THEME["primary"], risk_label("At Risk"): THEME["coral"]},
        title="Nhóm có nguy cơ khác nhóm còn lại ở khía cạnh nào?",
        hover_data={"Difference": ":.2f"},
    )
    fig.update_layout(xaxis_title="Điểm trung bình (0-100)", yaxis_title="", legend_title_text="")
    return style_figure(fig, height)


def make_construct_delta_chart(impact_df: pd.DataFrame, height: int = 430):
    if impact_df.empty:
        return None
    chart_df = add_semantic_construct_column(impact_df).sort_values("Thay đổi khi construct tăng (%)", ascending=True)
    values = pd.to_numeric(chart_df["Thay đổi khi construct tăng (%)"], errors="coerce")
    min_value = float(values.min()) if values.notna().any() else 0.0
    max_value = float(values.max()) if values.notna().any() else 0.0
    span = max(12.0, max_value - min_value)
    x_min = min(0.0, min_value) - span * 0.18
    x_max = max(0.0, max_value) + span * 0.18
    fig = px.bar(
        chart_df,
        x="Thay đổi khi construct tăng (%)",
        y="Khía cạnh",
        orientation="h",
        text="Thay đổi khi construct tăng (%)",
        color="Thay đổi khi construct tăng (%)",
        color_continuous_scale=[[0, THEME["teal_soft"]], [0.5, THEME["gold"]], [1, THEME["danger"]]],
        title="Khi một khía cạnh tăng, tỷ lệ nguy cơ thay đổi bao nhiêu?",
    )
    fig.update_traces(texttemplate="%{text:+.1f}%", textposition="outside", cliponaxis=False)
    fig = style_figure(fig, height)
    fig.update_layout(
        yaxis_title="",
        xaxis_title="Chênh lệch tỷ lệ nguy cơ (+ cao hơn, - thấp hơn)",
        coloraxis_showscale=False,
        margin=dict(l=210, r=92, t=72, b=58),
        uniformtext_minsize=11,
        uniformtext_mode="show",
    )
    fig.update_xaxes(range=[x_min, x_max], zeroline=True, zerolinewidth=1, zerolinecolor=THEME["grid"])
    fig.update_yaxes(automargin=True)
    return fig


def make_construct_trend_chart(trend_df: pd.DataFrame, score_col: str, height: int = 430):
    if trend_df.empty:
        return None
    trend_df = localize_response_df(trend_df)
    fig = px.line(
        trend_df,
        x="Mức construct",
        y="At Risk Rate",
        markers=True,
        text="At Risk Rate",
        title=f"{construct_label(score_col)}: đường xu hướng nguy cơ",
        hover_data={"n": True, "Mean score": ":.2f"},
        labels={"n": "Số bản ghi", "Mean score": "Điểm trung bình", "At Risk Rate": "Tỷ lệ nguy cơ (%)"},
    )
    fig.update_traces(line=dict(color=THEME["coral"], width=4), marker=dict(size=10), textposition="top center")
    fig.update_layout(xaxis_title="Mức điểm của khía cạnh", yaxis_title="Tỷ lệ có dấu hiệu nguy cơ (%)")
    return style_figure(fig, height)


def make_demographic_construct_heatmap(table: pd.DataFrame, group_name: str, value_col: str, title: str, height: int = 500):
    if table.empty:
        return None
    chart_df = add_semantic_construct_column(table)
    if group_name in chart_df.columns:
        chart_df[group_name] = chart_df[group_name].map(response_label)
    pivot = chart_df.pivot(index=group_name, columns="Khía cạnh", values=value_col)
    fig = px.imshow(
        pivot,
        text_auto=".1f",
        color_continuous_scale=[[0, THEME["primary_soft"]], [0.5, THEME["gold"]], [1, THEME["danger"]]],
        title=title,
        aspect="auto",
    )
    fig.update_layout(xaxis_title="", yaxis_title="")
    return style_figure(fig, height)


def make_demographic_mental_summary_chart(summary: pd.DataFrame, title: str, height: int = 430):
    if summary.empty:
        return None
    chart_df = summary.copy()
    for col in ["Most elevated construct", "Top pressure construct"]:
        if col in chart_df.columns:
            chart_df[col] = chart_df[col].map(construct_label)
    if "Group" in chart_df.columns:
        chart_df["Group"] = chart_df["Group"].map(response_label)
    chart_df = chart_df.sort_values("At Risk Rate", ascending=True)
    fig = px.bar(
        chart_df,
        x="At Risk Rate",
        y="Group",
        color="Dimension",
        orientation="h",
        text="At Risk Rate",
        hover_data=["n", "Most elevated construct", "Elevation vs overall"],
        labels={
            "n": "Số bản ghi",
            "Most elevated construct": "Khía cạnh nổi bật hơn toàn mẫu",
            "Elevation vs overall": "Chênh lệch so với toàn mẫu",
        },
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Tỷ lệ có dấu hiệu nguy cơ (%)")
    fig = style_figure(fig, height)
    fig.update_layout(margin=dict(l=170, r=78, t=72, b=58), legend_title_text="")
    fig.update_yaxes(automargin=True)
    return fig


def make_group_construct_profile_chart(profile_df: pd.DataFrame, selected_group: str, height: int = 430):
    if profile_df.empty:
        return None
    chart_df = add_semantic_construct_column(profile_df).sort_values("Difference", ascending=True)
    selected_group_label = response_label(selected_group)
    fig = px.bar(
        chart_df,
        x="Difference",
        y="Khía cạnh",
        orientation="h",
        text="Difference",
        color="Difference",
        color_continuous_scale=[[0, THEME["teal_soft"]], [0.5, THEME["gold"]], [1, THEME["danger"]]],
        title=f"Nhóm {selected_group_label} nổi bật hơn toàn mẫu ở đâu?",
    )
    fig.update_traces(texttemplate="%{text:.1f}", textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Chênh lệch điểm so với toàn mẫu (+ cao hơn, - thấp hơn)", coloraxis_showscale=False)
    return style_figure(fig, height)


def make_response_pattern_skew_chart(patterns: pd.DataFrame, selected_group: str, height: int = 560):
    if patterns.empty:
        return None
    chart_df = patterns.head(18).copy()
    chart_df = localize_response_df(chart_df)
    chart_df["Question response"] = chart_df.apply(
        lambda row: f"{question_label(row['qnum'], row.get('Question', ''))} • {row['Response']}", axis=1
    )
    chart_df = chart_df.sort_values("Difference", ascending=True)
    selected_group_label = response_label(selected_group)
    fig = px.bar(
        chart_df,
        x="Difference",
        y="Question response",
        orientation="h",
        text="Difference",
        color="Difference",
        color_continuous_scale=[[0, THEME["teal"]], [0.5, THEME["surface_alt"]], [1, THEME["danger"]]],
        hover_data=["Question", "Selected group %", "Overall %", "Selected group At Risk %", "Selected group n"],
        labels={
            "Question": "Yếu tố dữ liệu",
            "Selected group %": "Tỷ lệ trong nhóm (%)",
            "Overall %": "Tỷ lệ toàn mẫu (%)",
            "Selected group At Risk %": "Nguy cơ trong nhóm (%)",
            "Selected group n": "Số bản ghi nhóm đang chọn",
        },
        title=f"Phản hồi nào xuất hiện khác biệt ở nhóm {selected_group_label}?",
    )
    fig.update_traces(texttemplate="%{text:+.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(
        yaxis_title="",
        xaxis_title="Chênh lệch tỷ lệ chọn đáp án so với toàn mẫu (+ cao hơn, - thấp hơn)",
        coloraxis_showscale=False,
    )
    return style_figure(fig, height)


def make_source_response_comparison_chart(response_df: pd.DataFrame, qnum: int, selected_group: str, height: int = 430):
    if response_df.empty:
        return None
    response_df = localize_response_df(response_df)
    selected_group_label = response_label(selected_group)
    fig = px.bar(
        response_df,
        x="Response",
        y="Percentage",
        color="Group",
        barmode="group",
        text="Percentage",
        hover_data=["At Risk %", "n"],
        labels={"At Risk %": "Tỷ lệ nguy cơ (%)", "n": "Số bản ghi"},
        color_discrete_map={"Nhóm đang chọn": THEME["coral"], "Toàn mẫu": THEME["primary"]},
        title=f"{question_label(qnum)}: nhóm {selected_group_label} khác toàn mẫu như thế nào?",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="", yaxis_title="Tỷ lệ chọn đáp án (%)", legend_title_text="")
    return style_figure(fig, height)


def make_source_response_risk_chart(response_df: pd.DataFrame, qnum: int, selected_group: str, height: int = 430):
    if response_df.empty:
        return None
    response_df = localize_response_df(response_df)
    selected = response_df[response_df["Group"] == "Nhóm đang chọn"].copy()
    if selected.empty:
        return None
    selected = selected.sort_values("At Risk %", ascending=True)
    fig = px.bar(
        selected,
        x="At Risk %",
        y="Response",
        orientation="h",
        text="At Risk %",
        color="At Risk %",
        color_continuous_scale=[[0, THEME["coral_soft"]], [0.5, THEME["gold"]], [1, THEME["danger"]]],
        hover_data=["Percentage", "n"],
        labels={"Percentage": "Tỷ lệ chọn đáp án (%)", "n": "Số bản ghi", "At Risk %": "Tỷ lệ nguy cơ (%)"},
        title=f"{question_label(qnum)}: phản hồi nào đi cùng nguy cơ cao hơn?",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(yaxis_title="", xaxis_title="Tỷ lệ có dấu hiệu nguy cơ trong nhóm (%)", coloraxis_showscale=False)
    return style_figure(fig, height)


def render_no_data_state() -> None:
    hero()
    st.info("Chưa đọc được Gold survey trên Cloud Storage. Hãy chạy Spark Bronze -> Silver -> Gold và kiểm tra các Gold table.")
    st.markdown(
        """
        ### Dashboard sẽ trả lời
        - Quy mô nguy cơ sức khỏe tinh thần trong nhóm học sinh/sinh viên đang xem
        - Nhóm nhân khẩu học nào có tỷ lệ nguy cơ cao hơn
        - Trải nghiệm, hành vi và bối cảnh nào liên quan mạnh tới nhóm nguy cơ
        - Khía cạnh bối cảnh nào nổi bật để phục vụ phân tích và báo cáo
        - Dữ liệu có thiếu, lệch hoặc cần thận trọng ở đâu
        """
    )


def render_header(raw_df: pd.DataFrame, filtered_raw: pd.DataFrame, processed) -> None:
    hero()
    target_counts = explain_target_counts(filtered_raw)
    at_risk_row = target_counts[target_counts["Target"] == "At Risk"]
    at_risk_pct = float(at_risk_row["Percentage"].iloc[0]) if not at_risk_row.empty else 0.0
    missing_pct = float(processed.cleaned.isna().sum().sum()) / max(1, processed.cleaned.size) * 100
    population_count = (
        filtered_raw[POPULATION_COLUMN].dropna().astype(str).nunique()
        if POPULATION_COLUMN in filtered_raw.columns
        else 1
    )
    construct_count = sum(
        1
        for feature in RESEARCH_FEATURES
        if feature in processed.cleaned.columns and processed.cleaned[feature].notna().any()
    )

    cols = st.columns(5, gap="medium")
    with cols[0]:
        kpi_card("Cỡ mẫu đang đọc", f"{filtered_raw.shape[0]:,}", f"Tổng dữ liệu: {raw_df.shape[0]:,}")
    with cols[1]:
        kpi_card("Nhóm nguy cơ", f"{at_risk_pct:.2f}%", "Tỷ lệ người tham gia có dấu hiệu nguy cơ")
    with cols[2]:
        kpi_card("Khía cạnh phân tích", f"{construct_count:,}", "Nhóm chỉ số tâm lý - giáo dục")
    with cols[3]:
        kpi_card("Nhóm người tham gia", f"{population_count:,}", "Học sinh và sinh viên trong phạm vi phân tích")
    with cols[4]:
        kpi_card("Thiếu dữ liệu", f"{missing_pct:.2f}%", "Tỷ lệ ô trống sau xử lý")


def render_overview_tab(filtered_raw: pd.DataFrame, processed) -> None:
    section_head(
        "Tín hiệu chính trong dữ liệu",
        "Bắt đầu từ quy mô nguy cơ, cấu trúc mẫu và nhóm người tham gia cần được chú ý trước.",
    )

    if filtered_raw.empty:
        st.warning("Không còn bản ghi nào sau khi áp dụng bộ lọc.")
        return

    top = st.columns(3, gap="large")
    with top[0]:
        st.plotly_chart(make_target_donut(filtered_raw), width="stretch", config=PLOT_CONFIG, key="overview_target_donut")
    with top[1]:
        population_fig = make_population_distribution_bar(filtered_raw)
        if population_fig is not None:
            st.plotly_chart(population_fig, width="stretch", config=PLOT_CONFIG, key="overview_population_distribution")
    with top[2]:
        gender_fig = make_simple_donut(filtered_raw, 2, "Mẫu phân bố theo giới tính như thế nào?")
        if gender_fig is not None:
            st.plotly_chart(gender_fig, width="stretch", config=PLOT_CONFIG, key="overview_gender_donut")

    second = st.columns(2, gap="large")
    with second[0]:
        grade_fig = make_group_count_bar(filtered_raw, 3, "Mẫu phân bố theo bậc/lớp ra sao?")
        if grade_fig is not None:
            st.plotly_chart(grade_fig, width="stretch", config=PLOT_CONFIG, key="overview_grade_count")
    with second[1]:
        age_fig = make_group_count_bar(filtered_raw, 1, "Mẫu phân bố theo độ tuổi ra sao?")
        if age_fig is not None:
            st.plotly_chart(age_fig, width="stretch", config=PLOT_CONFIG, key="overview_age_count")

    section_head(
        "Nhóm nào cần được chú ý hơn?",
        "So sánh tỷ lệ nguy cơ theo tuổi, giới tính và bậc/lớp để tìm nhóm có rủi ro tương đối cao hơn.",
    )

    pcols = st.columns(3, gap="large")
    age_df = target_prevalence_by_group(filtered_raw, 1, "Age")
    gender_df = target_prevalence_by_group(filtered_raw, 2, "Gender")
    grade_df = target_prevalence_by_group(filtered_raw, 3, "Grade")

    with pcols[0]:
        if not age_df.empty:
            st.plotly_chart(make_prevalence_bar(age_df, "Age", "At Risk cao hơn ở nhóm tuổi nào?"), width="stretch", config=PLOT_CONFIG, key="overview_age_prevalence")
    with pcols[1]:
        if not gender_df.empty:
            st.plotly_chart(make_prevalence_bar(gender_df, "Gender", "At Risk cao hơn theo giới tính nào?"), width="stretch", config=PLOT_CONFIG, key="overview_gender_prevalence")
    with pcols[2]:
        if not grade_df.empty:
            st.plotly_chart(make_prevalence_bar(grade_df, "Grade", "At Risk cao hơn theo bậc/lớp nào?"), width="stretch", config=PLOT_CONFIG, key="overview_grade_prevalence")

    score_cols = [feature for feature in RESEARCH_FEATURES if feature in processed.cleaned.columns]
    filtered_cleaned = processed.cleaned.loc[filtered_raw.index].copy()
    summary = all_demographic_mental_summary(filtered_raw, filtered_cleaned, score_cols)
    if not summary.empty:
        section_head(
            "Nếu xếp hạng chung, nhóm nào nổi bật nhất?",
            "Gộp các lát cắt tuổi, giới tính và bậc/lớp vào cùng một ranking để người xem nhìn ra nhóm ưu tiên nhanh hơn.",
        )
        fig = make_demographic_mental_summary_chart(summary.head(12), "Top nhóm có tỷ lệ At Risk cao nhất", height=520)
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key="overview_top_group_risk")

    section_head(
        "Phản hồi nào tạo khác biệt nguy cơ lớn nhất?",
        "Xếp hạng các yếu tố thành phần theo chênh lệch nguy cơ giữa nhóm phản hồi cao nhất và thấp nhất.",
    )

    gap_df = top_target_gap_questions(processed, top_n=12, min_category_n=40, df=filtered_raw)
    gap_fig = make_gap_bar(gap_df, height=660)
    if gap_fig is not None:
        st.plotly_chart(gap_fig, width="stretch", config=PLOT_CONFIG, key="overview_gap_bar")
    if not gap_df.empty:
        with st.expander("Chi tiết dữ liệu của ranking phản hồi", expanded=False):
            view = gap_df[
                [
                    "qnum",
                    "question",
                    "cluster",
                    "At Risk Gap (%)",
                    "Highest-risk response",
                    "Highest-risk rate (%)",
                ]
            ].copy()
            render_table(view, height=360)


def render_raw_data_tab(filtered_raw: pd.DataFrame, processed) -> None:
    section_head(
        "2. Yếu tố liên quan trực tiếp",
        "Phần này trả lời: khi một trải nghiệm hoặc hành vi thay đổi, tỷ lệ At Risk thay đổi như thế nào.",
    )

    if filtered_raw.empty:
        st.warning("Không còn bản ghi nào sau khi áp dụng bộ lọc.")
        return

    catalog = raw_question_catalog(processed)
    gap_df = top_target_gap_questions(processed, top_n=15, min_category_n=40, df=filtered_raw)
    if not gap_df.empty:
        section_head(
            "Ưu tiên yếu tố theo mức chênh lệch nguy cơ",
            "Bảng này giúp nhà nghiên cứu không bị lạc trong bộ dữ liệu: bắt đầu từ các biến có khác biệt At Risk lớn nhất.",
        )
        st.plotly_chart(make_gap_bar(gap_df, height=560), width="stretch", config=PLOT_CONFIG, key="factor_gap_bar")
        view = gap_df[
            [
                "qnum",
                "question",
                "cluster",
                "At Risk Gap (%)",
                "Highest-risk response",
                "Highest-risk rate (%)",
                "Lowest-risk response",
                "Lowest-risk rate (%)",
            ]
        ].copy()
        render_table(view, height=420)

    section_head(
        "Kiểm tra nhanh các biến trọng tâm",
        "Các câu hỏi mặc định tập trung vào sức khỏe tinh thần, bạo lực/bắt nạt, chất gây nghiện, gia đình, kết nối trường học và khó khăn chức năng.",
    )
    available_qnums = [q for q in KEY_RAW_QNUMS if find_q_col(filtered_raw, q)]
    selected_qnums = st.multiselect(
        "Chọn yếu tố để xem phân bố phản hồi",
        available_qnums,
        default=available_qnums[:6],
        format_func=lambda q: question_label(q, catalog.loc[catalog["qnum"] == q, "question"].iloc[0] if not catalog[catalog["qnum"] == q].empty else q),
    )

    if selected_qnums:
        cols = st.columns(3, gap="large")
        for idx, qnum in enumerate(selected_qnums):
            col = find_q_col(filtered_raw, qnum)
            if col is None:
                continue
            freq = question_frequency_table(filtered_raw, col, qnum)
            title = question_label(qnum, processed.raw_col_to_display.get(col, col))
            with cols[idx % 3]:
                st.plotly_chart(make_frequency_chart(freq, title, 390), width="stretch", config=PLOT_CONFIG, key=f"raw_quick_frequency_q{qnum}")

    section_head(
        "Một yếu tố ảnh hưởng như thế nào?",
        "Chọn một câu hỏi để xem ba lớp bằng chứng: phân bố phản hồi, số lượng At Risk và tỷ lệ At Risk trong từng nhóm phản hồi.",
    )

    clusters = sorted(catalog["cluster"].dropna().unique().tolist())
    selected_cluster = st.selectbox("Chọn cụm", clusters, key="raw_cluster_selector")
    sub_catalog = catalog[catalog["cluster"] == selected_cluster].copy()
    options = sub_catalog["column"].tolist()
    selected_col = st.selectbox(
        "Chọn yếu tố dữ liệu",
        options,
        format_func=lambda col: question_label(
            sub_catalog.loc[sub_catalog["column"] == col, "qnum"].iloc[0],
            sub_catalog.loc[sub_catalog["column"] == col, "question"].iloc[0],
        ),
        key="raw_question_selector",
    )

    selected_qnum = int(sub_catalog.loc[sub_catalog["column"] == selected_col, "qnum"].iloc[0])
    freq_df = question_frequency_table(filtered_raw, selected_col, selected_qnum)
    target_df = target_by_response_table(filtered_raw, selected_col, selected_qnum)
    if not target_df.empty:
        high = target_df.iloc[0]
        low = target_df.sort_values("At Risk Rate", ascending=True).iloc[0]
        insight_box(
            f"{question_label(selected_qnum, processed.raw_col_to_display.get(selected_col, selected_col))} liên quan tới nguy cơ ra sao?",
            f"Phản hồi '{response_label(high['Response'])}' có tỷ lệ nguy cơ {high['At Risk Rate']:.2f}%, trong khi phản hồi '{response_label(low['Response'])}' là {low['At Risk Rate']:.2f}%.",
            "Cần xem đồng thời cỡ mẫu từng nhóm để tránh diễn giải quá mức các nhóm ít quan sát.",
        )

    row1 = st.columns(2, gap="large")
    selected_question_name = question_label(selected_qnum, processed.raw_col_to_display.get(selected_col, selected_col))
    with row1[0]:
        st.plotly_chart(make_frequency_chart(freq_df, f"Phản hồi phổ biến: {selected_question_name}", 440), width="stretch", config=PLOT_CONFIG, key=f"raw_detail_frequency_q{selected_qnum}")
    with row1[1]:
        st.plotly_chart(make_target_response_bar(target_df, f"Cơ cấu nhóm nguy cơ theo phản hồi: {selected_question_name}", 440), width="stretch", config=PLOT_CONFIG, key=f"raw_detail_target_stack_q{selected_qnum}")

    row2 = st.columns([1.15, 1.0], gap="large")
    with row2[0]:
        st.plotly_chart(make_target_rate_bar(target_df, f"Nguy cơ theo phản hồi: {selected_question_name}", 440), width="stretch", config=PLOT_CONFIG, key=f"raw_detail_target_rate_q{selected_qnum}")
    with row2[1]:
        render_table(target_df, height=440)

    section_head(
        "Biến nào cần thận trọng vì thiếu dữ liệu?",
        "Nếu một yếu tố có tỷ lệ thiếu cao, kết luận về yếu tố đó nên được xem như gợi ý hơn là bằng chứng mạnh.",
    )

    missing_df = top_missing_questions(processed, top_n=15)
    row3 = st.columns([1.3, 1.0], gap="large")
    with row3[0]:
        st.plotly_chart(make_missing_bar(missing_df), width="stretch", config=PLOT_CONFIG, key="raw_missing_bar")
    with row3[1]:
        view = missing_df[["qnum", "question", "cluster", "missing", "missing_pct"]].copy()
        render_table(view, height=540)


def render_cluster_tab(filtered_raw: pd.DataFrame, processed) -> None:
    section_head(
        "3. Cơ chế theo chủ đề",
        "Phần này gom các câu hỏi rời rạc thành miền nghiên cứu: gia đình, trường học, chất gây nghiện, lối sống, bạo lực và bối cảnh an toàn.",
    )

    catalog = raw_question_catalog(processed)
    cluster_df = cluster_overview_table(processed)
    gap_df = cluster_gap_summary(processed)

    if not gap_df.empty:
        top_cluster = gap_df.iloc[0]
        insight_box(
            "Miền chủ đề nào tạo khác biệt At Risk lớn nhất?",
            f"Cụm '{top_cluster['cluster']}' có chênh lệch At Risk trung bình cao nhất trong các câu hỏi đủ dữ liệu, với Average Gap khoảng {top_cluster['Average Gap (%)']:.2f}%.",
            "Cụm có nhiều câu hỏi hơn có thể có nhiều cơ hội xuất hiện gap lớn; nên đọc kèm số câu hỏi và coverage.",
        )

    row1 = st.columns(2, gap="large")
    with row1[0]:
        st.plotly_chart(make_cluster_question_bar(cluster_df), width="stretch", config=PLOT_CONFIG, key="cluster_question_count")
    with row1[1]:
        st.plotly_chart(make_cluster_coverage_bar(cluster_df), width="stretch", config=PLOT_CONFIG, key="cluster_coverage")

    row2 = st.columns([1.15, 1.0], gap="large")
    with row2[0]:
        if not gap_df.empty:
            st.plotly_chart(make_cluster_gap_bar(gap_df), width="stretch", config=PLOT_CONFIG, key="cluster_gap")
    with row2[1]:
        render_table(cluster_df, height=300)
        if not gap_df.empty:
            render_table(gap_df, height=170)

    section_head(
        "Đọc sâu một miền chủ đề",
        "Chọn cụm để xem các câu hỏi đại diện. Mục tiêu là hiểu nội dung của miền, không chỉ xem số cột trong dataset.",
    )

    clusters = sorted(catalog["cluster"].dropna().unique().tolist())
    selected_cluster = st.selectbox("Chọn cụm để xem gallery", clusters, key="cluster_gallery_selector")
    selected_catalog = catalog[catalog["cluster"] == selected_cluster].copy()
    max_questions = min(12, len(selected_catalog))
    show_count = st.slider("Số câu hỏi hiển thị", 1, max(1, max_questions), min(6, max(1, max_questions)), 1)

    cols = st.columns(3, gap="large")
    for idx, row in enumerate(selected_catalog.head(show_count).itertuples()):
        col = row.column
        qnum = int(row.qnum)
        if col not in filtered_raw.columns:
            continue
        freq = question_frequency_table(filtered_raw, col, qnum)
        with cols[idx % 3]:
            st.plotly_chart(make_frequency_chart(freq, question_label(qnum, row.question), 390), width="stretch", config=PLOT_CONFIG, key=f"cluster_gallery_{selected_cluster}_q{qnum}_{idx}")

    section_head(
        "Miền nào cần ưu tiên đọc sâu?",
        "Phần này dùng chênh lệch At Risk và coverage dữ liệu để chọn miền phân tích ưu tiên.",
    )

    if not gap_df.empty:
        priority = gap_df.iloc[0]
        insight_box(
            "Miền ưu tiên trong dữ liệu hiện tại là gì?",
            f"Cụm '{priority['cluster']}' có Average Gap cao nhất ({priority['Average Gap (%)']:.2f}%). Đây là miền nên đọc sâu trước khi viết nhận định.",
            "Đây là thống kê mô tả theo dữ liệu đang lọc; cần đối chiếu missing và cỡ mẫu trước khi kết luận.",
        )


def render_score_tab(filtered_cleaned: pd.DataFrame, processed) -> None:
    section_head(
        "4. Hồ sơ nguy cơ tổng hợp",
        "Phần này đọc các bối cảnh gia đình, học tập, an toàn xã hội, sang chấn, dùng chất và thiếu hụt phục hồi như các construct nghiên cứu.",
    )

    score_cols = [feature for feature in RESEARCH_FEATURES if feature in filtered_cleaned.columns]

    summary = derived_score_summary(filtered_cleaned, score_cols)
    if not summary.empty:
        view = summary.copy()
        for col in ["Mean", "Median", "Std", "Min", "Max"]:
            view[col] = view[col].round(3)
        render_table(view)

    if not score_cols:
        st.info("Không có construct nghiên cứu để hiển thị.")
        return

    insight_box(
        "Vì sao cần khía cạnh tổng hợp thay vì đọc từng cột riêng lẻ?",
        "Nhà nghiên cứu cần hiểu dữ liệu đại diện cho khái niệm nào. Ví dụ gia đình & an toàn tại nhà đại diện cho bất lợi gia đình; học tập & kết nối trường đại diện cho khó khăn học thuật.",
        "Các chỉ số này phục vụ khám phá mô tả; cần mô tả công thức rõ khi báo cáo.",
    )

    impact = construct_impact_table(filtered_cleaned, score_cols)
    if not impact.empty:
        section_head(
            "Khi construct tăng, tỷ lệ At Risk thay đổi thế nào?",
        "Bảng này đọc theo hướng nghiên cứu: từ nhóm có mức construct thấp sang nhóm có mức construct cao, tỷ lệ At Risk tăng hay giảm bao nhiêu phần trăm.",
        )
        render_table(impact, height=360)

    selected_scores = st.multiselect(
        "Chọn khía cạnh để so sánh theo nhóm nguy cơ",
        score_cols,
        default=[score for score in ["Family Pressure Index", "Academic Pressure Index", "Peer & Safety Stress Index"] if score in score_cols],
        format_func=construct_label,
    )

    if selected_scores:
        st.plotly_chart(make_derived_boxplot(filtered_cleaned, selected_scores), width="stretch", config=PLOT_CONFIG, key="score_derived_boxplot")

    section_head(
        "Đọc sâu một khía cạnh",
        "Chia điểm khía cạnh thành các khoảng để quan sát xu hướng nguy cơ theo mức bất lợi/rủi ro.",
    )

    score = st.selectbox("Chọn khía cạnh", score_cols, format_func=construct_label, key="score_bin_selector")
    row = st.columns([1.18, 1.0], gap="large")
    with row[0]:
        st.plotly_chart(make_score_bin_rate(filtered_cleaned, score), width="stretch", config=PLOT_CONFIG, key=f"score_bin_rate_{score}")
    with row[1]:
        table = target_prevalence_by_score_bins(filtered_cleaned, score, bins=5)
        render_table(table, height=430)

    if len(score_cols) >= 2:
        st.plotly_chart(make_score_corr(filtered_cleaned, score_cols), width="stretch", config=PLOT_CONFIG, key="score_corr_heatmap")


def render_report_tab(raw_df: pd.DataFrame, filtered_raw: pd.DataFrame, filtered_cleaned: pd.DataFrame, processed) -> None:
    section_head(
        "Tóm tắt để đưa vào báo cáo",
        "Các phát hiện mô tả quan trọng nhất trong phạm vi bộ lọc hiện tại, viết theo ngôn ngữ dữ liệu thay vì ngôn ngữ mã cột.",
    )

    if filtered_raw.empty:
        st.warning("Không còn bản ghi nào sau khi áp dụng bộ lọc.")
        return

    score_cols = [feature for feature in RESEARCH_FEATURES if feature in filtered_cleaned.columns]
    target_counts = explain_target_counts(filtered_raw)
    at_risk_row = target_counts[target_counts["Target"] == "At Risk"]
    at_risk_pct = float(at_risk_row["Percentage"].iloc[0]) if not at_risk_row.empty else 0.0

    report_rows = [
        {
            "Mục báo cáo": "Cỡ mẫu đang phân tích",
            "Kết quả": f"{filtered_raw.shape[0]:,} bản ghi",
            "Diễn giải": "Quy mô dữ liệu sau khi áp dụng bộ lọc sidebar.",
        },
        {
            "Mục báo cáo": "Tỷ lệ nhóm có dấu hiệu nguy cơ",
            "Kết quả": f"{at_risk_pct:.2f}%",
            "Diễn giải": "Tỷ lệ người tham gia có dấu hiệu nguy cơ theo nhãn phân tích đã chuẩn hóa giữa nhóm học sinh và nhóm sinh viên.",
        },
    ]

    profile = construct_profile_table(filtered_cleaned, score_cols)
    if not profile.empty:
        top_profile = profile.iloc[0]
        report_rows.append(
            {
                "Mục báo cáo": "Khía cạnh khác biệt nhất giữa hai nhóm nguy cơ",
                "Kết quả": f"{construct_label(top_profile['Construct'])} (+{top_profile['Difference']:.2f})",
                "Diễn giải": "Khía cạnh có điểm trung bình cao hơn nhiều nhất ở nhóm có dấu hiệu nguy cơ.",
            }
        )

    impact = construct_impact_table(filtered_cleaned, score_cols)
    if not impact.empty:
        top_impact = impact.iloc[0]
        report_rows.append(
            {
                "Mục báo cáo": "Khía cạnh có độ dốc nguy cơ mạnh nhất",
                "Kết quả": f"{construct_label(top_impact['Construct'])} ({top_impact['Thay đổi khi construct tăng (%)']:.2f} điểm %)",
                "Diễn giải": "Chênh lệch nguy cơ giữa nhóm có điểm khía cạnh thấp và cao.",
            }
        )

    gap_df = top_target_gap_questions(processed, top_n=10, min_category_n=40, df=filtered_raw)
    if not gap_df.empty:
        top_gap = gap_df.iloc[0]
        report_rows.append(
            {
                "Mục báo cáo": "Yếu tố thành phần tạo chênh lệch lớn nhất",
                "Kết quả": f"{question_label(top_gap['qnum'], top_gap['question'])} - {top_gap['At Risk Gap (%)']:.2f} điểm %",
                "Diễn giải": "Yếu tố thành phần có khoảng cách nguy cơ lớn nhất giữa các nhóm phản hồi.",
            }
        )

    if score_cols:
        group_summary = all_demographic_mental_summary(filtered_raw, filtered_cleaned, score_cols)
        if not group_summary.empty:
            top_group = group_summary.iloc[0]
            report_rows.append(
                {
                    "Mục báo cáo": "Phân tầng cần chú ý nhất",
                    "Kết quả": f"{top_group['Dimension']} = {top_group['Group']} ({top_group['At Risk Rate']:.2f}%)",
                    "Diễn giải": f"Khía cạnh nổi bật hơn trung bình: {construct_label(top_group['Most elevated construct'])} (+{top_group['Elevation vs overall']:.2f}).",
                }
            )

    report_df = pd.DataFrame(report_rows)
    render_table(report_df, height=260)
    st.download_button(
        "Tải báo cáo tóm tắt CSV",
        data=report_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="mental_health_dashboard_summary.csv",
        mime="text/csv",
    )

    row = st.columns([0.85, 1.15], gap="large")
    with row[0]:
        st.plotly_chart(make_target_donut(filtered_raw), width="stretch", config=PLOT_CONFIG, key="report_target_donut")
    with row[1]:
        if not impact.empty:
            fig = make_construct_delta_chart(impact, height=430)
            if fig is not None:
                st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key="report_construct_delta")

    section_head(
        "Chất lượng dữ liệu",
        "Các bảng này giúp xác định phần dữ liệu nào đủ chắc để diễn giải và phần nào chỉ nên xem như tín hiệu gợi ý.",
    )
    render_table(compact_data_quality_summary(raw_df, processed), height=220)

    hms_quality = hms_data_quality_summary(processed)
    if not hms_quality.empty:
        render_table(hms_quality, height=260)

    section_head(
        "Khung đọc dashboard",
        "Mỗi trang trong dashboard được nối với một câu hỏi phân tích rõ ràng để tránh biến dashboard thành danh sách biểu đồ rời rạc.",
    )
    render_table(research_question_table(), height=260)

    section_head(
        "Ý nghĩa các khía cạnh phân tích",
        "Bảng này là phần giải thích tri thức dữ liệu: mỗi chỉ số đại diện cho khái niệm nào và được tạo từ câu hỏi nguồn nào.",
    )
    render_table(construct_definition_table(), height=420)

    st.markdown(
        """
        <div class="note-box">
            <b>Giới hạn diễn giải:</b> Dashboard này là phân tích mô tả dữ liệu khảo sát.
            Kết quả không chứng minh nguyên nhân, không thay thế đánh giá lâm sàng, và cần thận trọng với nhóm có cỡ mẫu nhỏ hoặc missing cao.
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_methodology_tab(raw_df: pd.DataFrame, processed) -> None:
    section_head(
        "Phương pháp và giới hạn diễn giải",
        "Phần này giúp người xem hiểu dashboard đo cái gì, tạo chỉ số như thế nào và đâu là giới hạn khi đọc kết quả.",
    )

    section_head(
        "Khung câu hỏi nghiên cứu",
        "Bảng này là bản đồ đọc dashboard: mỗi biểu đồ phải trả lời một câu hỏi, không chỉ hiển thị thống kê.",
    )
    render_table(research_question_table(), height=260)

    quality_df = compact_data_quality_summary(raw_df, processed)
    render_table(quality_df)

    hms_quality = hms_data_quality_summary(processed)
    if not hms_quality.empty:
        section_head(
            "Kiểm tra dữ liệu nhóm sinh viên",
            "Bảng này kiểm tra missing, ngoại lệ tuổi và độ phủ khía cạnh trong nhóm sinh viên sau khi quy đổi về cùng thang phân tích.",
        )
        render_table(hms_quality, height=260)

    section_head(
        "Construct được tạo từ dữ liệu khảo sát",
        "Các feature chính của dashboard được tạo bằng cách gom nhóm câu hỏi có cùng ý nghĩa nghiên cứu.",
    )

    render_table(construct_definition_table(), height=420)

    st.markdown(
        """
        <div class="note-box">
            <b>Giới hạn quan trọng:</b> Dashboard này mô tả mối liên hệ trong dữ liệu khảo sát cắt ngang.
            Kết quả không chứng minh nguyên nhân, không thay thế đánh giá lâm sàng, và cần thận trọng với biến thiếu dữ liệu nhiều hoặc nhóm phản hồi có cỡ mẫu nhỏ.
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_construct_effect_tab(filtered_cleaned: pd.DataFrame) -> None:
    score_cols = [feature for feature in RESEARCH_FEATURES if feature in filtered_cleaned.columns]
    section_head(
        "Bối cảnh nào liên quan tới nguy cơ cao hơn?",
        "Đọc các khía cạnh như thang ý nghĩa 0-100: điểm cao hơn là bất lợi, rủi ro hoặc thiếu hụt cao hơn trong bối cảnh đó.",
    )
    if not score_cols:
        st.warning("Không có khía cạnh phân tích để hiển thị.")
        return

    impact = construct_impact_table(filtered_cleaned, score_cols)
    profile = construct_profile_table(filtered_cleaned, score_cols)
    fig = make_construct_delta_chart(impact, height=500)
    if fig is not None:
        st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key="construct_delta_full")

    fig = make_construct_profile_chart(profile, height=500)
    if fig is not None:
        st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key="construct_profile_full")

    selected = st.selectbox(
        "Chọn khía cạnh để đọc đường xu hướng",
        score_cols,
        format_func=construct_label,
        key="construct_trend_select",
    )
    trend = construct_trend_table(filtered_cleaned, selected, bins=4)
    trend_fig = make_construct_trend_chart(trend, selected)
    if trend_fig is not None:
        st.plotly_chart(trend_fig, width="stretch", config=PLOT_CONFIG, key=f"construct_trend_{selected}")
    with st.expander("Chi tiết dữ liệu đường xu hướng", expanded=False):
        render_table(trend, height=260)


def render_raw_construct_data_tab(filtered_raw: pd.DataFrame, processed) -> None:
    section_head(
        "Mỗi khía cạnh được tạo từ phản hồi nào?",
        "Mục này giải thích mỗi khía cạnh được tạo từ những tín hiệu khảo sát nào, phản hồi nào phổ biến và phản hồi nào đi cùng nguy cơ cao hơn.",
    )
    if filtered_raw.empty:
        st.warning("Không còn bản ghi nào sau khi áp dụng bộ lọc.")
        return

    construct = st.selectbox(
        "Chọn khía cạnh để xem dữ liệu thành phần",
        RESEARCH_FEATURES,
        format_func=lambda name: f"{construct_label(name)}",
        key="raw_construct_selector",
    )
    definition = RESEARCH_FEATURE_DEFINITIONS[construct]
    insight_box(
        f"{construct_label(construct)} đang đại diện cho điều gì?",
        CONSTRUCT_ANALYST_MEANING.get(construct, definition["meaning"]),
        "Các biểu đồ dưới đây giúp giải thích dữ liệu thành phần, không dùng để kết luận riêng lẻ từ một mã cột.",
    )

    source_table = construct_source_question_table(processed, construct)
    if source_table.empty:
        return
    available_qnums = source_table["qnum"].dropna().astype(int).tolist()

    section_head(
        "Nguồn dữ liệu của khía cạnh đang chọn",
        "Các yếu tố dưới đây mô tả những phản hồi học sinh đóng góp vào khía cạnh đang chọn.",
    )
    with st.expander("Danh sách tín hiệu nguồn", expanded=False):
        render_table(source_table, height=260)

    hms_source_table = hms_construct_source_table(construct)
    if not hms_source_table.empty:
        section_head(
            "Tín hiệu tương thích ở nhóm sinh viên",
            "Các tín hiệu sinh viên được quy đổi cùng chiều để cùng biểu diễn một ý nghĩa phân tích với nhóm học sinh.",
        )
        with st.expander("Nguồn tín hiệu sinh viên", expanded=False):
            render_table(hms_source_table, height=150)

    selected_qnums = st.multiselect(
        "Chọn yếu tố thành phần để vẽ",
        available_qnums,
        default=available_qnums[: min(3, len(available_qnums))],
        format_func=lambda q: question_label(q),
        key="raw_construct_q_selector",
    )

    for qnum in selected_qnums:
        col = find_q_col(filtered_raw, qnum)
        if col is None:
            continue
        question_name = question_label(qnum, processed.raw_col_to_display.get(col, col))
        freq_df = question_frequency_table(filtered_raw, col, qnum)
        target_df = target_by_response_table(filtered_raw, col, qnum)

        section_head(
            question_name,
            "Biểu đồ trái cho biết phản hồi nào phổ biến. Biểu đồ phải cho biết phản hồi nào đi cùng tỷ lệ nguy cơ cao hơn.",
        )

        row = st.columns(2, gap="large")
        with row[0]:
            st.plotly_chart(
                make_frequency_chart(freq_df, f"Phản hồi phổ biến: {question_name}", height=420),
                width="stretch",
                config=PLOT_CONFIG,
                key=f"raw_construct_freq_{construct}_{qnum}",
            )
        with row[1]:
            st.plotly_chart(
                make_target_rate_bar(target_df, f"Nguy cơ theo phản hồi: {question_name}", height=420),
                width="stretch",
                config=PLOT_CONFIG,
                key=f"raw_construct_risk_{construct}_{qnum}",
            )

        with st.expander("Chi tiết phản hồi", expanded=False):
            view = target_df[["Response", "Count", "At Risk Count", "At Risk Rate"]].copy()
            render_table(view, height=240)


def render_group_pressure_tab(filtered_raw: pd.DataFrame, filtered_cleaned: pd.DataFrame, processed) -> None:
    section_head(
        "Nhóm nào cần được chú ý trước?",
        "Kết hợp tỷ lệ nguy cơ với các khía cạnh bối cảnh để tìm nhóm nổi bật theo tuổi, giới tính và bậc học.",
    )

    scope_options = {
        "Tổng hợp học sinh + sinh viên": None,
        "Học sinh": MENTAL_SCHOOL_POPULATION_LABEL,
        "Sinh viên": HMS_POPULATION_LABEL,
    }
    selected_scope = st.radio(
        "Phạm vi phân tầng",
        list(scope_options.keys()),
        horizontal=True,
        key="group_population_scope",
    )
    selected_population = scope_options[selected_scope]

    if selected_population and POPULATION_COLUMN not in filtered_raw.columns:
        st.warning("Dataset hiện tại chưa có cột Population để tách học sinh và sinh viên.")
        return
    if selected_population:
        scoped_raw = filtered_raw[filtered_raw[POPULATION_COLUMN] == selected_population].copy()
        scoped_cleaned = filtered_cleaned.loc[scoped_raw.index].copy()
    else:
        scoped_raw = filtered_raw.copy()
        scoped_cleaned = filtered_cleaned.copy()

    score_cols = [feature for feature in RESEARCH_FEATURES if feature in scoped_cleaned.columns]
    st.caption(f"Đang phân tích: {selected_scope} | Số bản ghi: {scoped_raw.shape[0]:,}")

    if scoped_raw.empty or not score_cols:
        return

    combined_summary = all_demographic_mental_summary(scoped_raw, scoped_cleaned, score_cols)
    if not combined_summary.empty:
        fig = make_demographic_mental_summary_chart(
            combined_summary.head(12),
            "Ranking chung: nhóm nào có At Risk cao nhất?",
            height=520,
        )
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key=f"group_combined_summary_{selected_scope}")

    section_head(
        "Tách riêng từng lát cắt phân tầng",
        "Tách riêng bậc/lớp, độ tuổi và giới tính để tránh trộn nhiều loại nhóm vào một biểu đồ.",
    )
    prevalence_cols = st.columns(3, gap="large")
    prevalence_specs = [
        ("Bậc/lớp", 3, "Grade", "At Risk cao hơn theo bậc/lớp nào?"),
        ("Độ tuổi", 1, "Age", "At Risk cao hơn ở nhóm tuổi nào?"),
        ("Giới tính", 2, "Gender", "At Risk cao hơn theo giới tính nào?"),
    ]
    for col_slot, (_label, qnum_demo, group_col, chart_title) in zip(prevalence_cols, prevalence_specs):
        prevalence_df = target_prevalence_by_group(scoped_raw, qnum_demo, group_col)
        with col_slot:
            if not prevalence_df.empty:
                st.plotly_chart(
                    make_prevalence_bar(prevalence_df, group_col, chart_title, height=390),
                    width="stretch",
                    config=PLOT_CONFIG,
                    key=f"group_prevalence_{group_col}_{selected_scope}",
                )

    group_options = {"Bậc/lớp": (3, "Grade"), "Độ tuổi": (1, "Age"), "Giới tính": (2, "Gender")}
    selected_group = st.selectbox("Chọn nhóm phân tầng", list(group_options.keys()), key="group_pressure_select")
    qnum, group_name = group_options[selected_group]
    table = demographic_construct_table(scoped_raw, scoped_cleaned, qnum, group_name, score_cols)
    group_summary = demographic_mental_summary(scoped_raw, scoped_cleaned, qnum, group_name, score_cols)
    if group_summary.empty:
        return

    selected_detail_group = st.selectbox(
        "Chọn một nhóm cụ thể để giải thích bằng các khía cạnh",
        group_summary[group_name].tolist(),
        format_func=response_label,
        key=f"group_detail_select_{group_name}",
    )
    detail_profile = group_construct_profile(
        scoped_raw,
        scoped_cleaned,
        qnum,
        group_name,
        selected_detail_group,
        score_cols,
    )
    if not detail_profile.empty:
        top_context = detail_profile.iloc[0]
        selected_rate = float(group_summary.loc[group_summary[group_name] == selected_detail_group, "At Risk Rate"].iloc[0])
        insight_box(
            f"Vì sao nhóm '{response_label(selected_detail_group)}' đáng chú ý?",
            f"Nhóm này có tỷ lệ nguy cơ {selected_rate:.2f}%. Khía cạnh cao hơn trung bình mẫu nhiều nhất là {construct_label(top_context['Construct'])} (+{top_context['Difference']:.2f}), gợi ý bối cảnh cần xem sâu hơn.",
            "Đây là mô tả bối cảnh liên quan; không chứng minh khía cạnh đó gây ra vấn đề sức khỏe tinh thần.",
        )
        profile_fig = make_group_construct_profile_chart(detail_profile, selected_detail_group)
        if profile_fig is not None:
            st.plotly_chart(profile_fig, width="stretch", config=PLOT_CONFIG, key=f"group_construct_profile_{group_name}_{selected_detail_group}")

    score_fig = make_demographic_construct_heatmap(
        table,
        group_name,
        "Mean score",
        f"Khía cạnh bối cảnh nổi bật theo {selected_group.lower()}",
    )
    if score_fig is not None:
        st.plotly_chart(score_fig, width="stretch", config=PLOT_CONFIG, key="group_construct_heatmap")

    risk_by_group = target_prevalence_by_group(scoped_raw, qnum, group_name)
    if not risk_by_group.empty:
        st.plotly_chart(
            make_prevalence_bar(risk_by_group, group_name, f"Tỷ lệ nguy cơ theo {selected_group.lower()}", height=390),
            width="stretch",
            config=PLOT_CONFIG,
            key="group_at_risk_bar",
        )
    section_head(
        f"Tóm tắt nguy cơ theo {selected_group.lower()}",
        "Bảng này xếp hạng từng nhóm theo tỷ lệ nguy cơ và khía cạnh nổi bật hơn toàn mẫu.",
    )
    group_summary_chart = group_summary.rename(columns={group_name: "Group"}).copy()
    group_summary_chart["Dimension"] = selected_group
    fig = make_demographic_mental_summary_chart(
        group_summary_chart,
        f"Nhóm {selected_group.lower()} nào có At Risk cao nhất?",
        height=430,
    )
    if fig is not None:
        st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key=f"group_summary_chart_{group_name}")
    with st.expander("Chi tiết dữ liệu theo nhóm", expanded=False):
        render_table(group_summary, height=260)
    section_head(
        f"Điểm khía cạnh theo {selected_group.lower()}",
        "Bảng này phân rã từng nhóm theo các khía cạnh bối cảnh để xem điểm trung bình và số bản ghi có dữ liệu.",
    )
    with st.expander("Chi tiết điểm khía cạnh", expanded=False):
        render_table(table, height=360)

    section_head(
        "Nhóm này trả lời khác toàn mẫu ở điểm nào?",
        "Phân rã một khía cạnh thành các yếu tố thành phần để xem nhóm đang chọn có mẫu phản hồi nào nổi bật và phản hồi đó đi cùng nguy cơ ra sao.",
    )
    analysis_cols = st.columns(2, gap="large")
    with analysis_cols[0]:
        selected_construct = st.selectbox(
            "Chọn khía cạnh để phân rã",
            RESEARCH_FEATURES,
            format_func=construct_label,
            key="group_response_construct",
        )
    with analysis_cols[1]:
        selected_detail_group_for_response = st.selectbox(
            "Chọn nhóm phân tích",
            group_summary[group_name].tolist() if not group_summary.empty else [],
            format_func=response_label,
            key=f"group_response_detail_select_{group_name}",
        )

    if selected_detail_group_for_response:
        patterns = group_source_response_patterns(
            scoped_raw,
            scoped_cleaned,
            qnum,
            selected_detail_group_for_response,
            selected_construct,
        )
        if not patterns.empty:
            skew_fig = make_response_pattern_skew_chart(patterns, selected_detail_group_for_response)
            if skew_fig is not None:
                st.plotly_chart(skew_fig, width="stretch", config=PLOT_CONFIG, key="group_response_skew")

            q_options = sorted(patterns["qnum"].dropna().unique().astype(int).tolist())
            selected_qnum = st.selectbox(
                "Chọn yếu tố thành phần để đọc chi tiết",
                q_options,
                format_func=lambda q: question_label(q, QNUM_TO_ENGLISH.get(q, q)),
                key="group_response_source_q",
            )
            response_df = source_question_response_comparison(patterns, selected_qnum)

            detail_cols = st.columns(2, gap="large")
            with detail_cols[0]:
                compare_fig = make_source_response_comparison_chart(response_df, selected_qnum, selected_detail_group_for_response)
                if compare_fig is not None:
                    st.plotly_chart(compare_fig, width="stretch", config=PLOT_CONFIG, key="group_response_compare")
            with detail_cols[1]:
                risk_fig = make_source_response_risk_chart(response_df, selected_qnum, selected_detail_group_for_response)
                if risk_fig is not None:
                    st.plotly_chart(risk_fig, width="stretch", config=PLOT_CONFIG, key="group_response_risk")

            with st.expander("Chi tiết phản hồi khác toàn mẫu", expanded=False):
                render_table(
                    patterns[
                        [
                            "qnum",
                            "Question",
                            "Response",
                            "Selected group %",
                            "Overall %",
                            "Difference",
                            "Selected group At Risk %",
                            "Selected group n",
                        ]
                    ].head(30),
                    height=420,
                )


def render_data_quality_tab(raw_df: pd.DataFrame, filtered_raw: pd.DataFrame, processed) -> None:
    section_head(
        "Dữ liệu có đủ sạch để đọc không?",
        "Phần này chỉ giữ các kiểm tra cần thiết để biết dữ liệu nào đủ tốt để trực quan hóa và diễn giải.",
    )

    render_table(semantic_quality_summary(raw_df, processed), height=320)

    missing_df = top_missing_questions(processed, top_n=12)
    if not missing_df.empty:
        section_head(
            "Yếu tố nào còn thiếu dữ liệu nhiều nhất?",
            "Biểu đồ này giúp quyết định phần nào nên đọc thận trọng hoặc cần bổ sung dữ liệu trước khi kết luận.",
        )
        fig = make_missing_bar(missing_df, height=520)
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key="quality_missing_bar")

    section_head(
        "Các khía cạnh đang đại diện cho điều gì?",
        "Bảng này là bản đồ ngữ nghĩa để người xem hiểu mỗi chỉ số đại diện cho bối cảnh tâm lý - giáo dục nào.",
    )
    with st.expander("Chi tiết ý nghĩa các khía cạnh", expanded=False):
        render_table(construct_definition_table(), height=380)


@st.cache_data(ttl=60, show_spinner=False)
def load_cached_chatbot_gold_tables() -> Dict[str, pd.DataFrame]:
    from dashboard_gcs_loader import load_chat_gold_tables

    return load_chat_gold_tables()


def aggregate_chat_distribution(frame: pd.DataFrame, value_column: str, label_column: str) -> pd.DataFrame:
    if frame.empty or value_column not in frame.columns or "count" not in frame.columns:
        return pd.DataFrame(columns=[label_column, "Messages", "Share %"])
    result = (
        frame.groupby(value_column, as_index=False)["count"]
        .sum()
        .rename(columns={value_column: label_column, "count": "Messages"})
        .sort_values("Messages", ascending=False)
    )
    total = float(result["Messages"].sum())
    result["Share %"] = np.where(total > 0, (result["Messages"] / total * 100).round(2), 0.0)
    return result


CHAT_COUNT_COLUMNS = [
    "total_messages",
    "rag_messages",
    "non_rag_messages",
    "high_risk_count",
    "medium_risk_count",
    "low_risk_count",
    "positive_count",
    "neutral_count",
    "negative_count",
    "harm_intent_count",
    "self_harm_count",
    "mental_health_count",
    "rag_question_count",
    "general_count",
]

CHAT_RISK_LABELS = {
    "low": "Thấp",
    "medium": "Trung bình",
    "high": "Cao",
    "unknown": "Không rõ",
}

CHAT_SENTIMENT_LABELS = {
    "positive": "Tích cực",
    "neutral": "Trung tính",
    "negative": "Tiêu cực",
    "unknown": "Không rõ",
}

CHAT_RISK_COLORS = {
    "Thấp": THEME["teal"],
    "Trung bình": THEME["gold"],
    "Cao": THEME["danger"],
    "Không rõ": THEME["muted"],
}

CHAT_SENTIMENT_COLORS = {
    "Tích cực": THEME["secondary"],
    "Trung tính": THEME["muted"],
    "Tiêu cực": THEME["danger"],
    "Không rõ": THEME["muted"],
}

CHAT_TOPIC_LABELS = {
    "rag_question": "Hỏi tài liệu / hỗ trợ học tập",
    "harm_intent": "Nội dung nguy cơ gây hại",
    "self_harm": "Nội dung tự gây hại",
    "mental_health": "Sức khỏe tâm lý",
    "general": "Hỗ trợ chung",
    "unknown": "Không rõ chủ đề",
}

CHAT_HOURLY_TOPIC_COLUMNS = {
    "rag_question_count": "Hỏi tài liệu / hỗ trợ học tập",
    "harm_intent_count": "Nội dung nguy cơ gây hại",
    "self_harm_count": "Nội dung tự gây hại",
    "mental_health_count": "Sức khỏe tâm lý",
    "general_count": "Hỗ trợ chung",
}


def chat_label(value: object, mapping: Dict[str, str]) -> str:
    key = str(value).strip().lower()
    return mapping.get(key, str(value))


def chat_pct(value: float) -> str:
    return f"{float(value):.1f}%"


def chat_count(value: float) -> str:
    return f"{int(round(float(value))):,} lượt"


def chat_time_label(value: object, include_hour: bool = True) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return "-"
    return timestamp.strftime("%d/%m %H:00") if include_hour else timestamp.strftime("%d/%m")


def prepare_chat_timeline(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty or "date" not in metrics.columns or "hour" not in metrics.columns:
        return pd.DataFrame()
    timeline = metrics.copy()
    numeric_columns = [
        "hour",
        "total_messages",
        "unique_sessions",
        "rag_messages",
        "non_rag_messages",
        "avg_question_length",
        "avg_answer_length",
        "high_risk_count",
        "medium_risk_count",
        "low_risk_count",
        "positive_count",
        "neutral_count",
        "negative_count",
        "harm_intent_count",
        "self_harm_count",
        "mental_health_count",
        "rag_question_count",
        "general_count",
    ]
    for column in numeric_columns:
        if column not in timeline.columns:
            timeline[column] = 0
        timeline[column] = pd.to_numeric(timeline[column], errors="coerce").fillna(0)
    if not timeline.empty:
        for avg_column in ["avg_question_length", "avg_answer_length"]:
            timeline[f"__weighted_{avg_column}"] = timeline[avg_column] * timeline["total_messages"]
        grouped = (
            timeline.groupby(["date", "hour"], as_index=False)
            [[
                *CHAT_COUNT_COLUMNS,
                "__weighted_avg_question_length",
                "__weighted_avg_answer_length",
            ]]
            .sum()
        )
        grouped["avg_question_length"] = np.where(
            grouped["total_messages"] > 0,
            (grouped["__weighted_avg_question_length"] / grouped["total_messages"]).round(2),
            0.0,
        )
        grouped["avg_answer_length"] = np.where(
            grouped["total_messages"] > 0,
            (grouped["__weighted_avg_answer_length"] / grouped["total_messages"]).round(2),
            0.0,
        )
        timeline = grouped.drop(columns=["__weighted_avg_question_length", "__weighted_avg_answer_length"])
    timeline["date_hour"] = pd.to_datetime(
        timeline["date"].astype(str)
        + " "
        + timeline["hour"].round().astype(int).astype(str).str.zfill(2)
        + ":00",
        errors="coerce",
    )
    timeline = timeline.dropna(subset=["date_hour"]).sort_values("date_hour").reset_index(drop=True)
    timeline["rag_rate_pct"] = np.where(
        timeline["total_messages"] > 0,
        (timeline["rag_messages"] / timeline["total_messages"] * 100).round(2),
        0.0,
    )
    return timeline


def aggregate_chat_timeframe(timeline: pd.DataFrame, level: str) -> pd.DataFrame:
    if timeline.empty:
        return pd.DataFrame()
    work = timeline.copy()
    if level == "day":
        work["period"] = work["date_hour"].dt.floor("D")
        label = "date"
    elif level == "month":
        work["period"] = work["date_hour"].dt.to_period("M").dt.to_timestamp()
        label = "month"
    elif level == "year":
        work["period"] = work["date_hour"].dt.to_period("Y").dt.to_timestamp()
        label = "year"
    else:
        work["period"] = work["date_hour"]
        label = "date_hour"

    count_columns = [column for column in CHAT_COUNT_COLUMNS if column in work.columns]
    grouped = work.groupby("period", as_index=False)[count_columns].sum()
    grouped["rag_rate_pct"] = np.where(
        grouped["total_messages"] > 0,
        (grouped["rag_messages"] / grouped["total_messages"] * 100).round(2),
        0.0,
    )
    grouped = grouped.rename(columns={"period": label})
    return grouped.sort_values(label).reset_index(drop=True)


def aggregate_chat_hour_of_day(timeline: pd.DataFrame) -> pd.DataFrame:
    if timeline.empty or "date_hour" not in timeline.columns:
        return pd.DataFrame()
    work = timeline.copy()
    work["hour"] = work["date_hour"].dt.hour
    count_columns = [column for column in CHAT_COUNT_COLUMNS if column in work.columns]
    grouped = work.groupby("hour", as_index=False)[count_columns].sum()
    grouped["hour_label"] = grouped["hour"].astype(int).astype(str).str.zfill(2) + ":00"
    grouped["rag_rate_pct"] = np.where(
        grouped["total_messages"] > 0,
        (grouped["rag_messages"] / grouped["total_messages"] * 100).round(2),
        0.0,
    )
    return grouped.sort_values("hour").reset_index(drop=True)


def make_chat_time_line(frame: pd.DataFrame, x_column: str, title: str):
    if frame.empty or x_column not in frame.columns or "total_messages" not in frame.columns:
        return None
    plot_df = frame[[x_column, "total_messages"]].copy()
    plot_df["total_messages"] = pd.to_numeric(plot_df["total_messages"], errors="coerce").fillna(0)
    fig = px.line(
        plot_df,
        x=x_column,
        y="total_messages",
        markers=True,
        title=title,
        labels={x_column: "", "total_messages": "Lượt chat"},
    )
    fig.update_traces(line=dict(color=THEME["primary"], width=3), marker=dict(size=8))
    fig.update_layout(hovermode="x unified", showlegend=False, yaxis_title="Lượt chat")
    if x_column == "hour_label":
        fig.update_xaxes(type="category", categoryorder="array", categoryarray=plot_df[x_column].tolist(), title_text="Giờ trong ngày")
    elif x_column == "date":
        fig.update_xaxes(tickformat="%b %d", title_text="Ngày")
    elif x_column == "month":
        fig.update_xaxes(tickformat="%b %Y", title_text="Tháng")
    elif x_column == "year":
        fig.update_xaxes(tickformat="%Y", title_text="Năm")
    fig.update_yaxes(rangemode="tozero")
    return style_figure(fig, 390)


def make_chat_sentiment_line(frame: pd.DataFrame, x_column: str, title: str):
    if frame.empty or x_column not in frame.columns:
        return None
    value_columns = [column for column in ["positive_count", "neutral_count", "negative_count"] if column in frame.columns]
    if not value_columns:
        return None
    labels = {
        "positive_count": "Tích cực",
        "neutral_count": "Trung tính",
        "negative_count": "Tiêu cực",
    }
    plot_df = frame[[x_column, *value_columns]].copy()
    for column in value_columns:
        plot_df[column] = pd.to_numeric(plot_df[column], errors="coerce").fillna(0)
    plot_df = plot_df.melt(
        id_vars=x_column,
        value_vars=value_columns,
        var_name="Cảm xúc",
        value_name="Messages",
    )
    plot_df["Cảm xúc"] = plot_df["Cảm xúc"].map(labels).fillna(plot_df["Cảm xúc"])
    fig = px.line(
        plot_df,
        x=x_column,
        y="Messages",
        color="Cảm xúc",
        markers=True,
        title=title,
        color_discrete_map=CHAT_SENTIMENT_COLORS,
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=8))
    fig.update_layout(hovermode="x unified", legend_title_text="", yaxis_title="Lượt chat")
    if x_column == "date":
        fig.update_xaxes(tickformat="%b %d", title_text="Ngày")
    fig.update_yaxes(rangemode="tozero")
    return style_figure(fig, 390)


def prepare_chat_construct_detail(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty or "chat_construct" not in summary.columns:
        return pd.DataFrame()
    detail = summary.copy()
    for column in [
        "count",
        "high_risk_count",
        "negative_count",
        "rag_messages",
        "unique_sessions",
        "avg_question_length",
        "percentage",
        "high_risk_rate",
        "negative_rate",
        "rag_rate",
    ]:
        if column not in detail.columns:
            detail[column] = 0
        detail[column] = pd.to_numeric(detail[column], errors="coerce").fillna(0)
    if "date" in detail.columns:
        detail["date"] = pd.to_datetime(detail["date"], errors="coerce")
    detail["chat_construct"] = detail["chat_construct"].fillna("Không rõ cụm").astype(str)
    detail = detail.dropna(subset=["date"]).reset_index(drop=True)
    if detail.empty:
        return detail
    detail["__weighted_avg_question_length"] = detail["avg_question_length"] * detail["count"]
    detail = (
        detail.groupby(["date", "chat_construct"], as_index=False)
        [[
            "count",
            "high_risk_count",
            "negative_count",
            "rag_messages",
            "unique_sessions",
            "__weighted_avg_question_length",
        ]]
        .sum()
    )
    detail["avg_question_length"] = np.where(
        detail["count"] > 0,
        (detail["__weighted_avg_question_length"] / detail["count"]).round(2),
        0.0,
    )
    date_total = detail.groupby("date")["count"].transform("sum")
    detail["percentage"] = np.where(date_total > 0, (detail["count"] / date_total * 100).round(2), 0.0)
    detail["high_risk_rate"] = np.where(detail["count"] > 0, (detail["high_risk_count"] / detail["count"] * 100).round(2), 0.0)
    detail["negative_rate"] = np.where(detail["count"] > 0, (detail["negative_count"] / detail["count"] * 100).round(2), 0.0)
    detail["rag_rate"] = np.where(detail["count"] > 0, (detail["rag_messages"] / detail["count"] * 100).round(2), 0.0)
    return detail.drop(columns=["__weighted_avg_question_length"]).reset_index(drop=True)


def chat_construct_options(detail: pd.DataFrame) -> List[str]:
    if detail.empty or "chat_construct" not in detail.columns:
        return []
    totals = detail.groupby("chat_construct")["count"].sum().sort_values(ascending=False)
    return totals.index.tolist()


def chat_construct_summary_metrics(detail: pd.DataFrame, selected_construct: str) -> Dict[str, object]:
    selected = detail[detail["chat_construct"] == selected_construct].copy()
    total_all = float(detail["count"].sum()) if not detail.empty else 0.0
    total = int(selected["count"].sum()) if not selected.empty else 0
    high_risk = int(selected["high_risk_count"].sum()) if not selected.empty else 0
    negative = int(selected["negative_count"].sum()) if not selected.empty else 0
    rag = int(selected["rag_messages"].sum()) if not selected.empty else 0
    sessions = int(selected["unique_sessions"].sum()) if not selected.empty else 0
    peak_label = "-"
    peak_messages = 0
    if not selected.empty:
        peak = selected.sort_values("count", ascending=False).iloc[0]
        peak_label = chat_time_label(peak.get("date"), include_hour=False)
        peak_messages = int(peak.get("count", 0))
    return {
        "total": total,
        "share": round(total / total_all * 100, 1) if total_all else 0.0,
        "high_risk": high_risk,
        "high_risk_rate": round(high_risk / total * 100, 1) if total else 0.0,
        "negative": negative,
        "negative_rate": round(negative / total * 100, 1) if total else 0.0,
        "rag": rag,
        "rag_rate": round(rag / total * 100, 1) if total else 0.0,
        "sessions": sessions,
        "peak_label": peak_label,
        "peak_messages": peak_messages,
    }


def make_chat_construct_detail_trend(detail: pd.DataFrame, selected_construct: str):
    selected = detail[detail["chat_construct"] == selected_construct].copy()
    if selected.empty:
        return None
    selected = selected.sort_values("date")
    plot = selected[["date", "count", "high_risk_count", "negative_count"]].melt(
        id_vars="date",
        value_vars=["count", "high_risk_count", "negative_count"],
        var_name="Chỉ số",
        value_name="Lượt",
    )
    labels = {
        "count": "Tổng lượt",
        "high_risk_count": "Nguy cơ cao",
        "negative_count": "Cảm xúc tiêu cực",
    }
    plot["Chỉ số"] = plot["Chỉ số"].map(labels)
    fig = px.line(
        plot,
        x="date",
        y="Lượt",
        color="Chỉ số",
        markers=True,
        title=f"Diễn biến theo ngày của cụm: {selected_construct}",
        color_discrete_map={
            "Tổng lượt": THEME["primary"],
            "Nguy cơ cao": THEME["danger"],
            "Cảm xúc tiêu cực": THEME["gold"],
        },
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=8))
    fig.update_xaxes(tickformat="%d/%m", title_text="Ngày")
    fig.update_yaxes(rangemode="tozero")
    fig.update_layout(hovermode="x unified", yaxis_title="Lượt")
    return board_chart_layout(fig, 390, right_margin=48, bottom_margin=72)


def make_chat_construct_detail_rates(metrics: Dict[str, object], selected_construct: str):
    plot = pd.DataFrame(
        {
            "Chỉ số": ["Nguy cơ cao", "Cảm xúc tiêu cực", "Dùng tài liệu tham chiếu"],
            "Tỷ lệ (%)": [metrics["high_risk_rate"], metrics["negative_rate"], metrics["rag_rate"]],
        }
    )
    fig = px.bar(
        plot,
        x="Chỉ số",
        y="Tỷ lệ (%)",
        text="Tỷ lệ (%)",
        color="Chỉ số",
        color_discrete_map={
            "Nguy cơ cao": THEME["danger"],
            "Cảm xúc tiêu cực": THEME["gold"],
            "Dùng tài liệu tham chiếu": THEME["primary"],
        },
        title=f"Cụm {selected_construct} có tín hiệu gì đáng chú ý?",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_yaxes(rangemode="tozero")
    fig.update_layout(xaxis_title="", yaxis_title="Tỷ lệ trong cụm", showlegend=False)
    return board_chart_layout(fig, 390, right_margin=48, bottom_margin=72)


def chat_construct_detail_table(detail: pd.DataFrame, selected_construct: str) -> pd.DataFrame:
    selected = detail[detail["chat_construct"] == selected_construct].copy()
    if selected.empty:
        return pd.DataFrame()
    selected = selected.sort_values("date")
    table = selected[
        [
            "date",
            "count",
            "high_risk_count",
            "negative_count",
            "rag_messages",
            "unique_sessions",
            "high_risk_rate",
            "negative_rate",
            "rag_rate",
        ]
    ].copy()
    table["date"] = table["date"].dt.strftime("%d/%m/%Y")
    return table.rename(
        columns={
            "date": "Ngày",
            "count": "Tổng lượt",
            "high_risk_count": "Nguy cơ cao",
            "negative_count": "Cảm xúc tiêu cực",
            "rag_messages": "Dùng tài liệu tham chiếu",
            "unique_sessions": "Phiên hội thoại",
            "high_risk_rate": "Tỷ lệ nguy cơ cao (%)",
            "negative_rate": "Tỷ lệ tiêu cực (%)",
            "rag_rate": "Tỷ lệ dùng tài liệu (%)",
        }
    )


def chat_peak_topic_from_row(row: pd.Series) -> Tuple[str, int]:
    counts = {}
    for column, label in CHAT_HOURLY_TOPIC_COLUMNS.items():
        if column in row.index:
            counts[label] = int(pd.to_numeric(pd.Series([row[column]]), errors="coerce").fillna(0).iloc[0])
    if not counts:
        return "Không rõ chủ đề", 0
    topic, count = max(counts.items(), key=lambda item: item[1])
    return topic, count


def build_chat_time_insights(
    timeline: pd.DataFrame,
    daily_timeline: pd.DataFrame,
    hourly_timeline: pd.DataFrame,
    topic_totals: pd.DataFrame,
    sentiment_totals: pd.DataFrame,
) -> Dict[str, object]:
    empty = {
        "peak_time": "-",
        "peak_messages": 0,
        "peak_share": 0.0,
        "peak_topic": "Không rõ chủ đề",
        "peak_topic_count": 0,
        "peak_high_risk": 0,
        "peak_negative": 0,
        "peak_rag": 0,
        "peak_reason": "Chưa có dữ liệu thời gian để xác định cao điểm.",
        "top_day": "-",
        "top_day_messages": 0,
        "top_hour": "-",
        "top_hour_messages": 0,
        "top_topic": "Không rõ chủ đề",
        "top_sentiment": "Không rõ cảm xúc",
    }
    if timeline.empty:
        return empty

    total_messages = float(pd.to_numeric(timeline.get("total_messages", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    peak = timeline.sort_values("total_messages", ascending=False).iloc[0]
    peak_messages = int(peak.get("total_messages", 0))
    peak_topic, peak_topic_count = chat_peak_topic_from_row(peak)
    peak_high_risk = int(peak.get("high_risk_count", 0))
    peak_negative = int(peak.get("negative_count", 0))
    peak_rag = int(peak.get("rag_messages", 0))

    top_day = daily_timeline.sort_values("total_messages", ascending=False).iloc[0] if not daily_timeline.empty else pd.Series(dtype=object)
    top_hour = hourly_timeline.sort_values("total_messages", ascending=False).iloc[0] if not hourly_timeline.empty else pd.Series(dtype=object)
    top_topic = "Không rõ chủ đề"
    if not topic_totals.empty and {"Topic", "Messages"}.issubset(topic_totals.columns):
        topic_row = topic_totals.sort_values("Messages", ascending=False).iloc[0]
        top_topic = chat_label(topic_row["Topic"], CHAT_TOPIC_LABELS)
    top_sentiment = "Không rõ cảm xúc"
    if not sentiment_totals.empty and {"Sentiment", "Messages"}.issubset(sentiment_totals.columns):
        sentiment_row = sentiment_totals.sort_values("Messages", ascending=False).iloc[0]
        top_sentiment = chat_label(sentiment_row["Sentiment"], CHAT_SENTIMENT_LABELS)

    risk_part = f"{peak_high_risk} lượt nguy cơ cao" if peak_high_risk else "không có lượt nguy cơ cao"
    negative_part = f"{peak_negative} lượt tiêu cực" if peak_negative else "không có lượt tiêu cực"
    rag_part = f"{peak_rag} lượt dùng tài liệu tham chiếu" if peak_rag else "không có lượt dùng tài liệu tham chiếu"
    peak_reason = (
        f"Cao điểm tập trung ở nhóm '{peak_topic}' ({peak_topic_count}/{peak_messages} lượt), "
        f"kèm {risk_part}, {negative_part} và {rag_part}."
    )
    return {
        "peak_time": chat_time_label(peak.get("date_hour")),
        "peak_messages": peak_messages,
        "peak_share": round(peak_messages / total_messages * 100, 1) if total_messages else 0.0,
        "peak_topic": peak_topic,
        "peak_topic_count": peak_topic_count,
        "peak_high_risk": peak_high_risk,
        "peak_negative": peak_negative,
        "peak_rag": peak_rag,
        "peak_reason": peak_reason,
        "top_day": chat_time_label(top_day.get("date"), include_hour=False) if not top_day.empty else "-",
        "top_day_messages": int(top_day.get("total_messages", 0)) if not top_day.empty else 0,
        "top_hour": str(top_hour.get("hour_label", "-")) if not top_hour.empty else "-",
        "top_hour_messages": int(top_hour.get("total_messages", 0)) if not top_hour.empty else 0,
        "top_topic": top_topic,
        "top_sentiment": top_sentiment,
    }


def make_chat_peak_slot_bar(timeline: pd.DataFrame):
    if timeline.empty or not {"date_hour", "total_messages"}.issubset(timeline.columns):
        return None
    plot = timeline.copy()
    for column in ["total_messages", "high_risk_count", "negative_count", "rag_messages"]:
        if column not in plot.columns:
            plot[column] = 0
        plot[column] = pd.to_numeric(plot[column], errors="coerce").fillna(0)
    plot = plot.sort_values("total_messages", ascending=False).head(8).copy()
    if plot.empty:
        return None
    plot["Khung thời gian"] = plot["date_hour"].apply(chat_time_label)
    plot["Chủ đề nổi bật"] = plot.apply(lambda row: chat_peak_topic_from_row(row)[0], axis=1)
    plot = plot.sort_values("total_messages", ascending=True)
    fig = px.bar(
        plot,
        x="total_messages",
        y="Khung thời gian",
        orientation="h",
        text="total_messages",
        color="high_risk_count",
        color_continuous_scale=[[0, THEME["primary_soft"]], [0.5, THEME["gold"]], [1, THEME["danger"]]],
        title="Các khung thời gian có nhu cầu tư vấn tâm lý cao nhất",
        labels={"total_messages": "Lượt chat", "high_risk_count": "Nguy cơ cao"},
        hover_data={
            "Chủ đề nổi bật": True,
            "total_messages": ":,",
            "high_risk_count": ":,",
            "negative_count": ":,",
            "rag_messages": ":,",
        },
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Lượt chat", yaxis_title="", coloraxis_colorbar_title="Nguy cơ cao")
    return board_chart_layout(fig, 420, left_margin=112, right_margin=64)


def make_chat_hour_volume_bar(hourly_timeline: pd.DataFrame):
    if hourly_timeline.empty or not {"hour_label", "total_messages"}.issubset(hourly_timeline.columns):
        return None
    plot = hourly_timeline.copy()
    plot["total_messages"] = pd.to_numeric(plot["total_messages"], errors="coerce").fillna(0)
    plot["high_risk_count"] = pd.to_numeric(plot.get("high_risk_count", 0), errors="coerce").fillna(0)
    plot = plot.sort_values("hour")
    fig = px.bar(
        plot,
        x="hour_label",
        y="total_messages",
        text="total_messages",
        color="high_risk_count",
        color_continuous_scale=[[0, THEME["primary_soft"]], [0.5, THEME["gold"]], [1, THEME["danger"]]],
        title="Khung giờ người dùng hỏi nhiều nhất",
        labels={"hour_label": "Giờ trong ngày", "total_messages": "Lượt chat", "high_risk_count": "Nguy cơ cao"},
        hover_data={"high_risk_count": ":,"},
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside", cliponaxis=False)
    fig.update_xaxes(type="category", categoryorder="array", categoryarray=plot["hour_label"].tolist())
    fig.update_layout(yaxis_title="Lượt chat", coloraxis_colorbar_title="Nguy cơ cao")
    fig.update_yaxes(rangemode="tozero")
    return board_chart_layout(fig, 370, right_margin=48, bottom_margin=72)


def chat_topic_total_by_label(topic_totals: pd.DataFrame, topic_label: str) -> int:
    if topic_totals.empty or not {"Topic", "Messages"}.issubset(topic_totals.columns):
        return 0
    work = topic_totals.copy()
    work["Chủ đề"] = work["Topic"].map(lambda value: chat_label(value, CHAT_TOPIC_LABELS))
    return int(pd.to_numeric(work.loc[work["Chủ đề"] == topic_label, "Messages"], errors="coerce").fillna(0).sum())


def make_chat_peak_profile_bar(timeline: pd.DataFrame, topic_totals: pd.DataFrame):
    if timeline.empty:
        return None
    total_messages = float(pd.to_numeric(timeline["total_messages"], errors="coerce").fillna(0).sum())
    if total_messages <= 0:
        return None
    peak = timeline.sort_values("total_messages", ascending=False).iloc[0]
    peak_messages = float(peak.get("total_messages", 0))
    if peak_messages <= 0:
        return None
    peak_topic, peak_topic_count = chat_peak_topic_from_row(peak)
    rows = [
        {
            "Yếu tố": f"Chủ đề chính",
            "Cao điểm": peak_topic_count / peak_messages * 100,
            "Toàn kỳ": chat_topic_total_by_label(topic_totals, peak_topic) / total_messages * 100,
            "Ghi chú": peak_topic,
        },
        {
            "Yếu tố": "Dùng tài liệu",
            "Cao điểm": float(peak.get("rag_messages", 0)) / peak_messages * 100,
            "Toàn kỳ": float(pd.to_numeric(timeline["rag_messages"], errors="coerce").fillna(0).sum()) / total_messages * 100,
            "Ghi chú": "Câu trả lời có dùng tài liệu tham chiếu",
        },
        {
            "Yếu tố": "Nguy cơ cao",
            "Cao điểm": float(peak.get("high_risk_count", 0)) / peak_messages * 100,
            "Toàn kỳ": float(pd.to_numeric(timeline["high_risk_count"], errors="coerce").fillna(0).sum()) / total_messages * 100,
            "Ghi chú": "Tin nhắn có mức nguy cơ cao",
        },
        {
            "Yếu tố": "Cảm xúc tiêu cực",
            "Cao điểm": float(peak.get("negative_count", 0)) / peak_messages * 100,
            "Toàn kỳ": float(pd.to_numeric(timeline["negative_count"], errors="coerce").fillna(0).sum()) / total_messages * 100,
            "Ghi chú": "Tin nhắn có cảm xúc tiêu cực",
        },
    ]
    plot = pd.DataFrame(rows).melt(
        id_vars=["Yếu tố", "Ghi chú"],
        value_vars=["Cao điểm", "Toàn kỳ"],
        var_name="Phạm vi",
        value_name="Tỷ lệ (%)",
    )
    plot["Tỷ lệ (%)"] = plot["Tỷ lệ (%)"].round(1)
    fig = px.bar(
        plot,
        x="Tỷ lệ (%)",
        y="Yếu tố",
        color="Phạm vi",
        barmode="group",
        orientation="h",
        text="Tỷ lệ (%)",
        title="Trong cao điểm, yếu tố nào tăng mạnh?",
        color_discrete_map={"Cao điểm": THEME["danger"], "Toàn kỳ": THEME["muted"]},
        hover_data={"Ghi chú": True, "Tỷ lệ (%)": ":.1f"},
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Tỷ lệ trong phạm vi (%)", yaxis_title="", legend_title_text="")
    fig.update_xaxes(range=[0, min(110, max(100, plot["Tỷ lệ (%)"].max() * 1.18))])
    return board_chart_layout(fig, 390, left_margin=118, right_margin=62)


def make_chat_risk_sentiment_summary(risk_totals: pd.DataFrame, sentiment_totals: pd.DataFrame):
    rows = []
    if not risk_totals.empty and {"Risk level", "Messages"}.issubset(risk_totals.columns):
        for _, row in risk_totals.iterrows():
            label = chat_label(row["Risk level"], CHAT_RISK_LABELS)
            rows.append(
                {
                    "Nhóm": "Nguy cơ",
                    "Mức": label,
                    "Lượt": int(row["Messages"]),
                    "Tỷ lệ (%)": float(row.get("Share %", 0.0)),
                }
            )
    if not sentiment_totals.empty and {"Sentiment", "Messages"}.issubset(sentiment_totals.columns):
        for _, row in sentiment_totals.iterrows():
            label = chat_label(row["Sentiment"], CHAT_SENTIMENT_LABELS)
            rows.append(
                {
                    "Nhóm": "Cảm xúc",
                    "Mức": label,
                    "Lượt": int(row["Messages"]),
                    "Tỷ lệ (%)": float(row.get("Share %", 0.0)),
                }
            )
    plot = pd.DataFrame(rows)
    if plot.empty:
        return None
    color_map = {**CHAT_RISK_COLORS, **CHAT_SENTIMENT_COLORS}
    fig = px.bar(
        plot,
        x="Mức",
        y="Lượt",
        color="Mức",
        facet_col="Nhóm",
        text="Tỷ lệ (%)",
        title="Nguy cơ và cảm xúc tổng hợp",
        color_discrete_map=color_map,
        hover_data={"Lượt": ":,", "Tỷ lệ (%)": ":.1f"},
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(showlegend=False, yaxis_title="Lượt chat")
    fig.for_each_annotation(lambda annotation: annotation.update(text=annotation.text.split("=")[-1]))
    return board_chart_layout(fig, 390, right_margin=48, bottom_margin=82)


def chat_sentiment_summary_values(sentiment_totals: pd.DataFrame, timeline: pd.DataFrame) -> Dict[str, object]:
    values: Dict[str, Dict[str, float]] = {
        "Tích cực": {"count": 0, "share": 0.0},
        "Trung tính": {"count": 0, "share": 0.0},
        "Tiêu cực": {"count": 0, "share": 0.0},
    }
    if not sentiment_totals.empty and {"Sentiment", "Messages"}.issubset(sentiment_totals.columns):
        for _, row in sentiment_totals.iterrows():
            label = chat_label(row["Sentiment"], CHAT_SENTIMENT_LABELS)
            if label in values:
                values[label] = {
                    "count": int(row["Messages"]),
                    "share": float(row.get("Share %", 0.0)),
                }
    dominant = max(values.items(), key=lambda item: item[1]["count"])[0] if values else "-"
    negative_peak = {"time": "-", "count": 0, "share": 0.0, "total": 0}
    if not timeline.empty and {"negative_count", "total_messages", "date_hour"}.issubset(timeline.columns):
        work = timeline.copy()
        work["negative_count"] = pd.to_numeric(work["negative_count"], errors="coerce").fillna(0)
        work["total_messages"] = pd.to_numeric(work["total_messages"], errors="coerce").fillna(0)
        if work["negative_count"].max() > 0:
            peak = work.sort_values(["negative_count", "total_messages"], ascending=False).iloc[0]
            total = float(peak.get("total_messages", 0))
            negative = int(peak.get("negative_count", 0))
            negative_peak = {
                "time": chat_time_label(peak.get("date_hour")),
                "count": negative,
                "share": round(negative / total * 100, 1) if total else 0.0,
                "total": int(total),
            }
    return {
        "values": values,
        "dominant": dominant,
        "negative_peak": negative_peak,
    }


def make_chat_negative_peak_bar(timeline: pd.DataFrame):
    if timeline.empty or not {"date_hour", "negative_count", "total_messages"}.issubset(timeline.columns):
        return None
    plot = timeline.copy()
    for column in ["negative_count", "neutral_count", "positive_count", "total_messages"]:
        if column not in plot.columns:
            plot[column] = 0
        plot[column] = pd.to_numeric(plot[column], errors="coerce").fillna(0)
    plot = plot[plot["negative_count"] > 0].copy()
    if plot.empty:
        return None
    plot["Khung thời gian"] = plot["date_hour"].apply(chat_time_label)
    plot["Tỷ lệ tiêu cực (%)"] = np.where(
        plot["total_messages"] > 0,
        (plot["negative_count"] / plot["total_messages"] * 100).round(1),
        0.0,
    )
    plot = plot.sort_values(["negative_count", "Tỷ lệ tiêu cực (%)"], ascending=False).head(8)
    plot = plot.sort_values("negative_count", ascending=True)
    fig = px.bar(
        plot,
        x="negative_count",
        y="Khung thời gian",
        orientation="h",
        text="Tỷ lệ tiêu cực (%)",
        color="Tỷ lệ tiêu cực (%)",
        color_continuous_scale=[[0, THEME["gold"]], [1, THEME["danger"]]],
        title="Khung thời gian có cảm xúc tiêu cực cao nhất",
        labels={"negative_count": "Lượt tiêu cực"},
        hover_data={
            "total_messages": ":,",
            "positive_count": ":,",
            "neutral_count": ":,",
            "negative_count": ":,",
            "Tỷ lệ tiêu cực (%)": ":.1f",
        },
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Lượt tiêu cực", yaxis_title="", coloraxis_colorbar_title="Tiêu cực (%)")
    return board_chart_layout(fig, 390, left_margin=112, right_margin=62)


def make_chat_peak_emotion_profile(timeline: pd.DataFrame):
    if timeline.empty or not {"date_hour", "total_messages", "negative_count"}.issubset(timeline.columns):
        return None
    work = timeline.copy()
    for column in ["positive_count", "neutral_count", "negative_count", "total_messages"]:
        if column not in work.columns:
            work[column] = 0
        work[column] = pd.to_numeric(work[column], errors="coerce").fillna(0)
    if work["negative_count"].max() <= 0:
        return None
    peak = work.sort_values(["negative_count", "total_messages"], ascending=False).iloc[0]
    peak_total = float(peak["total_messages"])
    overall_total = float(work["total_messages"].sum())
    rows = []
    labels = {
        "positive_count": "Tích cực",
        "neutral_count": "Trung tính",
        "negative_count": "Tiêu cực",
    }
    for column, label in labels.items():
        rows.append(
            {
                "Cảm xúc": label,
                "Cao điểm tiêu cực": float(peak[column]) / peak_total * 100 if peak_total else 0.0,
                "Toàn kỳ": float(work[column].sum()) / overall_total * 100 if overall_total else 0.0,
            }
        )
    plot = pd.DataFrame(rows).melt(
        id_vars="Cảm xúc",
        value_vars=["Cao điểm tiêu cực", "Toàn kỳ"],
        var_name="Phạm vi",
        value_name="Tỷ lệ (%)",
    )
    plot["Tỷ lệ (%)"] = plot["Tỷ lệ (%)"].round(1)
    fig = px.bar(
        plot,
        x="Tỷ lệ (%)",
        y="Cảm xúc",
        color="Phạm vi",
        barmode="group",
        orientation="h",
        text="Tỷ lệ (%)",
        title=f"Cơ cấu cảm xúc tại cao điểm {chat_time_label(peak['date_hour'])}",
        color_discrete_map={"Cao điểm tiêu cực": THEME["danger"], "Toàn kỳ": THEME["muted"]},
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Tỷ lệ (%)", yaxis_title="", legend_title_text="")
    fig.update_xaxes(range=[0, min(110, max(100, plot["Tỷ lệ (%)"].max() * 1.18))])
    return board_chart_layout(fig, 390, left_margin=95, right_margin=62)


def make_chat_selected_sentiment_trend(daily_timeline: pd.DataFrame, label: str):
    column_by_label = {
        "Tích cực": "positive_count",
        "Trung tính": "neutral_count",
        "Tiêu cực": "negative_count",
    }
    column = column_by_label.get(label)
    if daily_timeline.empty or column not in daily_timeline.columns or "date" not in daily_timeline.columns:
        return None
    plot = daily_timeline[["date", column, "total_messages"]].copy()
    plot[column] = pd.to_numeric(plot[column], errors="coerce").fillna(0)
    plot["total_messages"] = pd.to_numeric(plot["total_messages"], errors="coerce").fillna(0)
    plot["Tỷ lệ (%)"] = np.where(plot["total_messages"] > 0, (plot[column] / plot["total_messages"] * 100).round(1), 0.0)
    fig = px.bar(
        plot,
        x="date",
        y=column,
        text="Tỷ lệ (%)",
        title=f"{label} theo ngày",
        labels={column: f"Lượt {label.lower()}", "date": "Ngày"},
    )
    fig.update_traces(
        marker_color=CHAT_SENTIMENT_COLORS.get(label, THEME["primary"]),
        texttemplate="%{text:.1f}%",
        textposition="outside",
        cliponaxis=False,
    )
    fig.update_xaxes(tickformat="%d/%m")
    fig.update_yaxes(rangemode="tozero")
    fig.update_layout(yaxis_title=f"Lượt {label.lower()}", showlegend=False)
    return board_chart_layout(fig, 360, right_margin=48, bottom_margin=72)


def chat_construct_emotion_metrics(detail: pd.DataFrame, selected_construct: str) -> Dict[str, object]:
    selected = detail[detail["chat_construct"] == selected_construct].copy()
    total_all = float(detail["count"].sum()) if not detail.empty else 0.0
    total = int(selected["count"].sum()) if not selected.empty else 0
    negative = int(selected["negative_count"].sum()) if not selected.empty else 0
    peak_label = "-"
    peak_negative = 0
    if not selected.empty:
        peak = selected.sort_values(["negative_count", "count"], ascending=False).iloc[0]
        peak_label = chat_time_label(peak.get("date"), include_hour=False)
        peak_negative = int(peak.get("negative_count", 0))
    return {
        "total": total,
        "share": round(total / total_all * 100, 1) if total_all else 0.0,
        "negative": negative,
        "negative_rate": round(negative / total * 100, 1) if total else 0.0,
        "peak_label": peak_label,
        "peak_negative": peak_negative,
    }


def make_chat_construct_emotion_trend(detail: pd.DataFrame, selected_construct: str):
    selected = detail[detail["chat_construct"] == selected_construct].copy()
    if selected.empty:
        return None
    selected = selected.sort_values("date")
    plot = selected[["date", "count", "negative_count"]].melt(
        id_vars="date",
        value_vars=["count", "negative_count"],
        var_name="Chỉ số",
        value_name="Lượt",
    )
    plot["Chỉ số"] = plot["Chỉ số"].map({"count": "Tổng lượt", "negative_count": "Cảm xúc tiêu cực"})
    fig = px.line(
        plot,
        x="date",
        y="Lượt",
        color="Chỉ số",
        markers=True,
        title=f"Cảm xúc tiêu cực trong cụm: {selected_construct}",
        color_discrete_map={"Tổng lượt": THEME["primary"], "Cảm xúc tiêu cực": THEME["danger"]},
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=8))
    fig.update_xaxes(tickformat="%d/%m", title_text="Ngày")
    fig.update_yaxes(rangemode="tozero")
    fig.update_layout(hovermode="x unified", yaxis_title="Lượt")
    return board_chart_layout(fig, 370, right_margin=48, bottom_margin=72)


def make_chat_construct_negative_rate(detail: pd.DataFrame, selected_construct: str):
    selected = detail[detail["chat_construct"] == selected_construct].copy()
    if selected.empty:
        return None
    selected = selected.sort_values("date")
    selected["negative_rate"] = np.where(
        selected["count"] > 0,
        (selected["negative_count"] / selected["count"] * 100).round(1),
        0.0,
    )
    fig = px.bar(
        selected,
        x="date",
        y="negative_rate",
        text="negative_rate",
        title="Tỷ lệ tiêu cực theo ngày trong cụm",
        labels={"date": "Ngày", "negative_rate": "Tỷ lệ tiêu cực (%)"},
    )
    fig.update_traces(marker_color=THEME["danger"], texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_xaxes(tickformat="%d/%m")
    fig.update_yaxes(rangemode="tozero")
    fig.update_layout(showlegend=False, yaxis_title="Tỷ lệ tiêu cực (%)")
    return board_chart_layout(fig, 370, right_margin=48, bottom_margin=72)


def chat_construct_emotion_table(detail: pd.DataFrame, selected_construct: str) -> pd.DataFrame:
    selected = detail[detail["chat_construct"] == selected_construct].copy()
    if selected.empty:
        return pd.DataFrame()
    selected = selected.sort_values("date")
    table = selected[["date", "count", "negative_count", "negative_rate"]].copy()
    table["date"] = table["date"].dt.strftime("%d/%m/%Y")
    return table.rename(
        columns={
            "date": "Ngày",
            "count": "Tổng lượt",
            "negative_count": "Cảm xúc tiêu cực",
            "negative_rate": "Tỷ lệ tiêu cực (%)",
        }
    )


def chat_distribution_value(distribution: pd.DataFrame, label_column: str, label_value: str) -> Tuple[int, float]:
    if distribution.empty or label_column not in distribution.columns or "Messages" not in distribution.columns:
        return 0, 0.0
    mask = distribution[label_column].astype(str).str.lower().eq(label_value.lower())
    if not mask.any():
        return 0, 0.0
    row = distribution.loc[mask].iloc[0]
    return int(row["Messages"]), float(row.get("Share %", 0.0))


def chat_distribution_contains(distribution: pd.DataFrame, label_column: str, pattern: str) -> Tuple[int, float]:
    if distribution.empty or label_column not in distribution.columns or "Messages" not in distribution.columns:
        return 0, 0.0
    mask = distribution[label_column].astype(str).str.contains(pattern, case=False, regex=True, na=False)
    if not mask.any():
        return 0, 0.0
    messages = int(distribution.loc[mask, "Messages"].sum())
    total = float(distribution["Messages"].sum())
    return messages, round(messages / total * 100, 2) if total else 0.0


def fallback_construct_from_topic(topic_totals: pd.DataFrame) -> pd.DataFrame:
    if topic_totals.empty or "Topic" not in topic_totals.columns or "Messages" not in topic_totals.columns:
        return pd.DataFrame(columns=["Cụm nội dung", "Messages", "Share %"])
    mapping = {
        "harm_intent": "Nguy cơ an toàn cấp cao",
        "self_harm": "Nguy cơ an toàn cấp cao",
        "mental_health": "Tâm trạng, lo âu & trầm cảm",
        "rag_question": "Hỗ trợ học tập / tài liệu",
        "general": "Hỗ trợ chung / chưa rõ cụm",
    }
    result = topic_totals.copy()
    result["Cụm nội dung"] = result["Topic"].map(mapping).fillna("Hỗ trợ chung / chưa rõ cụm")
    result = result.groupby("Cụm nội dung", as_index=False)["Messages"].sum().sort_values("Messages", ascending=False)
    total = float(result["Messages"].sum())
    result["Share %"] = np.where(total > 0, (result["Messages"] / total * 100).round(2), 0.0)
    return result


def make_chat_construct_bar(construct_totals: pd.DataFrame):
    if construct_totals.empty or "Cụm nội dung" not in construct_totals.columns or "Messages" not in construct_totals.columns:
        return None
    plot = construct_totals.sort_values("Messages", ascending=True).copy()
    height = max(390, min(560, 86 + len(plot) * 46))
    fig = px.bar(
        plot,
        x="Messages",
        y="Cụm nội dung",
        orientation="h",
        text="Share %",
        color="Share %",
        color_continuous_scale=["#15b8aa", "#ffc928", "#ef5350"],
        hover_data={"Messages": True, "Share %": ":.2f"},
        title="Cụm nội dung xuất hiện nhiều nhất",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Lượt chat", yaxis_title="", coloraxis_showscale=False)
    return board_chart_layout(fig, height, left_margin=190, right_margin=54)


def make_chat_risk_distribution_bar(risk_totals: pd.DataFrame):
    if risk_totals.empty or "Risk level" not in risk_totals.columns or "Messages" not in risk_totals.columns:
        return None
    plot = risk_totals.copy()
    plot["Mức nguy cơ"] = plot["Risk level"].map(lambda value: chat_label(value, CHAT_RISK_LABELS))
    plot["Share %"] = pd.to_numeric(plot.get("Share %", 0), errors="coerce").fillna(0)
    plot["Messages"] = pd.to_numeric(plot["Messages"], errors="coerce").fillna(0)
    order = ["Thấp", "Trung bình", "Cao", "Không rõ"]
    plot["Mức nguy cơ"] = pd.Categorical(plot["Mức nguy cơ"], categories=order, ordered=True)
    plot = plot.sort_values("Mức nguy cơ")
    plot["Nhãn"] = plot.apply(lambda row: f"{int(row['Messages']):,} / {row['Share %']:.1f}%", axis=1)
    fig = px.bar(
        plot,
        x="Mức nguy cơ",
        y="Messages",
        text="Nhãn",
        color="Mức nguy cơ",
        color_discrete_map=CHAT_RISK_COLORS,
        title="Phân bố mức nguy cơ",
        hover_data={"Messages": ":,", "Share %": ":.1f", "Nhãn": False},
    )
    fig.update_traces(textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="", yaxis_title="Lượt chat", showlegend=False)
    return board_chart_layout(fig, 365, right_margin=48, bottom_margin=72)


def make_chat_high_risk_trend(frame: pd.DataFrame, x_column: str, title: str):
    if frame.empty or x_column not in frame.columns or "high_risk_count" not in frame.columns:
        return None
    plot = frame[[x_column, "high_risk_count", "total_messages"]].copy()
    plot["high_risk_count"] = pd.to_numeric(plot["high_risk_count"], errors="coerce").fillna(0)
    plot["total_messages"] = pd.to_numeric(plot["total_messages"], errors="coerce").fillna(0)
    plot["Tỷ lệ nguy cơ cao (%)"] = np.where(
        plot["total_messages"] > 0,
        (plot["high_risk_count"] / plot["total_messages"] * 100).round(1),
        0.0,
    )
    fig = px.line(
        plot,
        x=x_column,
        y="high_risk_count",
        markers=True,
        title=title,
        labels={x_column: "", "high_risk_count": "Tin nhắn nguy cơ cao"},
        hover_data={"Tỷ lệ nguy cơ cao (%)": ":.1f", "total_messages": ":,"},
    )
    fig.update_traces(line=dict(color=THEME["danger"], width=3), marker=dict(size=8))
    if x_column == "date":
        fig.update_xaxes(tickformat="%b %d", title_text="Ngày")
    fig.update_layout(hovermode="x unified", showlegend=False, yaxis_title="Tin nhắn nguy cơ cao")
    fig.update_yaxes(rangemode="tozero")
    return board_chart_layout(fig, 365, right_margin=48, bottom_margin=72)


def make_chat_high_risk_hour_bar(hourly: pd.DataFrame):
    if hourly.empty or not {"hour_label", "high_risk_count"}.issubset(hourly.columns):
        return None
    plot = hourly.copy()
    plot["high_risk_count"] = pd.to_numeric(plot["high_risk_count"], errors="coerce").fillna(0)
    plot = plot[plot["high_risk_count"] > 0]
    if plot.empty:
        return None
    fig = px.bar(
        plot,
        x="hour_label",
        y="high_risk_count",
        text="high_risk_count",
        title="Tin nhắn nguy cơ cao theo giờ",
        labels={"hour_label": "Giờ trong ngày", "high_risk_count": "Tin nhắn nguy cơ cao"},
    )
    fig.update_traces(marker_color=THEME["danger"], texttemplate="%{text:,}", textposition="outside", cliponaxis=False)
    fig.update_xaxes(type="category", categoryorder="array", categoryarray=plot["hour_label"].tolist())
    fig.update_layout(showlegend=False, yaxis_title="Tin nhắn nguy cơ cao")
    fig.update_yaxes(rangemode="tozero")
    return board_chart_layout(fig, 365, right_margin=48, bottom_margin=72)


def make_chat_top_risky_categories(summary: pd.DataFrame, category_column: str, title: str):
    if summary.empty or not {category_column, "high_risk_count"}.issubset(summary.columns):
        return None
    work = summary.copy()
    if "count" not in work.columns:
        work["count"] = work["high_risk_count"]
    for column in ["count", "high_risk_count"]:
        work[column] = pd.to_numeric(work[column], errors="coerce").fillna(0)
    plot = (
        work.groupby(category_column, as_index=False)[["count", "high_risk_count"]]
        .sum()
        .rename(columns={category_column: "Nhóm"})
    )
    if category_column == "topic":
        plot["Nhóm"] = plot["Nhóm"].map(lambda value: chat_label(value, CHAT_TOPIC_LABELS))
    plot = plot[plot["high_risk_count"] > 0].copy()
    if plot.empty:
        return None
    plot["Tỷ lệ nguy cơ cao (%)"] = np.where(
        plot["count"] > 0,
        (plot["high_risk_count"] / plot["count"] * 100).round(1),
        0.0,
    )
    plot = plot.sort_values(["high_risk_count", "Tỷ lệ nguy cơ cao (%)"], ascending=True).tail(8)
    fig = px.bar(
        plot,
        x="high_risk_count",
        y="Nhóm",
        orientation="h",
        text="Tỷ lệ nguy cơ cao (%)",
        color="Tỷ lệ nguy cơ cao (%)",
        color_continuous_scale=[[0, THEME["gold"]], [1, THEME["danger"]]],
        title=title,
        hover_data={"count": ":,", "high_risk_count": ":,", "Tỷ lệ nguy cơ cao (%)": ":.1f"},
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Tin nhắn nguy cơ cao", yaxis_title="", coloraxis_showscale=False)
    return board_chart_layout(fig, 365, left_margin=180, right_margin=54)


def make_chat_topic_distribution(topic_totals: pd.DataFrame):
    if topic_totals.empty or "Topic" not in topic_totals.columns or "Messages" not in topic_totals.columns:
        return None
    plot = topic_totals.sort_values("Messages", ascending=True).tail(10).copy()
    plot["Chủ đề"] = plot["Topic"].map(lambda value: chat_label(value, CHAT_TOPIC_LABELS))
    plot["Share %"] = pd.to_numeric(plot.get("Share %", 0), errors="coerce").fillna(0)
    fig = px.bar(
        plot,
        x="Messages",
        y="Chủ đề",
        orientation="h",
        text="Share %",
        color="Share %",
        color_continuous_scale=[[0, THEME["primary_soft"]], [0.6, THEME["teal"]], [1, THEME["primary"]]],
        title="Nhóm chủ đề tư vấn tâm lý được hỏi nhiều nhất",
        hover_data={"Messages": ":,", "Share %": ":.1f"},
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="Lượt chat", yaxis_title="", coloraxis_showscale=False)
    return board_chart_layout(fig, 390, left_margin=150, right_margin=54)


def make_chat_topic_trend(summary: pd.DataFrame):
    if summary.empty or not {"date", "topic", "count"}.issubset(summary.columns):
        return None
    plot = summary.copy()
    plot["date"] = pd.to_datetime(plot["date"], errors="coerce")
    plot["count"] = pd.to_numeric(plot["count"], errors="coerce").fillna(0)
    plot = plot.dropna(subset=["date", "topic"])
    if plot.empty:
        return None
    top_topics = plot.groupby("topic")["count"].sum().sort_values(ascending=False).head(3).index.tolist()
    plot = plot[plot["topic"].isin(top_topics)].sort_values(["date", "topic"])
    if plot.empty:
        return None
    plot["Chủ đề"] = plot["topic"].map(lambda value: chat_label(value, CHAT_TOPIC_LABELS))
    fig = px.line(
        plot,
        x="date",
        y="count",
        color="Chủ đề",
        markers=True,
        title="Ba nhóm chủ đề nổi bật theo ngày",
        labels={"date": "Ngày", "count": "Lượt chat", "Chủ đề": "Chủ đề"},
        color_discrete_sequence=PALETTE,
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=8))
    fig.update_xaxes(tickformat="%b %d")
    fig.update_layout(hovermode="x unified", yaxis_title="Lượt chat")
    fig.update_yaxes(rangemode="tozero")
    return board_chart_layout(fig, 390, right_margin=48, bottom_margin=72)


def make_chat_sentiment_distribution(sentiment_totals: pd.DataFrame):
    if sentiment_totals.empty or "Sentiment" not in sentiment_totals.columns or "Messages" not in sentiment_totals.columns:
        return None
    plot = sentiment_totals.copy()
    plot["Cảm xúc"] = plot["Sentiment"].map(lambda value: chat_label(value, CHAT_SENTIMENT_LABELS))
    plot["Share %"] = pd.to_numeric(plot.get("Share %", 0), errors="coerce").fillna(0)
    plot["Messages"] = pd.to_numeric(plot["Messages"], errors="coerce").fillna(0)
    order = ["Tích cực", "Trung tính", "Tiêu cực", "Không rõ"]
    plot["Cảm xúc"] = pd.Categorical(plot["Cảm xúc"], categories=order, ordered=True)
    plot = plot.sort_values("Cảm xúc")
    plot["Nhãn"] = plot.apply(lambda row: f"{int(row['Messages']):,} / {row['Share %']:.1f}%", axis=1)
    fig = px.bar(
        plot,
        x="Cảm xúc",
        y="Messages",
        text="Nhãn",
        color="Cảm xúc",
        color_discrete_map=CHAT_SENTIMENT_COLORS,
        title="Phân bố cảm xúc",
        hover_data={"Messages": ":,", "Share %": ":.1f", "Nhãn": False},
    )
    fig.update_traces(textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="", yaxis_title="Lượt chat", showlegend=False)
    return board_chart_layout(fig, 365, right_margin=48, bottom_margin=72)


def make_chat_donut(distribution: pd.DataFrame, label_column: str, title: str):
    if distribution.empty or label_column not in distribution.columns or "Messages" not in distribution.columns:
        return None
    fig = px.pie(
        distribution,
        names=label_column,
        values="Messages",
        hole=0.58,
        title=title,
        color_discrete_sequence=PALETTE,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return board_chart_layout(fig, 360, left_margin=20, right_margin=20, bottom_margin=20)


def make_chat_vertical_distribution(distribution: pd.DataFrame, label_column: str, title: str):
    if distribution.empty or label_column not in distribution.columns or "Messages" not in distribution.columns:
        return None
    plot = distribution.sort_values("Messages", ascending=False).copy()
    fig = px.bar(
        plot,
        x=label_column,
        y="Messages",
        text="Share %",
        color=label_column,
        color_discrete_sequence=PALETTE,
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(xaxis_title="", yaxis_title="Lượt chat", showlegend=False)
    return board_chart_layout(fig, 360, right_margin=48, bottom_margin=78)


def make_chat_date_stack(summary: pd.DataFrame, category_column: str, title: str):
    if summary.empty or not {"date", category_column, "count"}.issubset(summary.columns):
        return None
    plot = summary.copy()
    plot["date"] = pd.to_datetime(plot["date"], errors="coerce")
    plot = plot.dropna(subset=["date"])
    if plot.empty:
        return None
    fig = px.bar(
        plot,
        x="date",
        y="count",
        color=category_column,
        text="count",
        color_discrete_sequence=PALETTE,
        title=title,
    )
    fig.update_traces(textposition="inside")
    fig.update_layout(xaxis_title="", yaxis_title="Lượt chat", barmode="stack")
    return board_chart_layout(fig, 405, right_margin=42, bottom_margin=72)


def make_chat_construct_bubble(summary: pd.DataFrame):
    if summary.empty or not {"date", "chat_construct", "count"}.issubset(summary.columns):
        return None
    plot = summary.copy()
    plot["date"] = pd.to_datetime(plot["date"], errors="coerce")
    plot["count"] = pd.to_numeric(plot["count"], errors="coerce").fillna(0)
    plot = plot.dropna(subset=["date", "chat_construct"])
    plot = plot[plot["count"] > 0]
    if plot.empty:
        return None

    for column in ["percentage", "high_risk_rate", "negative_rate", "rag_rate", "unique_sessions"]:
        if column not in plot.columns:
            plot[column] = 0.0
        plot[column] = pd.to_numeric(plot[column], errors="coerce").fillna(0)

    order = plot.groupby("chat_construct")["count"].sum().sort_values(ascending=True).index.tolist()
    plot["chat_construct"] = pd.Categorical(plot["chat_construct"], categories=order, ordered=True)
    plot["label"] = plot["count"].astype(int).astype(str)
    height = max(405, min(560, 150 + len(order) * 42))
    fig = px.scatter(
        plot.sort_values(["chat_construct", "date"]),
        x="date",
        y="chat_construct",
        size="count",
        color="high_risk_rate",
        text="label",
        size_max=34,
        color_continuous_scale=["#15b8aa", "#ffc928", "#ef5350"],
        hover_data={
            "count": True,
            "percentage": ":.2f",
            "high_risk_rate": ":.2f",
            "negative_rate": ":.2f",
            "rag_rate": ":.2f",
            "unique_sessions": True,
            "label": False,
        },
        labels={
            "date": "Ngày",
            "chat_construct": "Cụm nội dung",
            "count": "Lượt chat",
            "percentage": "Tỷ trọng trong ngày (%)",
            "high_risk_rate": "Tỷ lệ nguy cơ cao (%)",
            "negative_rate": "Tỷ lệ tiêu cực (%)",
            "rag_rate": "Tỷ lệ dùng tài liệu (%)",
            "unique_sessions": "Phiên chat",
        },
        title="Cụm nội dung nổi lên theo ngày",
    )
    fig.update_traces(textposition="middle center", textfont_size=11, marker=dict(line=dict(width=1, color="#ffffff")))
    fig.update_xaxes(tickformat="%b %d<br>%Y")
    fig.update_layout(
        xaxis_title="Ngày",
        yaxis_title="",
        coloraxis_colorbar_title="Nguy cơ cao (%)",
    )
    return board_chart_layout(fig, height, left_margin=190, right_margin=56, bottom_margin=72)


def make_chat_hour_heatmap(timeline: pd.DataFrame):
    if timeline.empty:
        return None
    plot = timeline.copy()
    plot["date"] = plot["date_hour"].dt.strftime("%Y-%m-%d")
    plot["hour_label"] = plot["date_hour"].dt.hour.astype(str).str.zfill(2) + ":00"
    pivot = plot.pivot_table(index="date", columns="hour_label", values="total_messages", aggfunc="sum", fill_value=0)
    if pivot.empty:
        return None
    text = pivot.astype(int).astype(str).mask(pivot.eq(0), "")
    fig = px.imshow(
        pivot,
        text_auto=False,
        aspect="auto",
        color_continuous_scale=[[0, "#f7fbfd"], [0.35, THEME["teal_soft"]], [0.7, THEME["teal"]], [1, THEME["danger"]]],
        title="Thời điểm phát sinh hội thoại",
        labels={"x": "Giờ", "y": "Ngày", "color": "Lượt chat"},
    )
    fig.update_traces(text=text.values, texttemplate="%{text}", hovertemplate="Ngày=%{y}<br>Giờ=%{x}<br>Lượt chat=%{z}<extra></extra>")
    return board_chart_layout(fig, 405, left_margin=72, right_margin=38, bottom_margin=62)


def inject_chat_dashboard_css() -> None:
    st.markdown(
        f"""
        <style>
        .chat-kpi-card {{
            background: #ffffff;
            border: 1px solid {THEME["border"]};
            border-left: 5px solid {THEME["primary"]};
            border-radius: 8px;
            padding: 0.92rem 0.96rem;
            min-height: 116px;
            margin-bottom: 1rem;
            box-shadow: 0 10px 24px rgba(27, 39, 51, 0.06);
        }}
        .chat-kpi-card.warning {{
            border-left-color: {THEME["danger"]};
            background: linear-gradient(180deg, #ffffff 0%, {THEME["coral_soft"]} 145%);
        }}
        .chat-kpi-card.neutral {{
            border-left-color: {THEME["muted"]};
        }}
        .chat-kpi-label {{
            color: {THEME["muted"]};
            font-size: 0.8rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0;
            margin-bottom: 0.38rem;
        }}
        .chat-kpi-value {{
            color: {THEME["text"]};
            font-size: 1.42rem;
            font-weight: 800;
            line-height: 1.16;
            margin-bottom: 0.36rem;
        }}
        .chat-kpi-caption {{
            color: {THEME["muted"]};
            font-size: 0.82rem;
            line-height: 1.28;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_chat_kpi_card(slot, label: str, value: str, caption: str = "", tone: str = "default") -> None:
    class_name = "chat-kpi-card"
    if tone in {"warning", "neutral"}:
        class_name += f" {tone}"
    slot.markdown(
        f"""
        <div class="{class_name}">
            <div class="chat-kpi-label">{html.escape(label)}</div>
            <div class="chat-kpi-value">{html.escape(value)}</div>
            <div class="chat-kpi-caption">{html.escape(caption)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def chat_audience_group_for_page(selected_page: str) -> str | None:
    if selected_page == "Học sinh":
        return "school"
    if selected_page == "Sinh viên":
        return "university"
    return None


def render_chatbot_gold_section(selected_page: str = "Tổng quan") -> None:
    from dashboard_gcs_loader import filter_chat_gold_tables_by_audience, get_dashboard_kpis

    inject_chat_dashboard_css()

    try:
        loaded_gold_tables = load_cached_chatbot_gold_tables()
    except Exception as exc:
        st.error(f"Không đọc được dữ liệu hội thoại đã xử lý: {exc}")
        return

    audience_group = chat_audience_group_for_page(selected_page)
    gold_tables = filter_chat_gold_tables_by_audience(loaded_gold_tables, audience_group)
    metrics = gold_tables.get("chat_hourly_metrics", pd.DataFrame())
    if metrics.empty:
        if audience_group:
            st.warning("Gold hội thoại chưa có đủ metadata độ tuổi/nhóm cho trang này.")
        else:
            st.warning("Chưa có số liệu hội thoại đã xử lý để hiển thị.")
        return

    kpis = get_dashboard_kpis(metrics)
    timeline = prepare_chat_timeline(metrics)
    daily_timeline = aggregate_chat_timeframe(timeline, "day")
    sentiment_totals = aggregate_chat_distribution(
        gold_tables.get("chat_sentiment_summary", pd.DataFrame()),
        "sentiment",
        "Sentiment",
    )
    if sentiment_totals.empty:
        sentiment_totals = pd.DataFrame(
            {
                "Sentiment": ["positive", "neutral", "negative"],
                "Messages": [
                    metrics["positive_count"].sum(),
                    metrics["neutral_count"].sum(),
                    metrics["negative_count"].sum(),
                ],
            }
        )
        total_sentiment = float(sentiment_totals["Messages"].sum())
        sentiment_totals["Share %"] = np.where(
            total_sentiment > 0,
            (sentiment_totals["Messages"] / total_sentiment * 100).round(2),
            0.0,
        )
    sentiment_info = chat_sentiment_summary_values(sentiment_totals, timeline)
    sentiment_values = sentiment_info["values"]
    negative_peak = sentiment_info["negative_peak"]
    negative_count, negative_rate = chat_distribution_value(sentiment_totals, "Sentiment", "negative")
    construct_detail = prepare_chat_construct_detail(gold_tables.get("chat_construct_summary", pd.DataFrame()))

    scope_title = "toàn bộ" if audience_group is None else selected_page.lower()
    board_section(f"Tổng quan cảm xúc trong hội thoại tư vấn tâm lý - {scope_title}")
    kpi_cols = st.columns(5, gap="small")
    render_chat_kpi_card(kpi_cols[0], "Tổng lượt", f"{kpis['total_messages']:,}", "Hội thoại đã xử lý")
    render_chat_kpi_card(kpi_cols[1], "Phiên", f"{kpis['unique_sessions']:,}", "Mã phiên ẩn danh")
    render_chat_kpi_card(kpi_cols[2], "Cảm xúc chủ đạo", str(sentiment_info["dominant"]), "Nhóm cảm xúc nhiều nhất")
    render_chat_kpi_card(
        kpi_cols[3],
        "Tiêu cực",
        f"{chat_pct(negative_rate)} / {negative_count:,} lượt",
        "Nhóm cần chú ý",
        tone="warning",
    )
    render_chat_kpi_card(
        kpi_cols[4],
        "Cao điểm tiêu cực",
        f"{negative_peak['time']} / {negative_peak['count']:,} lượt",
        f"{negative_peak['share']:.1f}% trong khung đó",
        tone="warning",
    )

    board_section("Cao điểm cảm xúc tiêu cực")
    peak_row = st.columns(2, gap="small")
    with peak_row[0]:
        peak_profile = make_chat_peak_emotion_profile(timeline)
        if peak_profile is not None:
            st.plotly_chart(peak_profile, width="stretch", config=PLOT_CONFIG, key="chat_peak_emotion_profile")
    with peak_row[1]:
        negative_peak_fig = make_chat_negative_peak_bar(timeline)
        if negative_peak_fig is not None:
            st.plotly_chart(negative_peak_fig, width="stretch", config=PLOT_CONFIG, key="chat_negative_peak_bar")

    board_section("Diễn biến cảm xúc")
    trend_row = st.columns(2, gap="small")
    with trend_row[0]:
        sentiment_fig = make_chat_sentiment_distribution(sentiment_totals)
        if sentiment_fig is not None:
            st.plotly_chart(sentiment_fig, width="stretch", config=PLOT_CONFIG, key="chat_sentiment_distribution")
    with trend_row[1]:
        sentiment_line = make_chat_sentiment_line(daily_timeline, "date", "Cảm xúc theo ngày")
        if sentiment_line is not None:
            st.plotly_chart(sentiment_line, width="stretch", config=PLOT_CONFIG, key="chat_sentiment_daily_line")

    board_section("Xem riêng từng nhóm cảm xúc")
    selected_sentiment = st.selectbox(
        "Chọn cảm xúc",
        ["Tiêu cực", "Trung tính", "Tích cực"],
        key="chat_sentiment_detail_selector",
    )
    selected_values = sentiment_values.get(selected_sentiment, {"count": 0, "share": 0.0})
    detail_cols = st.columns(3, gap="small")
    render_chat_kpi_card(detail_cols[0], "Số lượt", f"{int(selected_values['count']):,}", f"Chiếm {selected_values['share']:.1f}%")
    selected_trend = make_chat_selected_sentiment_trend(daily_timeline, selected_sentiment)
    if selected_trend is not None:
        selected_daily = daily_timeline.copy()
        column = {"Tích cực": "positive_count", "Trung tính": "neutral_count", "Tiêu cực": "negative_count"}[selected_sentiment]
        peak_day = selected_daily.sort_values(column, ascending=False).iloc[0]
        render_chat_kpi_card(detail_cols[1], "Ngày cao nhất", chat_time_label(peak_day["date"], include_hour=False), f"{int(peak_day[column]):,} lượt")
        render_chat_kpi_card(detail_cols[2], "Tổng hội thoại ngày đó", f"{int(peak_day['total_messages']):,}", "Để so sánh quy mô")
        st.plotly_chart(selected_trend, width="stretch", config=PLOT_CONFIG, key="chat_selected_sentiment_trend")

    board_section("Lọc cảm xúc theo cụm log")
    construct_options = chat_construct_options(construct_detail)
    if construct_options:
        selected_construct = st.selectbox(
            "Chọn cụm log",
            construct_options,
            key="chat_emotion_construct_selector",
        )
        construct_metrics = chat_construct_emotion_metrics(construct_detail, selected_construct)
        construct_cols = st.columns(4, gap="small")
        render_chat_kpi_card(
            construct_cols[0],
            "Tổng lượt trong cụm",
            f"{construct_metrics['total']:,}",
            f"Chiếm {construct_metrics['share']:.1f}% tổng hội thoại",
        )
        render_chat_kpi_card(
            construct_cols[1],
            "Tiêu cực trong cụm",
            f"{construct_metrics['negative_rate']:.1f}% / {construct_metrics['negative']:,} lượt",
            "Tỷ lệ cảm xúc tiêu cực",
            tone="warning" if int(construct_metrics["negative"]) > 0 else "neutral",
        )
        render_chat_kpi_card(
            construct_cols[2],
            "Ngày tiêu cực cao nhất",
            f"{construct_metrics['peak_label']}",
            f"{construct_metrics['peak_negative']:,} lượt tiêu cực",
            tone="warning" if int(construct_metrics["peak_negative"]) > 0 else "neutral",
        )
        render_chat_kpi_card(
            construct_cols[3],
            "Cụm đang xem",
            selected_construct,
            "Lọc từ log hội thoại đã xử lý",
        )
        construct_row = st.columns(2, gap="small")
        with construct_row[0]:
            construct_trend = make_chat_construct_emotion_trend(construct_detail, selected_construct)
            if construct_trend is not None:
                st.plotly_chart(construct_trend, width="stretch", config=PLOT_CONFIG, key="chat_construct_emotion_trend")
        with construct_row[1]:
            construct_rate = make_chat_construct_negative_rate(construct_detail, selected_construct)
            if construct_rate is not None:
                st.plotly_chart(construct_rate, width="stretch", config=PLOT_CONFIG, key="chat_construct_negative_rate")
        with st.expander("Bảng theo ngày của cụm đang chọn", expanded=False):
            st.dataframe(
                chat_construct_emotion_table(construct_detail, selected_construct),
                width="stretch",
                hide_index=True,
                height=220,
            )
    else:
        chart_note("Chưa có số liệu cụm log để lọc cảm xúc.")



FOCUSED_SCOPE_OPTIONS = {
    "Học sinh trung học": MENTAL_SCHOOL_POPULATION_LABEL,
    "Sinh viên đại học/cao đẳng": HMS_POPULATION_LABEL,
}
MIN_COMPARISON_GROUP_N = 100


def focused_scope_data(
    filtered_raw: pd.DataFrame,
    filtered_cleaned: pd.DataFrame,
    scope_label: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    population = FOCUSED_SCOPE_OPTIONS[scope_label]
    if POPULATION_COLUMN not in filtered_raw.columns:
        return filtered_raw.copy(), filtered_cleaned.copy()
    mask = filtered_raw[POPULATION_COLUMN] == population
    scope_raw = filtered_raw.loc[mask].copy()
    return scope_raw, filtered_cleaned.loc[scope_raw.index].copy()


def focused_rate_by_column(df: pd.DataFrame, column: str, label: str) -> pd.DataFrame:
    if df.empty or column not in df.columns or "Target" not in df.columns:
        return pd.DataFrame(columns=[label, "n", "At Risk Rate"])
    result = (
        df.dropna(subset=[column])
        .groupby(column, as_index=False)["Target"]
        .agg(n="size", rate="mean")
        .rename(columns={column: label})
    )
    result["At Risk Rate"] = (result["rate"] * 100).round(2)
    return result.drop(columns=["rate"]).sort_values("At Risk Rate", ascending=False).reset_index(drop=True)


def focused_demographic_rate(df: pd.DataFrame, qnum: int, label: str) -> pd.DataFrame:
    result = target_prevalence_by_group(df, qnum, label)
    if result.empty:
        return result
    return result[result["n"] >= MIN_COMPARISON_GROUP_N].copy()


def make_focused_rate_chart(
    chart_df: pd.DataFrame,
    category: str,
    title: str,
    height: int = 420,
):
    if chart_df.empty:
        return None
    plot_df = chart_df.sort_values("At Risk Rate", ascending=True).copy()
    fig = px.bar(
        plot_df,
        x="At Risk Rate",
        y=category,
        orientation="h",
        text="At Risk Rate",
        color="At Risk Rate",
        color_continuous_scale=[[0, THEME["primary_soft"]], [0.6, THEME["gold"]], [1, THEME["danger"]]],
        hover_data={"n": True, "At Risk Rate": ":.2f"},
        title=title,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(
        xaxis_title="Tỷ lệ At Risk (%)",
        yaxis_title="",
        coloraxis_showscale=False,
        margin=dict(l=140, r=56, t=70, b=48),
    )
    return style_figure(fig, height)


def focused_construct_impact(scope_cleaned: pd.DataFrame) -> pd.DataFrame:
    score_cols = [
        feature
        for feature in RESEARCH_FEATURES
        if feature in scope_cleaned.columns and scope_cleaned[feature].notna().any()
    ]
    impact = construct_impact_table(scope_cleaned, score_cols)
    if impact.empty:
        return impact
    impact["Khía cạnh"] = impact["Construct"].map(construct_label)
    return impact


def make_focused_construct_chart(impact: pd.DataFrame, scope_label: str, height: int = 500):
    if impact.empty:
        return None
    value_col = "Thay đổi khi construct tăng (%)"
    chart_df = impact.sort_values(value_col, ascending=True).copy()
    fig = px.bar(
        chart_df,
        x=value_col,
        y="Khía cạnh",
        orientation="h",
        text=value_col,
        color=value_col,
        color_continuous_scale=[[0, THEME["teal"]], [0.5, THEME["gold"]], [1, THEME["danger"]]],
        hover_data={
            "At Risk ở mức thấp (%)": ":.2f",
            "At Risk ở mức cao (%)": ":.2f",
        },
        title=f"{scope_label}: chênh lệch At Risk giữa mức construct cao và thấp",
    )
    fig.update_traces(texttemplate="%{text:+.1f} điểm %", textposition="outside", cliponaxis=False)
    fig.update_layout(
        xaxis_title="Chênh lệch tỷ lệ At Risk (điểm %)",
        yaxis_title="",
        coloraxis_showscale=False,
        margin=dict(l=210, r=75, t=70, b=48),
    )
    return style_figure(fig, height)


def focused_question_driver_table(scope_raw: pd.DataFrame, processed) -> pd.DataFrame:
    if scope_raw.empty:
        return pd.DataFrame()
    drivers = top_target_gap_questions(
        processed,
        top_n=10,
        min_category_n=40,
        df=scope_raw,
    )
    if drivers.empty:
        return drivers
    drivers["Yếu tố quan sát"] = drivers.apply(
        lambda row: question_label(row["qnum"], row["question"]),
        axis=1,
    )
    return drivers


def make_focused_driver_chart(drivers: pd.DataFrame, height: int = 540):
    if drivers.empty:
        return None
    plot_df = drivers.sort_values("At Risk Gap (%)", ascending=True).copy()
    fig = px.bar(
        plot_df,
        x="At Risk Gap (%)",
        y="Yếu tố quan sát",
        orientation="h",
        text="At Risk Gap (%)",
        color="At Risk Gap (%)",
        color_continuous_scale=[[0, THEME["primary"]], [1, THEME["danger"]]],
        hover_data={
            "Highest-risk response": True,
            "Highest-risk rate (%)": ":.2f",
            "Lowest-risk response": True,
            "Lowest-risk rate (%)": ":.2f",
        },
        title="Học sinh: yếu tố có khoảng cách At Risk lớn nhất giữa các phản hồi",
    )
    fig.update_traces(texttemplate="%{text:.1f} điểm %", textposition="outside", cliponaxis=False)
    fig.update_layout(
        xaxis_title="Khoảng cách tỷ lệ At Risk (điểm %)",
        yaxis_title="",
        coloraxis_showscale=False,
        margin=dict(l=230, r=70, t=70, b=48),
    )
    return style_figure(fig, height)


def render_focused_header(
    raw_df: pd.DataFrame,
    filtered_raw: pd.DataFrame,
) -> None:
    st.title("Student Mental Health Risk Dashboard")
    total = int(filtered_raw.shape[0])
    at_risk_rate = float(filtered_raw["Target"].mean() * 100) if total else 0.0
    student_count = (
        int((filtered_raw[POPULATION_COLUMN] == MENTAL_SCHOOL_POPULATION_LABEL).sum())
        if POPULATION_COLUMN in filtered_raw.columns
        else 0
    )
    college_count = (
        int((filtered_raw[POPULATION_COLUMN] == HMS_POPULATION_LABEL).sum())
        if POPULATION_COLUMN in filtered_raw.columns
        else 0
    )
    kpis = st.columns(4, gap="medium")
    kpis[0].metric("Bản ghi phân tích", f"{total:,}")
    kpis[1].metric("At Risk", f"{at_risk_rate:.2f}%")
    kpis[2].metric("Học sinh", f"{student_count:,}")
    kpis[3].metric("Sinh viên", f"{college_count:,}")


def render_focused_outcome_tab(filtered_raw: pd.DataFrame) -> None:
    st.subheader("Mục tiêu: tỷ lệ dấu hiệu nguy cơ sức khỏe tinh thần")
    if filtered_raw.empty:
        return
    charts = st.columns(2, gap="large")
    by_source = focused_rate_by_column(filtered_raw, DATA_SOURCE_COLUMN, "Nguồn dữ liệu")
    by_population = focused_rate_by_column(filtered_raw, POPULATION_COLUMN, "Nhóm")
    with charts[0]:
        fig = make_focused_rate_chart(by_source, "Nguồn dữ liệu", "At Risk theo bộ dữ liệu/năm khảo sát")
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key="focused_source_rate")
    with charts[1]:
        fig = make_focused_rate_chart(by_population, "Nhóm", "At Risk theo nhóm học sinh và sinh viên")
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key="focused_population_rate")

    target_counts = filtered_raw["Target"].map(risk_label).value_counts().rename_axis("Kết quả").reset_index(name="Số bản ghi")
    target_fig = px.bar(
        target_counts,
        x="Kết quả",
        y="Số bản ghi",
        text="Số bản ghi",
        color="Kết quả",
        color_discrete_sequence=[THEME["primary"], THEME["danger"]],
        title="Quy mô hai nhóm kết quả đang phân tích",
    )
    target_fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    st.plotly_chart(style_figure(target_fig, 380), width="stretch", config=PLOT_CONFIG, key="focused_target_count")


def render_focused_construct_tab(
    scope_raw: pd.DataFrame,
    scope_cleaned: pd.DataFrame,
    scope_label: str,
    processed,
) -> None:
    st.subheader(f"Construct liên quan tới At Risk - {scope_label}")
    impact = focused_construct_impact(scope_cleaned)
    fig = make_focused_construct_chart(impact, scope_label)
    if fig is not None:
        st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key=f"focused_construct_gap_{scope_label}")

    if not impact.empty:
        selected = st.selectbox(
            "Construct",
            impact["Construct"].tolist(),
            format_func=construct_label,
            key=f"focused_construct_select_{scope_label}",
        )
        trend = construct_trend_table(scope_cleaned, selected, bins=4)
        trend_fig = make_construct_trend_chart(trend, selected, height=420)
        if trend_fig is not None:
            st.plotly_chart(trend_fig, width="stretch", config=PLOT_CONFIG, key=f"focused_construct_trend_{scope_label}")

    if scope_label == "Học sinh trung học":
        drivers = focused_question_driver_table(scope_raw, processed)
        driver_fig = make_focused_driver_chart(drivers)
        if driver_fig is not None:
            st.plotly_chart(driver_fig, width="stretch", config=PLOT_CONFIG, key="focused_school_driver_gap")


def render_focused_demographic_tab(
    scope_raw: pd.DataFrame,
    scope_cleaned: pd.DataFrame,
    scope_label: str,
) -> None:
    st.subheader(f"Tỷ lệ At Risk theo độ tuổi và lớp - {scope_label}")
    age = focused_demographic_rate(scope_raw, 1, "Age")
    grade = focused_demographic_rate(scope_raw, 3, "Grade")
    row = st.columns(2, gap="large")
    with row[0]:
        fig = make_focused_rate_chart(age, "Age", f"Độ tuổi (n >= {MIN_COMPARISON_GROUP_N})")
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key=f"focused_age_{scope_label}")
    with row[1]:
        fig = make_focused_rate_chart(grade, "Grade", f"Bậc/lớp (n >= {MIN_COMPARISON_GROUP_N})")
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key=f"focused_grade_{scope_label}")

    score_cols = [
        feature
        for feature in RESEARCH_FEATURES
        if feature in scope_cleaned.columns and scope_cleaned[feature].notna().any()
    ]
    group_mode = st.radio(
        "Heatmap construct theo",
        ["Độ tuổi", "Bậc/lớp"],
        horizontal=True,
        key=f"focused_heatmap_mode_{scope_label}",
    )
    qnum, group_name = (1, "Age") if group_mode == "Độ tuổi" else (3, "Grade")
    table = demographic_construct_table(scope_raw, scope_cleaned, qnum, group_name, score_cols)
    group_counts = focused_demographic_rate(scope_raw, qnum, group_name)[[group_name]]
    table = table.merge(group_counts, on=group_name, how="inner") if not table.empty else table
    heatmap = make_demographic_construct_heatmap(
        table,
        group_name,
        "Mean score",
        f"Điểm construct trung bình theo {group_mode.lower()} - {scope_label}",
        height=510,
    )
    if heatmap is not None:
        st.plotly_chart(heatmap, width="stretch", config=PLOT_CONFIG, key=f"focused_construct_heatmap_{scope_label}_{group_mode}")


def render_focused_model_tab(
    scope_raw: pd.DataFrame,
    scope_cleaned: pd.DataFrame,
    scope_label: str,
    processed,
) -> None:
    st.subheader("Mô hình đo lường: Outcome At Risk và xếp hạng yếu tố liên quan")
    target_chart = make_target_donut(scope_raw)
    impact = focused_construct_impact(scope_cleaned)
    model_row = st.columns([0.8, 1.2], gap="large")
    with model_row[0]:
        if target_chart is not None:
            st.plotly_chart(target_chart, width="stretch", config=PLOT_CONFIG, key=f"focused_model_target_{scope_label}")
    with model_row[1]:
        fig = make_focused_construct_chart(impact, scope_label, height=430)
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key=f"focused_model_importance_{scope_label}")

    if scope_label == "Học sinh trung học":
        drivers = focused_question_driver_table(scope_raw, processed)
        fig = make_focused_driver_chart(drivers, height=540)
        if fig is not None:
            st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, key="focused_model_source_importance")


BOARD_GROUP_LABELS = {
    MENTAL_SCHOOL_POPULATION_LABEL: "Học sinh",
    HMS_POPULATION_LABEL: "Sinh viên",
}

BOARD_FACTOR_LABELS = {
    "Family Pressure Index": "Gia đình thiếu an toàn / hỗ trợ",
    "Academic Pressure Index": "Áp lực học tập",
    "Peer & Safety Stress Index": "Mất an toàn / bắt nạt (quy đổi chung)",
    "Trauma Exposure Index": "Bạo lực tình dục / quan hệ (quy đổi chung)",
    "Substance Coping Risk Index": "Đối phó bằng chất",
    "Lifestyle Recovery Deficit": "Thiếu ngủ / phục hồi",
}

BOARD_SOURCE_CLUSTER_LABELS = {
    "Demographics": "Nhân khẩu học",
    "Housing & Basic Needs": "Nơi ở & nhu cầu cơ bản",
    "Family Support & Monitoring": "Gia đình & giám sát",
    "Functional Difficulty": "Khó khăn chức năng",
    "School Climate & Academic Context": "Môi trường học & học tập",
    "Lifestyle Factors": "Lối sống",
    "Safety & Driving": "An toàn giao thông",
    "Violence & Safety": "Bạo lực & an toàn",
    "Sexual Violence": "Bạo lực tình dục",
    "Substance Use": "Sử dụng chất",
    "Sexual Behavior": "Hành vi tình dục",
    "Sexual Identity & Contacts": "Bản dạng & quan hệ",
    "Family & ACEs": "Bất lợi trong gia đình",
    "Mental Health Indicators": "Sức khỏe tinh thần",
    "Other Health Risks": "Rủi ro sức khỏe khác",
    "Digital Behavior": "Hành vi số",
    "Health Care & Preventive Services": "Chăm sóc & phòng ngừa",
}

BOARD_SCHOOL_DRIVER_GROUPS = {
    "Sức khỏe tinh thần & tự hại": [26, 27, 28, 29, 30, 84],
    "An toàn giao thông & vũ khí": [8, 9, 10, 11, 12, 13],
    "Bạo lực & an toàn học đường": list(range(12, 19)),
    "Bắt nạt & phân biệt đối xử": [23, 24, 25],
    "Bạo lực tình dục & hẹn hò": [19, 20, 21, 22, 88],
    "Thuốc lá & vape": list(range(31, 41)),
    "Rượu & ma túy": list(range(41, 56)) + [92, 93],
    "Hành vi tình dục & sức khỏe sinh sản": list(range(56, 66)) + [94],
    "Ăn uống, giấc ngủ & vận động": list(range(68, 81)) + [85, 95, 96, 97],
    "Chăm sóc sức khỏe & nhà ở": [81, 82, 83, 86, 98, 99],
    "Trải nghiệm gia đình & cá nhân": [89, 90, 91, 99, 100, 101, 102, 103, 104, 105, 106, 107],
}
BOARD_SCHOOL_QNUM_TO_GROUP = {
    qnum: group
    for group, qnums in BOARD_SCHOOL_DRIVER_GROUPS.items()
    for qnum in qnums
}

BOARD_COLLEGE_NATIVE_LABELS = {
    name: definition["label"]
    for name, definition in HMS_NATIVE_FEATURE_DEFINITIONS.items()
}

BOARD_GRADE_LABELS = {
    "9th": "Lớp 9",
    "10th": "Lớp 10",
    "11th": "Lớp 11",
    "12th": "Lớp 12",
    "College 1st year": "Năm 1",
    "College 2nd year": "Năm 2",
    "College 3rd year": "Năm 3",
    "College 4th+ year": "Năm 4+",
    "Graduate/Professional": "Sau ĐH",
    "Other college": "Khác",
    "Ungraded/Other": "Khác",
}
BOARD_SCHOOL_GRADE_LABELS = {"Lớp 9", "Lớp 10", "Lớp 11", "Lớp 12"}
BOARD_COLLEGE_UNDERGRAD_AGE_LABELS = {"18+", "19-20", "21-24"}


def board_filter_q_labels(df: pd.DataFrame, qnum: int, allowed_labels: set[str]) -> pd.DataFrame:
    col = find_q_col(df, qnum)
    if df.empty or col is None:
        return df.copy()
    labels = df[col].apply(lambda value: value_to_label(value, qnum))
    if qnum == 3:
        labels = labels.map(BOARD_GRADE_LABELS).fillna(labels)
    return df.loc[labels.isin(allowed_labels)].copy()


def board_filter_table_labels(df: pd.DataFrame, column: str, allowed_labels: set[str]) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df
    return df[df[column].isin(allowed_labels)].copy()


def board_college_undergraduate_years_only(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only undergraduate years 1-4+ in college year-level comparisons."""
    grade_col = find_q_col(df, 3)
    if df.empty or grade_col is None or POPULATION_COLUMN not in df.columns:
        return df
    labels = df[grade_col].apply(lambda value: value_to_label(value, 3))
    labels = labels.map(BOARD_GRADE_LABELS).fillna(labels)
    undergraduate_years = {"Năm 1", "Năm 2", "Năm 3", "Năm 4+"}
    college_row = df[POPULATION_COLUMN] == HMS_POPULATION_LABEL
    include = ~college_row | labels.isin(undergraduate_years)
    return df.loc[include].copy()


def board_college_undergraduate_ages_only(df: pd.DataFrame) -> pd.DataFrame:
    """Keep college rows in undergraduate age bands for overview age comparisons."""
    age_col = find_q_col(df, 1)
    if df.empty or age_col is None or POPULATION_COLUMN not in df.columns:
        return df
    labels = df[age_col].apply(lambda value: value_to_label(value, 1))
    college_row = df[POPULATION_COLUMN] == HMS_POPULATION_LABEL
    include = ~college_row | labels.isin(BOARD_COLLEGE_UNDERGRAD_AGE_LABELS)
    return df.loc[include].copy()


def inject_board_css() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSegmentedControl"] {
            margin: 0 0 0.75rem;
        }
        [data-testid="stSegmentedControl"] button {
            min-height: 2.8rem;
            min-width: 10.5rem;
            border: 1px solid #d9e2ec;
            border-radius: 8px !important;
            background: #ffffff;
            color: #344453;
            font-weight: 700;
        }
        [data-testid="stSegmentedControl"] button[aria-pressed="true"] {
            border-color: #2f9dd8;
            background: #2f9dd8;
            color: #ffffff;
        }
        .board-head {
            background: #ffffff;
            border: 1px solid #e1e6ec;
            border-radius: 8px;
            padding: 1.0rem 1.25rem 0.85rem;
            margin: 0 0 0.7rem;
        }
        .board-head h1 {
            color: #23323a;
            font-size: 1.55rem;
            font-weight: 760;
            letter-spacing: 0.01em;
            margin: 0;
        }
        .board-head p {
            color: #64727d;
            font-size: 0.86rem;
            margin: 0.28rem 0 0;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e1e6ec;
            border-radius: 8px;
            padding: 0.72rem 0.8rem;
            min-height: 104px;
        }
        [data-testid="stMetricLabel"] {
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.73rem;
        }
        [data-testid="stMetricValue"] {
            color: #27353c;
            font-size: 1.72rem;
        }
        [data-testid="stMetricDelta"] {
            font-size: 0.78rem;
        }
        div[data-testid="stPlotlyChart"] {
            border: 1px solid #e1e6ec;
            border-radius: 8px;
            background: #ffffff;
            padding: 0.1rem;
        }
        .board-section {
            margin: 1.35rem 0 0.7rem;
            padding: 0.6rem 0.85rem;
            border-left: 5px solid #10b6aa;
            color: #27353c;
            font-weight: 760;
            font-size: 1.2rem;
            background: #ffffff;
            border-radius: 0 8px 8px 0;
        }
        .board-section.school {
            border-left-color: #ef5350;
        }
        .board-section .section-subtitle {
            display: block;
            color: #6a7882;
            font-size: 0.84rem;
            font-weight: 500;
            margin-top: 0.18rem;
        }
        .chart-note {
            color: #667580;
            font-size: 0.84rem;
            line-height: 1.35;
            margin: -0.18rem 0 0.7rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def board_section(title: str, subtitle: str | None = None, tone: str = "default") -> None:
    class_name = "board-section school" if tone == "school" else "board-section"
    subtitle_html = f'<span class="section-subtitle">{html.escape(subtitle)}</span>' if subtitle else ""
    st.markdown(
        f'<div class="{class_name}">{html.escape(title)}{subtitle_html}</div>',
        unsafe_allow_html=True,
    )


def chart_note(text: str) -> None:
    st.markdown(f'<div class="chart-note">{html.escape(text)}</div>', unsafe_allow_html=True)


def board_short_label(value: object, max_len: int = 34) -> str:
    text = str(value)
    return text if len(text) <= max_len else f"{text[: max_len - 3].rstrip()}..."


def board_source_label(value: object) -> str:
    source = str(value)
    if source == "Mental School":
        return "Khối phổ thông"
    for school_year in ["2022-2023", "2023-2024", "2024-2025"]:
        if school_year in source:
            return school_year
    return source


def board_population_summary(df: pd.DataFrame) -> pd.DataFrame:
    if POPULATION_COLUMN not in df.columns or df.empty:
        return pd.DataFrame()
    out = (
        df.groupby(POPULATION_COLUMN, as_index=False)["Target"]
        .agg(**{"Số khảo sát": "size", "Có dấu hiệu nguy cơ": "sum", "Tỷ lệ nguy cơ": "mean"})
    )
    out["Nhóm"] = out[POPULATION_COLUMN].map(BOARD_GROUP_LABELS).fillna(out[POPULATION_COLUMN])
    out["Không có dấu hiệu"] = out["Số khảo sát"] - out["Có dấu hiệu nguy cơ"]
    out["Tỷ lệ nguy cơ"] = (out["Tỷ lệ nguy cơ"] * 100).round(2)
    return out


def board_year_summary(df: pd.DataFrame) -> pd.DataFrame:
    if DATA_SOURCE_COLUMN not in df.columns:
        return pd.DataFrame()
    college = df[df[POPULATION_COLUMN] == HMS_POPULATION_LABEL].copy()
    out = (
        college.groupby(DATA_SOURCE_COLUMN, as_index=False)["Target"]
        .agg(**{"Số khảo sát": "size", "Tỷ lệ nguy cơ": "mean"})
    )
    out["Năm khảo sát"] = out[DATA_SOURCE_COLUMN].map(board_source_label)
    out["Tỷ lệ nguy cơ"] = (out["Tỷ lệ nguy cơ"] * 100).round(2)
    return out.sort_values("Năm khảo sát")


def board_construct_comparison(raw_df: pd.DataFrame, cleaned_df: pd.DataFrame) -> pd.DataFrame:
    frames = []
    if POPULATION_COLUMN not in raw_df.columns:
        return pd.DataFrame()
    for internal_group, display_group in BOARD_GROUP_LABELS.items():
        group_index = raw_df.index[raw_df[POPULATION_COLUMN] == internal_group]
        group_cleaned = cleaned_df.loc[group_index]
        score_cols = [
            feature
            for feature in RESEARCH_FEATURES
            if feature in group_cleaned.columns and group_cleaned[feature].notna().any()
        ]
        impact = construct_impact_table(group_cleaned, score_cols)
        if impact.empty:
            continue
        impact["Nhóm"] = display_group
        impact["Yếu tố"] = impact["Construct"].map(BOARD_FACTOR_LABELS)
        impact["Chênh lệch tỷ lệ (%)"] = impact["Thay đổi khi construct tăng (%)"]
        impact["Tỷ lệ khi bất lợi thấp (%)"] = impact["At Risk ở mức thấp (%)"]
        impact["Tỷ lệ khi bất lợi cao (%)"] = impact["At Risk ở mức cao (%)"]
        frames.append(impact)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def board_impact_transition(
    impact: pd.DataFrame,
    factor_col: str,
) -> pd.DataFrame:
    if impact.empty:
        return pd.DataFrame()
    id_cols = [factor_col]
    if "Nhóm" in impact.columns:
        id_cols.append("Nhóm")
    transition = impact.melt(
        id_vars=id_cols,
        value_vars=["Tỷ lệ khi bất lợi thấp (%)", "Tỷ lệ khi bất lợi cao (%)"],
        var_name="Mức bất lợi",
        value_name="Tỷ lệ nguy cơ (%)",
    )
    transition["Mức bất lợi"] = transition["Mức bất lợi"].map(
        {
            "Tỷ lệ khi bất lợi thấp (%)": "Thấp",
            "Tỷ lệ khi bất lợi cao (%)": "Cao",
        }
    )
    transition["Thứ tự"] = transition["Mức bất lợi"].map({"Thấp": 0, "Cao": 1})
    return transition.sort_values(id_cols + ["Thứ tự"])


def board_group_rates(raw_df: pd.DataFrame, qnum: int, category_name: str) -> pd.DataFrame:
    if POPULATION_COLUMN not in raw_df.columns:
        return pd.DataFrame()
    frames = []
    for internal_group, display_group in BOARD_GROUP_LABELS.items():
        group = raw_df[raw_df[POPULATION_COLUMN] == internal_group]
        rate = target_prevalence_by_group(group, qnum, category_name)
        if rate.empty:
            continue
        rate = rate[rate["n"] >= MIN_COMPARISON_GROUP_N].copy()
        rate["Nhóm"] = display_group
        rate["Tỷ lệ nguy cơ (%)"] = rate["At Risk Rate"]
        if category_name == "Grade":
            rate[category_name] = rate[category_name].map(BOARD_GRADE_LABELS).fillna(rate[category_name])
        frames.append(rate)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def board_school_drivers(raw_df: pd.DataFrame, processed) -> pd.DataFrame:
    school = raw_df[raw_df[POPULATION_COLUMN] == MENTAL_SCHOOL_POPULATION_LABEL]
    drivers = top_target_gap_questions(processed, top_n=7, min_category_n=40, df=school)
    if drivers.empty:
        return drivers
    drivers["Tín hiệu cảnh báo"] = drivers.apply(
        lambda row: question_label(row["qnum"], row["question"]),
        axis=1,
    )
    drivers["Chênh lệch tỷ lệ (%)"] = drivers["At Risk Gap (%)"]
    return drivers


def board_scope_data(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    population: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if POPULATION_COLUMN not in raw_df.columns:
        return pd.DataFrame(), pd.DataFrame()
    scoped_raw = raw_df[raw_df[POPULATION_COLUMN] == population].copy()
    return scoped_raw, cleaned_df.loc[scoped_raw.index].copy()


def board_single_group_rate(df: pd.DataFrame, qnum: int, category_name: str) -> pd.DataFrame:
    rate = target_prevalence_by_group(df, qnum, category_name)
    if rate.empty:
        return rate
    rate = rate[rate["n"] >= MIN_COMPARISON_GROUP_N].copy()
    rate["Tỷ lệ nguy cơ (%)"] = rate["At Risk Rate"]
    if category_name == "Grade":
        rate[category_name] = rate[category_name].map(BOARD_GRADE_LABELS).fillna(rate[category_name])
    return rate


def board_dimension_labels(df: pd.DataFrame, qnum: int, dimension_name: str) -> List[str]:
    col = find_q_col(df, qnum)
    if col is None:
        return []
    values = pd.to_numeric(df[col], errors="coerce").dropna().drop_duplicates().sort_values()
    labels = [value_to_label(value, qnum) for value in values]
    if qnum == 3:
        labels = [BOARD_GRADE_LABELS.get(label, label) for label in labels]
    return labels


def board_school_endpoint_by_dimension(
    school: pd.DataFrame,
    selected: pd.Series,
    dimension_qnum: int,
    dimension_name: str,
) -> pd.DataFrame:
    signal_col = find_q_col(school, int(selected["qnum"]))
    dimension_col = find_q_col(school, dimension_qnum)
    if signal_col is None or dimension_col is None:
        return pd.DataFrame()
    base = school[[signal_col, dimension_col, "Target"]].dropna().copy()
    if base.empty:
        return pd.DataFrame()
    base[dimension_name] = base[dimension_col].apply(lambda value: value_to_label(value, dimension_qnum))
    if dimension_qnum == 3:
        base[dimension_name] = base[dimension_name].map(BOARD_GRADE_LABELS).fillna(base[dimension_name])
    denominator = base.groupby(dimension_name).size()
    endpoints = [
        ("Phản hồi gắn với tỷ lệ thấp", selected["Giá trị phản hồi thấp"], selected["Phản hồi tỷ lệ thấp"]),
        ("Phản hồi gắn với tỷ lệ cao", selected["Giá trị phản hồi cao"], selected["Phản hồi tỷ lệ cao"]),
    ]
    rows = []
    for endpoint_name, endpoint_value, endpoint_label in endpoints:
        endpoint = base[base[signal_col] == endpoint_value]
        for dimension, group in endpoint.groupby(dimension_name):
            rows.append(
                {
                    dimension_name: dimension,
                    "Nhóm phản hồi": f"{endpoint_name}: {endpoint_label}",
                    "Tỷ trọng trong nhóm (%)": round(float(len(group) / denominator[dimension] * 100), 2),
                    "Tỷ lệ nguy cơ (%)": round(float(group["Target"].mean() * 100), 2),
                    "Số người": int(len(group)),
                }
            )
    return pd.DataFrame(rows)


def board_college_factor_by_dimension(
    college: pd.DataFrame,
    factor: str,
    dimension_qnum: int,
    dimension_name: str,
) -> pd.DataFrame:
    dimension_col = find_q_col(college, dimension_qnum)
    if factor not in college.columns or dimension_col is None:
        return pd.DataFrame()
    base = college[[factor, dimension_col, "Target"]].dropna().copy()
    if base.empty or base[factor].nunique() < 2:
        return pd.DataFrame()
    base[dimension_name] = base[dimension_col].apply(lambda value: value_to_label(value, dimension_qnum))
    if dimension_qnum == 3:
        base[dimension_name] = base[dimension_name].map(BOARD_GRADE_LABELS).fillna(base[dimension_name])
    rows = []
    for dimension, dimension_group in base.groupby(dimension_name):
        if len(dimension_group) < MIN_COMPARISON_GROUP_N or dimension_group[factor].nunique() < 2:
            continue
        low_threshold = float(dimension_group[factor].quantile(0.25))
        high_threshold = float(dimension_group[factor].quantile(0.75))
        if low_threshold < high_threshold:
            endpoints = [
                ("Mức bất lợi thấp", dimension_group[dimension_group[factor] <= low_threshold]),
                ("Mức bất lợi cao", dimension_group[dimension_group[factor] >= high_threshold]),
            ]
        else:
            endpoints = [
                ("Mức bất lợi thấp", dimension_group[dimension_group[factor] <= low_threshold]),
                ("Mức bất lợi cao", dimension_group[dimension_group[factor] > high_threshold]),
            ]
        for level_label, part in endpoints:
            if part.empty:
                continue
            rows.append(
                {
                    dimension_name: dimension,
                    "Nhóm phản hồi": level_label,
                    "Tỷ trọng trong nhóm (%)": round(float(len(part) / len(dimension_group) * 100), 2),
                    "Tỷ lệ nguy cơ (%)": round(float(part["Target"].mean() * 100), 2),
                    "Số người": int(len(part)),
                }
            )
    return pd.DataFrame(rows)


def render_dimension_heatmap(
    data: pd.DataFrame,
    dimension_name: str,
    dimension_order: List[str],
    metric: str,
    title: str,
    key: str,
    note: str | None = None,
    height: int = 295,
) -> None:
    if data.empty:
        return
    matrix = data.pivot(index="Nhóm phản hồi", columns=dimension_name, values=metric)
    ordered_columns = [label for label in dimension_order if label in matrix.columns]
    matrix = matrix.reindex(columns=ordered_columns or matrix.columns.tolist())
    fig = px.imshow(
        matrix,
        text_auto=".1f",
        aspect="auto",
        color_continuous_scale=["#e7f7f6", "#ffc928", "#ef5350"],
        labels={"x": dimension_name, "y": "", "color": metric},
        title=title,
    )
    fig.update_traces(
        hovertemplate=f"%{{y}}<br>{dimension_name}: %{{x}}<br>{metric}: %{{z:.2f}}%<extra></extra>"
    )
    st.plotly_chart(
        board_chart_layout(fig, height, left_margin=265, right_margin=42, bottom_margin=58),
        width="stretch",
        config=PLOT_CONFIG,
        key=key,
    )
    if note:
        chart_note(note)


def render_cluster_heatmap(
    data: pd.DataFrame,
    dimension_name: str,
    dimension_order: List[str],
    metric: str,
    title: str,
    key: str,
    top_n: int = 8,
    note: str | None = None,
    height: int = 430,
    value_suffix: str = "%",
) -> None:
    if data.empty or metric not in data.columns or "Cụm khảo sát" not in data.columns:
        return
    plot = data.dropna(subset=[dimension_name, metric]).copy()
    if plot.empty:
        return
    plot[metric] = pd.to_numeric(plot[metric], errors="coerce")
    plot = plot.dropna(subset=[metric])
    if plot.empty:
        return
    cluster_order = (
        plot.groupby("Cụm khảo sát")[metric]
        .max()
        .sort_values(ascending=False)
        .head(top_n)
        .index
        .tolist()
    )
    plot = plot[plot["Cụm khảo sát"].isin(cluster_order)].copy()
    matrix = plot.pivot(index="Cụm khảo sát", columns=dimension_name, values=metric)
    ordered_columns = [label for label in dimension_order if label in matrix.columns]
    matrix = matrix.reindex(index=cluster_order, columns=ordered_columns or matrix.columns.tolist())
    short_index = [board_short_label(label, 36) for label in matrix.index]
    customdata = [[cluster for _ in matrix.columns] for cluster in matrix.index]
    fig = go.Figure(
        data=go.Heatmap(
            z=matrix.values,
            x=matrix.columns.tolist(),
            y=short_index,
            customdata=customdata,
            colorscale=[[0.0, "#e7f7f6"], [0.55, "#ffc928"], [1.0, "#ef5350"]],
            colorbar={"title": metric},
            hovertemplate=(
                "Cụm: %{customdata}<br>"
                + f"{dimension_name}: "
                + "%{x}<br>"
                + f"{metric}: "
                + f"%{{z:.2f}}{value_suffix}<extra></extra>"
            ),
        )
    )
    fig.update_traces(text=np.round(matrix.values, 1), texttemplate="%{text:.1f}", textfont={"size": 11})
    fig.update_layout(title=title, xaxis_title=dimension_name, yaxis_title="")
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(
        board_chart_layout(fig, height, left_margin=230, right_margin=58, bottom_margin=70),
        width="stretch",
        config=PLOT_CONFIG,
        key=key,
    )
    if note:
        chart_note(note)


def board_school_all_question_gaps(school: pd.DataFrame, processed) -> pd.DataFrame:
    rows = []
    for col in school.columns:
        qnum = extract_qnum(col)
        cluster = BOARD_SCHOOL_QNUM_TO_GROUP.get(qnum) if qnum is not None else None
        if qnum is None or cluster is None:
            continue
        values = school[[col, "Target"]].dropna()
        if values.empty:
            continue
        grouped = values.groupby(col)["Target"].agg(rate="mean", n="size").reset_index()
        grouped = grouped[grouped["n"] >= 40]
        if len(grouped) < 2:
            continue
        high = grouped.loc[grouped["rate"].idxmax()]
        low = grouped.loc[grouped["rate"].idxmin()]
        rows.append(
            {
                "qnum": qnum,
                "Cụm khảo sát": BOARD_SOURCE_CLUSTER_LABELS.get(cluster, cluster),
                "Tín hiệu": question_label(qnum, QNUM_TO_ENGLISH.get(qnum, col)),
                "Chênh lệch tỷ lệ (%)": round(float(high["rate"] - low["rate"]) * 100, 2),
                "Tỷ lệ thấp (%)": round(float(low["rate"]) * 100, 2),
                "Tỷ lệ cao (%)": round(float(high["rate"]) * 100, 2),
                "Phản hồi tỷ lệ thấp": response_label(value_to_label(low[col], qnum)),
                "Phản hồi tỷ lệ cao": response_label(value_to_label(high[col], qnum)),
                "Giá trị phản hồi thấp": low[col],
                "Giá trị phản hồi cao": high[col],
            }
        )
    return pd.DataFrame(rows)


def board_school_cluster_summary(details: pd.DataFrame) -> pd.DataFrame:
    if details.empty:
        return details
    peak_index = details.groupby("Cụm khảo sát")["Chênh lệch tỷ lệ (%)"].idxmax()
    peak = details.loc[peak_index, ["Cụm khảo sát", "Tín hiệu"]].rename(columns={"Tín hiệu": "Tín hiệu nổi bật"})
    summary = (
        details.groupby("Cụm khảo sát", as_index=False)
        .agg(
            **{
                "Số chỉ báo": ("Tín hiệu", "size"),
                "Chênh lệch cao nhất (%)": ("Chênh lệch tỷ lệ (%)", "max"),
                "Chênh lệch trung bình (%)": ("Chênh lệch tỷ lệ (%)", "mean"),
            }
        )
        .merge(peak, on="Cụm khảo sát", how="left")
    )
    summary["Chênh lệch trung bình (%)"] = summary["Chênh lệch trung bình (%)"].round(2)
    return summary.sort_values("Chênh lệch cao nhất (%)", ascending=False)


def board_school_cluster_scores(school: pd.DataFrame, details: pd.DataFrame) -> pd.DataFrame:
    scores = pd.DataFrame(index=school.index)
    if details.empty:
        return scores
    for cluster, signals in details.groupby("Cụm khảo sát", sort=False):
        indicators = []
        for _, signal in signals.iterrows():
            signal_col = find_q_col(school, int(signal["qnum"]))
            if signal_col is None:
                continue
            values = school[signal_col]
            indicator = pd.Series(np.nan, index=school.index, dtype=float)
            valid = values.notna()
            indicator.loc[valid] = (values.loc[valid] == signal["Giá trị phản hồi cao"]).astype(float) * 100
            indicators.append(indicator)
        if indicators:
            scores[cluster] = pd.concat(indicators, axis=1).mean(axis=1, skipna=True)
    return scores


def board_construct_impact_by_dimension(
    raw_df: pd.DataFrame,
    score_df: pd.DataFrame,
    feature: str,
    feature_label: str,
    dimension_qnum: int,
    dimension_name: str,
) -> pd.DataFrame:
    dimension_col = find_q_col(raw_df, dimension_qnum)
    if dimension_col is None or feature not in score_df.columns:
        return pd.DataFrame()
    dimension_values = raw_df[dimension_col].apply(lambda value: value_to_label(value, dimension_qnum))
    if dimension_qnum == 3:
        dimension_values = dimension_values.map(BOARD_GRADE_LABELS).fillna(dimension_values)
    valid_dimension = ~dimension_values.astype(str).str.strip().str.lower().isin({"missing", "nan", "none", ""})
    rows = []
    for dimension in dimension_values[valid_dimension].drop_duplicates().tolist():
        index = raw_df.index[valid_dimension & (dimension_values == dimension)]
        if len(index) < MIN_COMPARISON_GROUP_N:
            continue
        sample = pd.DataFrame(
            {
                feature: pd.to_numeric(score_df.reindex(index)[feature], errors="coerce"),
                "Target": pd.to_numeric(raw_df.loc[index, "Target"], errors="coerce"),
            }
        )
        impact = construct_impact_table(sample, [feature])
        if impact.empty:
            continue
        row = impact.iloc[0].to_dict()
        row[dimension_name] = dimension
        row["Cụm khảo sát"] = feature_label
        row["Số khảo sát"] = int(len(index))
        rows.append(row)
    return pd.DataFrame(rows)


def board_overview_construct_impact_by_dimension(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    feature: str,
    dimension_qnum: int,
    dimension_name: str,
) -> pd.DataFrame:
    rows = []
    if POPULATION_COLUMN not in raw_df.columns:
        return pd.DataFrame()
    for internal_group, display_group in BOARD_GROUP_LABELS.items():
        group = raw_df[raw_df[POPULATION_COLUMN] == internal_group]
        impact = board_construct_impact_by_dimension(
            group,
            cleaned_df.loc[group.index],
            feature,
            BOARD_FACTOR_LABELS.get(feature, feature),
            dimension_qnum,
            dimension_name,
        )
        if impact.empty:
            continue
        impact["Nhóm"] = display_group
        impact["Phân tầng"] = impact["Nhóm"] + " | " + impact[dimension_name].astype(str)
        rows.append(impact)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def render_dimension_dumbbell(
    data: pd.DataFrame,
    row_column: str,
    title: str,
    key: str,
    height: int = 420,
    note: str | None = None,
) -> None:
    if data.empty:
        return
    plot = data.copy()
    fig = go.Figure()
    for _, row in plot.iterrows():
        fig.add_trace(
            go.Scatter(
                x=[row["At Risk ở mức thấp (%)"], row["At Risk ở mức cao (%)"]],
                y=[row[row_column], row[row_column]],
                mode="lines",
                line={"color": "#c8d3db", "width": 3},
                hoverinfo="skip",
                showlegend=False,
            )
        )
    for value_col, level, color in [
        ("At Risk ở mức thấp (%)", "Mức bất lợi thấp", "#10b6aa"),
        ("At Risk ở mức cao (%)", "Mức bất lợi cao", "#ef5350"),
    ]:
        fig.add_trace(
            go.Scatter(
                x=plot[value_col],
                y=plot[row_column],
                mode="markers",
                name=level,
                marker={"size": 11, "color": color},
                customdata=plot[["Cụm khảo sát", "Thay đổi khi construct tăng (%)", "Số khảo sát"]],
                hovertemplate=(
                    "%{y}<br>%{customdata[0]}<br>"
                    + level
                    + ": %{x:.2f}% nguy cơ<br>"
                    "Chênh lệch tỷ lệ: %{customdata[1]:.2f}%<br>"
                    "Số khảo sát: %{customdata[2]:,}<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        title=title,
        xaxis_title="Tỷ lệ có dấu hiệu nguy cơ (%)",
        yaxis_title="",
        legend={"orientation": "h", "yanchor": "top", "y": -0.18, "x": 0},
    )
    st.plotly_chart(
        board_chart_layout(fig, height, left_margin=158, right_margin=36, bottom_margin=92),
        width="stretch",
        config=PLOT_CONFIG,
        key=key,
    )
    if note:
        chart_note(note)


def board_college_native_impact(college: pd.DataFrame) -> pd.DataFrame:
    available = [
        feature for feature in HMS_NATIVE_FEATURES
        if feature in college.columns and college[feature].notna().any()
    ]
    impact = construct_impact_table(college, available)
    if impact.empty:
        return impact
    impact["Cụm khảo sát sinh viên"] = impact["Construct"].map(BOARD_COLLEGE_NATIVE_LABELS)
    impact["Chênh lệch tỷ lệ (%)"] = impact["Thay đổi khi construct tăng (%)"]
    impact["Tỷ lệ khi bất lợi thấp (%)"] = impact["At Risk ở mức thấp (%)"]
    impact["Tỷ lệ khi bất lợi cao (%)"] = impact["At Risk ở mức cao (%)"]
    return impact


def board_college_native_level_by_year(college: pd.DataFrame) -> pd.DataFrame:
    if DATA_SOURCE_COLUMN not in college.columns:
        return pd.DataFrame()
    rows = []
    for source, group in college.groupby(DATA_SOURCE_COLUMN):
        for feature in HMS_NATIVE_FEATURES:
            if feature not in group.columns or not group[feature].notna().any():
                continue
            rows.append(
                {
                    "Năm khảo sát": board_source_label(source),
                    "Cụm khảo sát sinh viên": BOARD_COLLEGE_NATIVE_LABELS.get(feature, feature),
                    "Mức bất lợi trung bình": round(float(group[feature].mean()), 2),
                    "Số trả lời hợp lệ": int(group[feature].notna().sum()),
                }
            )
    return pd.DataFrame(rows)


def board_college_change_priority(college: pd.DataFrame, impact: pd.DataFrame) -> pd.DataFrame:
    levels = board_college_native_level_by_year(college)
    if levels.empty or impact.empty:
        return pd.DataFrame()
    years = sorted(levels["Năm khảo sát"].unique().tolist())
    if len(years) < 2:
        return pd.DataFrame()
    change = levels.pivot(
        index="Cụm khảo sát sinh viên",
        columns="Năm khảo sát",
        values="Mức bất lợi trung bình",
    ).reset_index()
    change["Thay đổi mức bất lợi"] = (change[years[-1]] - change[years[0]]).round(2)
    return impact[["Cụm khảo sát sinh viên", "Chênh lệch tỷ lệ (%)"]].merge(
        change[["Cụm khảo sát sinh viên", "Thay đổi mức bất lợi"]],
        on="Cụm khảo sát sinh viên",
        how="inner",
    )


def board_score_cluster_by_dimension(
    raw_df: pd.DataFrame,
    score_df: pd.DataFrame,
    feature_labels: dict[str, str],
    dimension_qnum: int,
    dimension_name: str,
) -> pd.DataFrame:
    dimension_col = find_q_col(raw_df, dimension_qnum)
    if dimension_col is None or "Target" not in raw_df.columns:
        return pd.DataFrame()
    dimension_values = raw_df[dimension_col].apply(lambda value: value_to_label(value, dimension_qnum))
    if dimension_qnum == 3:
        dimension_values = dimension_values.map(BOARD_GRADE_LABELS).fillna(dimension_values)
    dimension_values = dimension_values.mask(
        dimension_values.astype(str).str.strip().str.lower().isin({"missing", "nan", "none", ""})
    )
    rows = []
    for feature, label in feature_labels.items():
        if feature not in score_df.columns:
            continue
        base = pd.DataFrame(
            {
                dimension_name: dimension_values,
                "Mức bất lợi trung bình (0-100)": pd.to_numeric(score_df.reindex(raw_df.index)[feature], errors="coerce"),
                "Target": pd.to_numeric(raw_df["Target"], errors="coerce"),
            }
        ).dropna()
        if base.empty or base["Mức bất lợi trung bình (0-100)"].nunique() < 2:
            continue
        feature_values = base["Mức bất lợi trung bình (0-100)"]
        high_threshold = float(feature_values.quantile(0.75))
        if high_threshold <= float(feature_values.min()):
            base["Biểu hiện cao"] = (feature_values > high_threshold).astype(int)
        else:
            base["Biểu hiện cao"] = (feature_values >= high_threshold).astype(int)
        if base["Biểu hiện cao"].sum() == 0:
            continue
        summary = (
            base.groupby(dimension_name, as_index=False)
            .agg(
                **{
                    "Mức bất lợi trung bình (0-100)": ("Mức bất lợi trung bình (0-100)", "mean"),
                    "Số khảo sát": ("Target", "size"),
                    "Tỷ lệ nguy cơ (%)": ("Target", "mean"),
                    "Số biểu hiện cao": ("Biểu hiện cao", "sum"),
                    "Tỷ lệ biểu hiện cao (%)": ("Biểu hiện cao", "mean"),
                }
            )
        )
        summary = summary[summary["Số khảo sát"] >= MIN_COMPARISON_GROUP_N].copy()
        summary["Cụm khảo sát"] = label
        summary["Mức bất lợi trung bình (0-100)"] = summary["Mức bất lợi trung bình (0-100)"].round(2)
        summary["Tỷ lệ nguy cơ (%)"] = (summary["Tỷ lệ nguy cơ (%)"] * 100).round(2)
        summary["Tỷ lệ biểu hiện cao (%)"] = (summary["Tỷ lệ biểu hiện cao (%)"] * 100).round(2)
        rows.append(summary)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def board_overview_cluster_by_dimension(
    raw_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    dimension_qnum: int,
    dimension_name: str,
) -> pd.DataFrame:
    if POPULATION_COLUMN not in raw_df.columns:
        return pd.DataFrame()
    labels = {
        feature: BOARD_FACTOR_LABELS.get(feature, feature)
        for feature in RESEARCH_FEATURES
    }
    rows = []
    for internal_group, display_group in BOARD_GROUP_LABELS.items():
        group = raw_df[raw_df[POPULATION_COLUMN] == internal_group]
        bubble = board_score_cluster_by_dimension(
            group,
            cleaned_df.loc[group.index],
            labels,
            dimension_qnum,
            dimension_name,
        )
        if bubble.empty:
            continue
        bubble["Nhóm"] = display_group
        rows.append(bubble)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def board_school_cluster_by_dimension(
    school: pd.DataFrame,
    details: pd.DataFrame,
    dimension_qnum: int,
    dimension_name: str,
) -> pd.DataFrame:
    dimension_col = find_q_col(school, dimension_qnum)
    if dimension_col is None or details.empty:
        return pd.DataFrame()
    rows = []
    for cluster, signals in details.groupby("Cụm khảo sát", sort=False):
        parts = []
        for _, signal in signals.iterrows():
            signal_col = find_q_col(school, int(signal["qnum"]))
            if signal_col is None:
                continue
            base = school[[dimension_col, signal_col, "Target"]].dropna().copy()
            if base.empty:
                continue
            base[dimension_name] = base[dimension_col].apply(lambda value: value_to_label(value, dimension_qnum))
            if dimension_qnum == 3:
                base[dimension_name] = base[dimension_name].map(BOARD_GRADE_LABELS).fillna(base[dimension_name])
            base = base[
                ~base[dimension_name].astype(str).str.strip().str.lower().isin({"missing", "nan", "none", ""})
            ].copy()
            base["Phản hồi cảnh báo"] = (base[signal_col] == signal["Giá trị phản hồi cao"]).astype(int)
            parts.append(base[[dimension_name, "Target", "Phản hồi cảnh báo"]])
        if not parts:
            continue
        answers = pd.concat(parts, ignore_index=True)
        summary = (
            answers.groupby(dimension_name, as_index=False)
            .agg(
                **{
                    "Số phản hồi cảnh báo": ("Phản hồi cảnh báo", "sum"),
                    "Số phản hồi hợp lệ": ("Phản hồi cảnh báo", "size"),
                    "Tỷ lệ phản hồi cảnh báo (%)": ("Phản hồi cảnh báo", "mean"),
                }
            )
        )
        alert_risk = (
            answers[answers["Phản hồi cảnh báo"] == 1]
            .groupby(dimension_name, as_index=False)["Target"]
            .mean()
            .rename(columns={"Target": "Tỷ lệ nguy cơ trong phản hồi cảnh báo (%)"})
        )
        summary = summary.merge(alert_risk, on=dimension_name, how="left")
        summary = summary[summary["Số phản hồi hợp lệ"] >= MIN_COMPARISON_GROUP_N].copy()
        summary["Cụm khảo sát"] = cluster
        summary["Tỷ lệ phản hồi cảnh báo (%)"] = (summary["Tỷ lệ phản hồi cảnh báo (%)"] * 100).round(2)
        summary["Tỷ lệ nguy cơ trong phản hồi cảnh báo (%)"] = (
            summary["Tỷ lệ nguy cơ trong phản hồi cảnh báo (%)"] * 100
        ).round(2)
        rows.append(summary)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def render_cluster_bubble_chart(
    data: pd.DataFrame,
    dimension_name: str,
    metric: str,
    size_column: str,
    title: str,
    key: str,
    facet_col: Optional[str] = None,
) -> None:
    if data.empty:
        return
    plot = data.dropna(subset=[dimension_name, metric, size_column]).copy()
    if plot.empty:
        return
    plot["Kích thước bubble"] = pd.to_numeric(plot[size_column], errors="coerce").clip(lower=1)
    hover_columns = {
        metric: ":.2f",
        "Kích thước bubble": False,
        size_column: True,
    }
    for column in [
        "Số khảo sát",
        "Tỷ lệ nguy cơ (%)",
        "Số phản hồi cảnh báo",
        "Số phản hồi hợp lệ",
        "Tỷ lệ nguy cơ trong phản hồi cảnh báo (%)",
        "Số biểu hiện cao",
        "Tỷ lệ biểu hiện cao (%)",
    ]:
        if column in plot.columns and column not in hover_columns:
            hover_columns[column] = ":.2f" if "(%)" in column else True
    figure_args = {
        "data_frame": plot,
        "x": dimension_name,
        "y": metric,
        "size": "Kích thước bubble",
        "color": "Cụm khảo sát",
        "hover_name": "Cụm khảo sát",
        "hover_data": hover_columns,
        "size_max": 38,
        "color_discrete_sequence": px.colors.qualitative.Bold,
        "title": title,
    }
    if facet_col is not None and facet_col in plot.columns:
        figure_args["facet_col"] = facet_col
    fig = px.scatter(**figure_args)
    fig.update_traces(marker={"line": {"width": 0.7, "color": "#ffffff"}, "opacity": 0.92})
    if facet_col is not None and facet_col in plot.columns:
        fig.for_each_annotation(lambda annotation: annotation.update(text=annotation.text.split("=")[-1]))
    fig.update_layout(
        xaxis_title=dimension_name,
        yaxis_title=metric,
        legend_title_text="Cụm khảo sát",
        legend={"orientation": "h", "yanchor": "top", "y": -0.24, "x": 0},
    )
    fig.update_xaxes(tickangle=-28)
    fig.update_yaxes(rangemode="tozero")
    height = 520 if facet_col is not None else 480
    st.plotly_chart(
        board_chart_layout(fig, height, left_margin=72, right_margin=38, bottom_margin=145),
        width="stretch",
        config=PLOT_CONFIG,
        key=key,
    )


def board_chart_layout(
    fig: go.Figure,
    height: int,
    left_margin: int = 42,
    right_margin: int = 30,
    bottom_margin: int = 40,
) -> go.Figure:
    fig.update_layout(
        template="plotly_white",
        height=height,
        font=dict(family="Arial", color="#33414a", size=12),
        title_font=dict(size=16, color="#33414a"),
        title_x=0.03,
        margin=dict(l=left_margin, r=right_margin, t=58, b=bottom_margin),
        paper_bgcolor="white",
        plot_bgcolor="white",
        legend_title_text="",
        hoverlabel=dict(bgcolor="white", font_size=12, font_family="Arial"),
    )
    fig.update_xaxes(gridcolor="#edf1f4", linecolor="#d9e0e6", title_font={"size": 13}, tickfont={"size": 12})
    fig.update_yaxes(gridcolor="#edf1f4", linecolor="#d9e0e6", title_font={"size": 13}, tickfont={"size": 12})
    return fig


def render_school_detail_board(filtered_raw: pd.DataFrame, filtered_cleaned: pd.DataFrame, processed) -> None:
    school, _school_cleaned = board_scope_data(filtered_raw, filtered_cleaned, MENTAL_SCHOOL_POPULATION_LABEL)
    if school.empty:
        return
    details = board_school_all_question_gaps(school, processed)
    clusters = board_school_cluster_summary(details)
    age = board_single_group_rate(school, 1, "Age")
    grade = board_filter_table_labels(board_single_group_rate(school, 3, "Grade"), "Grade", BOARD_SCHOOL_GRADE_LABELS)
    rate = float(school["Target"].mean() * 100)
    peak_cluster = str(clusters["Cụm khảo sát"].iloc[0]) if not clusters.empty else "-"
    peak_gap = float(clusters["Chênh lệch cao nhất (%)"].iloc[0]) if not clusters.empty else 0.0
    age_order = board_dimension_labels(school, 1, "Tuổi")
    grade_order = [label for label in board_dimension_labels(school, 3, "Lớp") if label in BOARD_SCHOOL_GRADE_LABELS]

    board_section("Tổng quan nhanh", "KPI, tỷ lệ nguy cơ theo tuổi/lớp và cụm chênh lệch nổi bật.", tone="school")
    cards = st.columns(4, gap="small")
    cards[0].metric("Số học sinh", f"{len(school):,}")
    cards[1].metric("Tỷ lệ có dấu hiệu nguy cơ", f"{rate:.2f}%")
    cards[2].metric("Cụm liên hệ nổi bật", board_short_label(peak_cluster, 30))
    cards[3].metric("Chênh lệch cao nhất", f"{peak_gap:.2f}%")

    first_row = st.columns([1, 1, 1.2], gap="small")
    with first_row[0]:
        if not age.empty:
            fig = px.bar(
                age.sort_values("Age"),
                x="Age",
                y="Tỷ lệ nguy cơ (%)",
                text="Tỷ lệ nguy cơ (%)",
                color="Tỷ lệ nguy cơ (%)",
                color_continuous_scale=["#15b8aa", "#ffc928", "#ef5350"],
                title="Theo tuổi: tỷ lệ nguy cơ về tâm lý",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(xaxis_title="Tuổi", yaxis_title="Tỷ lệ (%)", coloraxis_showscale=False)
            st.plotly_chart(board_chart_layout(fig, 360, right_margin=48), width="stretch", config=PLOT_CONFIG, key="school_age_rate")
    with first_row[1]:
        if not grade.empty:
            fig = px.bar(
                grade,
                x="Grade",
                y="Tỷ lệ nguy cơ (%)",
                text="Tỷ lệ nguy cơ (%)",
                color="Tỷ lệ nguy cơ (%)",
                color_continuous_scale=["#15b8aa", "#ffc928", "#ef5350"],
                title="Theo lớp: tỷ lệ nguy cơ về tâm lý",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(xaxis_title="", yaxis_title="Tỷ lệ (%)", coloraxis_showscale=False)
            st.plotly_chart(board_chart_layout(fig, 360, right_margin=48), width="stretch", config=PLOT_CONFIG, key="school_grade_rate")
    with first_row[2]:
        if not clusters.empty:
            plot = clusters.nlargest(7, "Chênh lệch cao nhất (%)").sort_values("Chênh lệch cao nhất (%)").copy()
            plot["Cụm rút gọn"] = plot["Cụm khảo sát"].apply(lambda value: board_short_label(value, 32))
            fig = px.bar(
                plot,
                x="Chênh lệch cao nhất (%)",
                y="Cụm rút gọn",
                orientation="h",
                text="Chênh lệch cao nhất (%)",
                color="Chênh lệch cao nhất (%)",
                color_continuous_scale=["#15b8aa", "#ffc928", "#ef5350"],
                hover_data={"Cụm khảo sát": True, "Tín hiệu nổi bật": True, "Số chỉ báo": True},
                title="Cụm nào chênh lệch mạnh nhất?",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
            fig.update_layout(xaxis_title="Chênh lệch tỷ lệ (%)", yaxis_title="", coloraxis_showscale=False)
            st.plotly_chart(board_chart_layout(fig, 405, left_margin=180, right_margin=62), width="stretch", config=PLOT_CONFIG, key="school_cluster_rank")

    if not details.empty:
        board_section("Cụm cảnh báo theo tuổi/lớp", "Màu đậm hơn nghĩa là phản hồi cảnh báo xuất hiện nhiều hơn trong nhóm đó.", tone="school")
        school_age_bubbles = board_school_cluster_by_dimension(school, details, 1, "Tuổi")
        school_grade_bubbles = board_school_cluster_by_dimension(school, details, 3, "Lớp")
        cluster_cols = st.columns(2, gap="small")
        with cluster_cols[0]:
            render_cluster_heatmap(
                school_age_bubbles,
                "Tuổi",
                age_order,
                "Tỷ lệ phản hồi cảnh báo (%)",
                "Cụm nào xuất hiện nhiều theo tuổi?",
                "school_cluster_age_heatmap",
                top_n=8,
                note="Màu thể hiện tỷ lệ phản hồi cảnh báo cao trong từng nhóm tuổi; chỉ hiển thị các cụm nổi bật nhất.",
            )
        with cluster_cols[1]:
            render_cluster_heatmap(
                school_grade_bubbles,
                "Lớp",
                grade_order,
                "Tỷ lệ phản hồi cảnh báo (%)",
                "Cụm nào xuất hiện nhiều theo lớp?",
                "school_cluster_grade_heatmap",
                top_n=8,
                note="Màu thể hiện tỷ lệ phản hồi cảnh báo cao trong từng lớp; chỉ hiển thị các cụm nổi bật nhất.",
            )

        school_cluster_scores = board_school_cluster_scores(school, details)
        available_clusters = [
            cluster for cluster in clusters["Cụm khảo sát"].tolist()
            if cluster in school_cluster_scores.columns
        ]
        if available_clusters:
            board_section("Phân tích cụm được chọn", "So sánh nhóm phản hồi mức thấp và mức cao trong từng tuổi/lớp.", tone="school")
            selected_cluster = st.selectbox(
                "Chọn cụm khảo sát",
                available_clusters,
                key="school_selected_construct_dumbbell",
            )
            age_impact = board_construct_impact_by_dimension(
                school,
                school_cluster_scores,
                selected_cluster,
                selected_cluster,
                1,
                "Tuổi",
            )
            grade_impact = board_construct_impact_by_dimension(
                school,
                school_cluster_scores,
                selected_cluster,
                selected_cluster,
                3,
                "Lớp",
            )
            grade_impact = board_filter_table_labels(grade_impact, "Lớp", BOARD_SCHOOL_GRADE_LABELS)
            dumbbell_cols = st.columns(2, gap="small")
            with dumbbell_cols[0]:
                render_dimension_dumbbell(
                    age_impact,
                    "Tuổi",
                    "Cụm được chọn: mức thấp vs mức cao theo tuổi",
                    "school_construct_age_dumbbell",
                    height=405,
                )
            with dumbbell_cols[1]:
                render_dimension_dumbbell(
                    grade_impact,
                    "Lớp",
                    "Cụm được chọn: mức thấp vs mức cao theo lớp",
                    "school_construct_grade_dumbbell",
                    height=405,
                )
            chart_note("Mỗi dòng so sánh tỷ lệ nguy cơ giữa nhóm phản hồi mức thấp và mức cao.")

        board_section("Phân tích tín hiệu được chọn", "Tách rõ tỷ trọng phản hồi và tỷ lệ nguy cơ trong nhóm phản hồi.", tone="school")
        selector_data = details.sort_values("Chênh lệch tỷ lệ (%)", ascending=False).reset_index(drop=True)
        selected_qnum = st.selectbox(
            "Chọn tín hiệu cụ thể",
            selector_data["qnum"].tolist(),
            format_func=lambda qnum: selector_data.loc[selector_data["qnum"] == qnum, "Tín hiệu"].iloc[0],
            key="school_selected_signal",
        )
        selected = selector_data[selector_data["qnum"] == selected_qnum].iloc[0]
        age_detail = board_school_endpoint_by_dimension(school, selected, 1, "Tuổi")
        grade_detail = board_school_endpoint_by_dimension(school, selected, 3, "Lớp")
        distribution_row = st.columns(2, gap="small")
        with distribution_row[0]:
            render_dimension_heatmap(
                age_detail,
                "Tuổi",
                age_order,
                "Tỷ trọng trong nhóm (%)",
                "Tín hiệu được chọn: tỷ trọng phản hồi theo tuổi",
                "school_signal_age_share",
                note="Biểu đồ này thể hiện tỷ lệ người chọn từng phản hồi trong từng nhóm tuổi/lớp.",
            )
        with distribution_row[1]:
            render_dimension_heatmap(
                grade_detail,
                "Lớp",
                grade_order,
                "Tỷ trọng trong nhóm (%)",
                "Tín hiệu được chọn: tỷ trọng phản hồi theo lớp",
                "school_signal_grade_share",
                note="Biểu đồ này thể hiện tỷ lệ người chọn từng phản hồi trong từng nhóm tuổi/lớp.",
            )
        risk_row = st.columns(2, gap="small")
        with risk_row[0]:
            render_dimension_heatmap(
                age_detail,
                "Tuổi",
                age_order,
                "Tỷ lệ nguy cơ (%)",
                "Tỷ lệ nguy cơ trong từng nhóm phản hồi theo tuổi",
                "school_signal_age_risk",
                note="Biểu đồ này thể hiện tỷ lệ có nguy cơ trong từng nhóm phản hồi, không phải tỷ lệ phản hồi.",
            )
        with risk_row[1]:
            render_dimension_heatmap(
                grade_detail,
                "Lớp",
                grade_order,
                "Tỷ lệ nguy cơ (%)",
                "Tỷ lệ nguy cơ trong từng nhóm phản hồi theo lớp",
                "school_signal_grade_risk",
                note="Biểu đồ này thể hiện tỷ lệ có nguy cơ trong từng nhóm phản hồi, không phải tỷ lệ phản hồi.",
            )


def render_college_detail_board(filtered_raw: pd.DataFrame, filtered_cleaned: pd.DataFrame) -> None:
    college, _college_cleaned = board_scope_data(filtered_raw, filtered_cleaned, HMS_POPULATION_LABEL)
    if college.empty:
        return
    college_by_grade = board_college_undergraduate_years_only(college)
    college_by_age = board_filter_q_labels(college_by_grade, 1, BOARD_COLLEGE_UNDERGRAD_AGE_LABELS)
    impact = board_college_native_impact(college)
    levels = board_college_native_level_by_year(college)
    priority = board_college_change_priority(college, impact)
    age = board_filter_table_labels(board_single_group_rate(college_by_age, 1, "Age"), "Age", BOARD_COLLEGE_UNDERGRAD_AGE_LABELS)
    grade = board_single_group_rate(college_by_grade, 3, "Grade")
    years = board_year_summary(college)
    age_order = [label for label in board_dimension_labels(college_by_age, 1, "Tuổi") if label in BOARD_COLLEGE_UNDERGRAD_AGE_LABELS]
    grade_order = board_dimension_labels(college_by_grade, 3, "Năm học")
    college_grade_bubbles = board_score_cluster_by_dimension(
        college_by_grade,
        college_by_grade,
        BOARD_COLLEGE_NATIVE_LABELS,
        3,
        "Năm học",
    )
    rate = float(college["Target"].mean() * 100)
    top_factor = str(impact["Cụm khảo sát sinh viên"].iloc[0]) if not impact.empty else "-"
    top_gap = float(impact["Chênh lệch tỷ lệ (%)"].iloc[0]) if not impact.empty else 0.0

    board_section("Tổng quan nhanh", "KPI, xu hướng theo năm khảo sát, năm học và tuổi.")
    cards = st.columns(4, gap="small")
    cards[0].metric("Số sinh viên", f"{len(college):,}")
    cards[1].metric("Tỷ lệ có dấu hiệu nguy cơ", f"{rate:.2f}%")
    cards[2].metric("Cụm liên hệ nổi bật", board_short_label(top_factor, 30))
    cards[3].metric("Chênh lệch cao nhất", f"{top_gap:.2f}%")

    first_row = st.columns([1, 1, 1], gap="small")
    with first_row[0]:
        if not years.empty:
            fig = px.line(
                years,
                x="Năm khảo sát",
                y="Tỷ lệ nguy cơ",
                markers=True,
                text="Tỷ lệ nguy cơ",
                title="Theo năm khảo sát: tỷ lệ nguy cơ về tâm lý",
            )
            fig.update_traces(line_color="#10b6aa", marker_size=10, texttemplate="%{text:.1f}%", textposition="top center")
            fig.update_layout(xaxis_title="", yaxis_title="Tỷ lệ (%)")
            st.plotly_chart(board_chart_layout(fig, 360, right_margin=48), width="stretch", config=PLOT_CONFIG, key="college_year_rate")
    with first_row[1]:
        if not age.empty:
            fig = px.bar(
                age,
                x="Age",
                y="Tỷ lệ nguy cơ (%)",
                text="Tỷ lệ nguy cơ (%)",
                color="Tỷ lệ nguy cơ (%)",
                color_continuous_scale=["#10b6aa", "#ffc928", "#ef5350"],
                hover_data={"Tỷ lệ nguy cơ (%)": ":.2f", "n": True},
                title="Theo tuổi: tỷ lệ nguy cơ về tâm lý",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
            fig.update_layout(xaxis_title="Tuổi", yaxis_title="Tỷ lệ (%)", coloraxis_showscale=False)
            st.plotly_chart(board_chart_layout(fig, 360, right_margin=42), width="stretch", config=PLOT_CONFIG, key="college_age_rate")
    with first_row[2]:
        if not grade.empty:
            fig = px.bar(
                grade,
                x="Grade",
                y="Tỷ lệ nguy cơ (%)",
                text="Tỷ lệ nguy cơ (%)",
                color="Tỷ lệ nguy cơ (%)",
                color_continuous_scale=["#10b6aa", "#ffc928", "#ef5350"],
                title="Theo năm học: tỷ lệ nguy cơ về tâm lý",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
            fig.update_layout(xaxis_title="", yaxis_title="Tỷ lệ (%)", coloraxis_showscale=False)
            fig.update_xaxes(tickangle=-28)
            st.plotly_chart(board_chart_layout(fig, 385, right_margin=48, bottom_margin=68), width="stretch", config=PLOT_CONFIG, key="college_grade_rate")

    board_section("Cụm bất lợi nổi bật", "Cụm nào liên quan mạnh nhất và tập trung ở năm khảo sát/năm học nào?")
    second_row = st.columns([1, 1], gap="small")
    with second_row[0]:
        if not impact.empty:
            impact_top = impact.nlargest(8, "Chênh lệch tỷ lệ (%)").copy()
            transition = board_impact_transition(impact_top, "Cụm khảo sát sinh viên")
            fig = go.Figure()
            for _, factor in transition.groupby("Cụm khảo sát sinh viên", sort=False):
                factor = factor.sort_values("Thứ tự")
                fig.add_trace(
                    go.Scatter(
                        x=factor["Tỷ lệ nguy cơ (%)"],
                        y=factor["Cụm khảo sát sinh viên"],
                        mode="lines",
                        line={"color": "#c5d0d8", "width": 3},
                        hoverinfo="skip",
                        showlegend=False,
                    )
                )
            for level, color in [("Thấp", "#10b6aa"), ("Cao", "#ef5350")]:
                endpoint = transition[transition["Mức bất lợi"] == level]
                fig.add_trace(
                    go.Scatter(
                        x=endpoint["Tỷ lệ nguy cơ (%)"],
                        y=endpoint["Cụm khảo sát sinh viên"],
                        mode="markers",
                        name=f"Mức bất lợi {level.lower()}",
                        marker={"size": 11, "color": color},
                        customdata=endpoint[["Mức bất lợi"]],
                        hovertemplate="%{y}<br>%{customdata[0]}: %{x:.2f}% nguy cơ<extra></extra>",
                    )
                )
            fig.update_layout(title="Cụm nào chênh lệch mạnh nhất?")
            fig.update_layout(xaxis_title="Tỷ lệ có dấu hiệu nguy cơ (%)", yaxis_title="")
            st.plotly_chart(board_chart_layout(fig, 500, left_margin=245, right_margin=35), width="stretch", config=PLOT_CONFIG, key="college_native_gap")
            chart_note("Mỗi dòng so sánh tỷ lệ nguy cơ giữa nhóm phản hồi mức thấp và mức cao.")
    with second_row[1]:
        if not levels.empty:
            year_levels = levels.rename(columns={"Cụm khảo sát sinh viên": "Cụm khảo sát"})
            render_cluster_heatmap(
                year_levels,
                "Năm khảo sát",
                sorted(year_levels["Năm khảo sát"].dropna().unique().tolist()),
                "Mức bất lợi trung bình",
                "Cụm bất lợi theo năm khảo sát",
                "college_native_year_heatmap",
                top_n=8,
                note="Heatmap chỉ hiển thị tối đa 8 cụm có mức bất lợi nổi bật nhất.",
                height=500,
                value_suffix="",
            )
    if not college_grade_bubbles.empty:
        render_cluster_heatmap(
            college_grade_bubbles,
            "Năm học",
            grade_order,
            "Tỷ lệ biểu hiện cao (%)",
            "Cụm bất lợi theo năm học",
            "college_native_grade_heatmap",
            top_n=8,
            note="Màu thể hiện tỷ lệ sinh viên có cụm bất lợi ở mức cao trong từng năm học.",
            height=390,
        )

    if not priority.empty:
        board_section("Ưu tiên can thiệp", "Xem cụm vừa tăng theo thời gian, vừa liên quan mạnh đến nguy cơ.")
        fig = px.scatter(
            priority,
            x="Thay đổi mức bất lợi",
            y="Chênh lệch tỷ lệ (%)",
            size=priority["Chênh lệch tỷ lệ (%)"].abs().clip(lower=1),
            color="Chênh lệch tỷ lệ (%)",
            color_continuous_scale=["#10b6aa", "#ffc928", "#ef5350"],
            hover_name="Cụm khảo sát sinh viên",
            hover_data={"Thay đổi mức bất lợi": ":.2f", "Chênh lệch tỷ lệ (%)": ":.2f"},
            title="Cụm nào nên ưu tiên xem xét?",
        )
        fig.update_layout(
            xaxis_title="Thay đổi mức bất lợi từ năm đầu đến năm cuối",
            yaxis_title="Chênh lệch tỷ lệ nguy cơ giữa mức cao và thấp (%)",
            coloraxis_showscale=False,
        )
        fig.add_vline(x=0, line_dash="dash", line_color="#9baab4")
        fig.add_hline(y=0, line_dash="dash", line_color="#9baab4")
        st.plotly_chart(board_chart_layout(fig, 465, right_margin=42), width="stretch", config=PLOT_CONFIG, key="college_change_priority")
        chart_note("Góc trên bên phải là nhóm vừa tăng theo thời gian, vừa liên quan mạnh đến nguy cơ. Các điểm phía trên thể hiện cụm có liên hệ mạnh với nguy cơ; điểm lệch sang phải thể hiện xu hướng tăng theo thời gian.")

    if not impact.empty:
        board_section("Phân tích cụm được chọn", "So sánh mức thấp/cao theo tuổi, năm học và từng nhóm phản hồi.")
        selected_factor = st.selectbox(
            "Chọn cụm khảo sát",
            impact["Construct"].tolist(),
            format_func=lambda factor: BOARD_COLLEGE_NATIVE_LABELS.get(factor, factor),
            key="college_selected_factor",
        )
        selected_label = BOARD_COLLEGE_NATIVE_LABELS.get(selected_factor, selected_factor)
        age_impact = board_construct_impact_by_dimension(
            college_by_age,
            college_by_age,
            selected_factor,
            selected_label,
            1,
            "Tuổi",
        )
        grade_impact = board_construct_impact_by_dimension(
            college_by_grade,
            college_by_grade,
            selected_factor,
            selected_label,
            3,
            "Năm học",
        )
        dumbbell_cols = st.columns(2, gap="small")
        with dumbbell_cols[0]:
            render_dimension_dumbbell(
                age_impact,
                "Tuổi",
                "Cụm được chọn: mức thấp vs mức cao theo tuổi",
                "college_construct_age_dumbbell",
                height=405,
            )
        with dumbbell_cols[1]:
            render_dimension_dumbbell(
                grade_impact,
                "Năm học",
                "Cụm được chọn: mức thấp vs mức cao theo năm học",
                "college_construct_grade_dumbbell",
                height=405,
            )
        chart_note("Mỗi dòng so sánh tỷ lệ nguy cơ giữa nhóm phản hồi mức thấp và mức cao.")
        age_detail = board_college_factor_by_dimension(college_by_age, selected_factor, 1, "Tuổi")
        grade_detail = board_college_factor_by_dimension(college_by_grade, selected_factor, 3, "Năm học")
        risk_row = st.columns(2, gap="small")
        with risk_row[0]:
            render_dimension_heatmap(
                age_detail,
                "Tuổi",
                age_order,
                "Tỷ lệ nguy cơ (%)",
                "Tỷ lệ nguy cơ trong từng nhóm phản hồi theo tuổi",
                "college_factor_age_risk",
                note="Biểu đồ này thể hiện tỷ lệ có nguy cơ trong từng nhóm phản hồi, không phải tỷ lệ phản hồi.",
            )
        with risk_row[1]:
            render_dimension_heatmap(
                grade_detail,
                "Năm học",
                grade_order,
                "Tỷ lệ nguy cơ (%)",
                "Tỷ lệ nguy cơ trong từng nhóm phản hồi theo năm học",
                "college_factor_grade_risk",
                note="Biểu đồ này thể hiện tỷ lệ có nguy cơ trong từng nhóm phản hồi, không phải tỷ lệ phản hồi.",
            )


def render_executive_board(
    filtered_raw: pd.DataFrame,
    filtered_cleaned: pd.DataFrame,
    processed,
    selected_page: str,
) -> None:
    heading_by_page = {
        "Tổng quan": (
            "THEO DÕI NGUY CƠ SỨC KHỎE TINH THẦN",
            "Học sinh và sinh viên | tỷ lệ trong nhóm và yếu tố cảnh báo liên quan",
        ),
        "Học sinh": (
            "PHÂN TÍCH NGUY CƠ | HỌC SINH",
            "Cụm khảo sát chi tiết, tuổi, lớp và tín hiệu liên quan nổi bật",
        ),
        "Sinh viên": (
            "PHÂN TÍCH NGUY CƠ | SINH VIÊN",
            "Cụm khảo sát chi tiết, năm học, độ tuổi và xu hướng theo năm",
        ),
    }
    heading, subtitle = heading_by_page.get(selected_page, heading_by_page["Tổng quan"])
    st.markdown(
        f"""
        <div class="board-head">
            <h1>{heading}</h1>
            <p>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if filtered_raw.empty:
        st.warning("Không có bản ghi phù hợp với bộ lọc hiện tại.")
        return

    if selected_page == "Học sinh":
        render_school_detail_board(filtered_raw, filtered_cleaned, processed)
        render_chatbot_gold_section(selected_page)
        return
    if selected_page == "Sinh viên":
        render_college_detail_board(filtered_raw, filtered_cleaned)
        render_chatbot_gold_section(selected_page)
        return

    board_section("Tổng quan nhanh", "Quy mô mẫu, tỷ lệ nguy cơ toàn bộ và nhóm có tỷ lệ cao hơn.")
    population = board_population_summary(filtered_raw)
    total = int(filtered_raw.shape[0])
    risk_rate = float(filtered_raw["Target"].mean() * 100) if total else 0.0
    college = population[population["Nhóm"] == "Sinh viên"]
    school = population[population["Nhóm"] == "Học sinh"]
    college_n = int(college["Số khảo sát"].iloc[0]) if not college.empty else 0
    school_n = int(school["Số khảo sát"].iloc[0]) if not school.empty else 0
    college_rate = float(college["Tỷ lệ nguy cơ"].iloc[0]) if not college.empty else 0.0
    school_rate = float(school["Tỷ lệ nguy cơ"].iloc[0]) if not school.empty else 0.0
    if school_rate > college_rate:
        higher_group = "Học sinh"
        higher_delta = school_rate - college_rate
    elif college_rate > school_rate:
        higher_group = "Sinh viên"
        higher_delta = college_rate - school_rate
    else:
        higher_group = "Tương đương"
        higher_delta = 0.0

    cards = st.columns(5, gap="small")
    cards[0].metric("Tổng số bản ghi", f"{total:,}")
    cards[1].metric("Tỷ lệ nguy cơ toàn bộ", f"{risk_rate:.3f}%")
    cards[2].metric("Số học sinh", f"{school_n:,}")
    cards[3].metric("Số sinh viên", f"{college_n:,}")
    cards[4].metric("Nhóm có nguy cơ cao hơn", higher_group, f"{higher_delta:.2f} điểm %")

    years = board_year_summary(filtered_raw)
    overview_cols = st.columns([1, 1, 1], gap="small") if not years.empty else st.columns([1, 1], gap="small")
    with overview_cols[0]:
        fig = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=risk_rate,
                number={"suffix": "%", "valueformat": ".3f", "font": {"size": 34}},
                gauge={
                    "axis": {"range": [0, 100], "ticksuffix": "%"},
                    "bar": {"color": "#ef5350", "thickness": 0.28},
                    "bgcolor": "#eef3f3",
                    "steps": [
                        {"range": [0, 35], "color": "#dff5f2"},
                        {"range": [35, 65], "color": "#fff2bf"},
                        {"range": [65, 100], "color": "#fde1dc"},
                    ],
                    "threshold": {
                        "line": {"color": "#27353c", "width": 4},
                        "thickness": 0.8,
                        "value": risk_rate,
                    },
                },
                title={"text": "TOÀN BỘ MẪU"},
            )
        )
        fig.update_layout(title_text="Tỷ lệ nguy cơ về tâm lý", title_x=0.03)
        st.plotly_chart(board_chart_layout(fig, 345, right_margin=36), width="stretch", config=PLOT_CONFIG, key="board_gauge_risk")

    with overview_cols[1]:
        rate_plot = population.sort_values("Tỷ lệ nguy cơ").copy()
        fig = px.bar(
            rate_plot,
            x="Tỷ lệ nguy cơ",
            y="Nhóm",
            orientation="h",
            text="Tỷ lệ nguy cơ",
            color="Nhóm",
            color_discrete_map={"Học sinh": "#ef5350", "Sinh viên": "#10b6aa"},
            title="Nhóm nào có tỷ lệ nguy cơ về tâm lý cao hơn?",
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(xaxis_title="Tỷ lệ (%)", yaxis_title="", showlegend=False)
        st.plotly_chart(board_chart_layout(fig, 345, right_margin=62), width="stretch", config=PLOT_CONFIG, key="board_rate_population")

    if len(overview_cols) > 2:
        with overview_cols[2]:
            fig = px.line(
                years,
                x="Năm khảo sát",
                y="Tỷ lệ nguy cơ",
                text="Tỷ lệ nguy cơ",
                markers=True,
                title="Sinh viên: tỷ lệ nguy cơ về tâm lý theo năm khảo sát",
            )
            fig.update_traces(line_color="#10b6aa", marker_size=11, texttemplate="%{text:.2f}%", textposition="top center")
            fig.update_layout(yaxis_title="Tỷ lệ (%)", xaxis_title="")
            st.plotly_chart(board_chart_layout(fig, 345, right_margin=52), width="stretch", config=PLOT_CONFIG, key="board_college_year_trend")

    construct = board_construct_comparison(filtered_raw, filtered_cleaned)
    if not construct.empty:
        board_section("Cụm nổi bật", "Xếp hạng cụm có chênh lệch nguy cơ lớn nhất giữa mức thấp và mức cao.")
        plot = construct.nlargest(10, "Chênh lệch tỷ lệ (%)").sort_values("Chênh lệch tỷ lệ (%)").copy()
        plot["Cụm rút gọn"] = plot["Yếu tố"].apply(lambda value: board_short_label(value, 34))
        plot["Nhãn"] = plot["Nhóm"] + " | " + plot["Cụm rút gọn"]
        fig = px.bar(
            plot,
            x="Chênh lệch tỷ lệ (%)",
            y="Nhãn",
            orientation="h",
            text="Chênh lệch tỷ lệ (%)",
            color="Nhóm",
            color_discrete_map={"Học sinh": "#ef5350", "Sinh viên": "#10b6aa"},
            hover_data={
                "Yếu tố": True,
                "Nhóm": True,
                "Tỷ lệ khi bất lợi thấp (%)": ":.2f",
                "Tỷ lệ khi bất lợi cao (%)": ":.2f",
                "Cụm rút gọn": False,
                "Nhãn": False,
            },
            title="Cụm nào chênh lệch mạnh nhất?",
        )
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
        fig.update_layout(xaxis_title="Chênh lệch tỷ lệ nguy cơ (%)", yaxis_title="", showlegend=True)
        st.plotly_chart(
            board_chart_layout(fig, 440, left_margin=230, right_margin=58, bottom_margin=80),
            width="stretch",
            config=PLOT_CONFIG,
            key="board_construct_compare",
        )

    overview_age_raw = board_college_undergraduate_ages_only(filtered_raw)
    overview_age_cleaned = filtered_cleaned.loc[overview_age_raw.index].copy()
    overview_grade_raw = board_college_undergraduate_years_only(filtered_raw)
    overview_grade_cleaned = filtered_cleaned.loc[overview_grade_raw.index].copy()
    age = board_group_rates(overview_age_raw, 1, "Age")
    grade = board_group_rates(overview_grade_raw, 3, "Grade")
    if not age.empty or not grade.empty:
        board_section("Phân bố theo nhóm", "So sánh tỷ lệ nguy cơ theo tuổi và lớp/năm học.")
    distribution_row = st.columns([1.0, 1.0], gap="small")
    with distribution_row[0]:
        if not age.empty:
            fig = px.bar(
                age,
                x="Age",
                y="Tỷ lệ nguy cơ (%)",
                color="Nhóm",
                text="Tỷ lệ nguy cơ (%)",
                barmode="group",
                color_discrete_map={"Học sinh": "#ef5350", "Sinh viên": "#10b6aa"},
                labels={"Age": "Tuổi", "n": "Số khảo sát"},
                hover_data={"Tỷ lệ nguy cơ (%)": ":.2f", "n": True},
                title="Theo tuổi: tỷ lệ nguy cơ về tâm lý",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
            fig.update_layout(xaxis_title="Tuổi", yaxis_title="Tỷ lệ (%)")
            st.plotly_chart(board_chart_layout(fig, 405, right_margin=42), width="stretch", config=PLOT_CONFIG, key="board_age_rate")
    with distribution_row[1]:
        if not grade.empty:
            fig = px.bar(
                grade,
                x="Grade",
                y="Tỷ lệ nguy cơ (%)",
                color="Nhóm",
                text="Tỷ lệ nguy cơ (%)",
                color_discrete_map={"Học sinh": "#ef5350", "Sinh viên": "#10b6aa"},
                labels={"Grade": "Lớp / năm học", "n": "Số khảo sát"},
                title="Lớp / năm học: tỷ lệ nguy cơ về tâm lý",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
            fig.update_layout(xaxis_title="", yaxis_title="Tỷ lệ (%)")
            fig.update_xaxes(tickangle=-26)
            st.plotly_chart(board_chart_layout(fig, 405, right_margin=46, bottom_margin=68), width="stretch", config=PLOT_CONFIG, key="board_grade_rate")

    available_common_constructs = [
        feature for feature in RESEARCH_FEATURES
        if feature in filtered_cleaned.columns and filtered_cleaned[feature].notna().any()
    ]
    if available_common_constructs:
        board_section("Phân tích chi tiết theo cụm được chọn", "Dropdown giúp demo một cụm chung và mức thấp/cao theo từng phân tầng.")
        selected_common_construct = st.selectbox(
            "Chọn cụm khảo sát",
            available_common_constructs,
            format_func=lambda feature: BOARD_FACTOR_LABELS.get(feature, feature),
            key="overview_selected_construct_dumbbell",
        )
        selected_label = BOARD_FACTOR_LABELS.get(selected_common_construct, selected_common_construct)
        overview_age_impact = board_overview_construct_impact_by_dimension(
            overview_age_raw,
            overview_age_cleaned,
            selected_common_construct,
            1,
            "Tuổi",
        )
        overview_grade_impact = board_overview_construct_impact_by_dimension(
            overview_grade_raw,
            overview_grade_cleaned,
            selected_common_construct,
            3,
            "Lớp / năm học",
        )
        detail_cols = st.columns(2, gap="small")
        with detail_cols[0]:
            render_dimension_dumbbell(
                overview_age_impact,
                "Phân tầng",
                "Cụm được chọn: mức thấp vs mức cao theo tuổi",
                "overview_construct_age_dumbbell",
                height=455,
            )
        with detail_cols[1]:
            render_dimension_dumbbell(
                overview_grade_impact,
                "Phân tầng",
                "Cụm được chọn: mức thấp vs mức cao theo lớp/năm học",
                "overview_construct_grade_dumbbell",
                height=455,
            )
        chart_note("Mỗi dòng so sánh tỷ lệ nguy cơ giữa nhóm phản hồi mức thấp và mức cao.")

    render_chatbot_gold_section(selected_page)

def main() -> None:
    inject_css()
    selected_page = main_navigation()

    processed, source_text, source_shape, cache_status = load_data_ui()

    if processed is None:
        render_no_data_state()
        return

    populations, sources, ages, genders, grades, targets = sidebar_description_filters(processed)
    filtered_raw = apply_description_filters(processed.raw_analysis, populations, sources, ages, genders, grades, targets)
    filtered_cleaned = processed.cleaned.loc[filtered_raw.index].copy()
    sidebar_status(source_text, source_shape, filtered_raw, cache_status)

    inject_board_css()
    render_executive_board(filtered_raw, filtered_cleaned, processed, selected_page)


if __name__ == "__main__":
    main()
