from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


QNUM_TO_ENGLISH: Dict[int, str] = {
    1: "Age",
    2: "Gender",
    3: "Grade",
    4: "Hispanic/Latino",
    5: "Race",
    6: "Height (m)",
    7: "Weight (kg)",
    8: "Seatbelt use",
    9: "Rode with drunk driver",
    10: "Drove after drinking",
    11: "Texted while driving",
    12: "Carried weapon to school",
    13: "Carried a gun",
    14: "Skipped school (felt unsafe)",
    15: "Threatened/injured with weapon",
    16: "Physical fight (any)",
    17: "Physical fight at school",
    18: "Witnessed neighbourhood violence",
    19: "Forced sexual intercourse",
    20: "Other forced sex acts",
    21: "Dating violence (forced sex)",
    22: "Dating violence (physical)",
    23: "Discriminated (race/ethnicity)",
    24: "Bullied at school",
    25: "Cyberbullied",
    26: "Felt sad or hopeless",
    27: "Considered suicide",
    28: "Made suicide plan",
    29: "Attempted suicide",
    30: "Suicide attempt needed treatment",
    31: "Ever smoked cigarettes",
    32: "Smoked before age 13",
    33: "Current cigarette use",
    34: "Cigarettes per day",
    35: "Ever used e-cigarettes",
    36: "Current e-cigarette use",
    37: "Source of e-cigarettes",
    38: "Smokeless tobacco",
    39: "Cigars",
    40: "Tried to quit smoking",
    41: "Drank alcohol before 13",
    42: "Current alcohol use",
    43: "Binge drinking",
    44: "Max drinks in a row",
    45: "Usual alcohol source",
    46: "Ever used marijuana",
    47: "Marijuana initiation age",
    48: "Current marijuana use",
    49: "Misused prescription painkillers",
    50: "Ever cocaine",
    51: "Ever inhalants",
    52: "Ever heroin",
    53: "Ever methamphetamine",
    54: "Ever ecstasy",
    55: "Ever injected drugs",
    56: "Ever had sex",
    57: "First sex before 13",
    58: "Lifetime partners (4+)",
    59: "Sexually active (3 mo)",
    60: "Alcohol/drugs before last sex",
    61: "Condom use at last sex",
    62: "Birth control use",
    63: "Sex of sexual partners",
    64: "Sexual orientation",
    65: "Transgender identity",
    66: "Self-perceived weight",
    67: "Weight intention",
    68: "100% fruit juice",
    69: "Fruit intake",
    70: "Salad intake",
    71: "Potato intake",
    72: "Carrot intake",
    73: "Other vegetables intake",
    74: "Soda intake",
    75: "Breakfast frequency",
    76: "Physical activity (60+ min)",
    77: "PE class attendance",
    78: "Sports team",
    79: "Sports concussion",
    80: "Social media screen time",
    81: "Ever tested HIV",
    82: "Tested for STD",
    83: "Last dental visit",
    84: "Poor mental health days",
    85: "Hours of sleep",
    86: "Place of sleep",
    87: "Self-reported grades",
    88: "Forced sex by adult (5+ years older)",
    89: "Verbal abuse by adult at home",
    90: "Physical abuse by adult at home",
    91: "Witnessed domestic violence",
    92: "Current misuse of painkillers",
    93: "Ever hallucinogens",
    94: "Asked verbal consent before sex",
    95: "Sports drinks intake",
    96: "Water intake",
    97: "Muscle strengthening",
    98: "Sunburn frequency",
    99: "Basic needs met by adult (food/clothing/safety)",
    100: "Lived with substance abuser",
    101: "Lived with mentally ill/suicidal",
    102: "Parent incarcerated",
    103: "School connectedness",
    104: "Parents know whereabouts",
    105: "Unfair discipline at school",
    106: "Difficulty concentrating/remembering",
    107: "English proficiency",
}


CLUSTER_MAP: Dict[int, str] = {
    1: "Demographics",
    2: "Demographics",
    3: "Demographics",
    4: "Demographics",
    5: "Demographics",
    6: "Demographics",
    7: "Demographics",
    86: "Housing & Basic Needs",
    99: "Housing & Basic Needs",
    104: "Family Support & Monitoring",
    106: "Functional Difficulty",
    23: "School Climate & Academic Context",
    24: "School Climate & Academic Context",
    25: "School Climate & Academic Context",
    87: "School Climate & Academic Context",
    103: "School Climate & Academic Context",
    105: "School Climate & Academic Context",
    107: "School Climate & Academic Context",
    66: "Lifestyle Factors",
    67: "Lifestyle Factors",
    68: "Lifestyle Factors",
    69: "Lifestyle Factors",
    70: "Lifestyle Factors",
    71: "Lifestyle Factors",
    72: "Lifestyle Factors",
    73: "Lifestyle Factors",
    74: "Lifestyle Factors",
    75: "Lifestyle Factors",
    76: "Lifestyle Factors",
    77: "Lifestyle Factors",
    78: "Lifestyle Factors",
    85: "Lifestyle Factors",
    95: "Lifestyle Factors",
    96: "Lifestyle Factors",
    97: "Lifestyle Factors",
    8: "Safety & Driving",
    9: "Safety & Driving",
    10: "Safety & Driving",
    11: "Safety & Driving",
    12: "Violence & Safety",
    13: "Violence & Safety",
    14: "Violence & Safety",
    15: "Violence & Safety",
    16: "Violence & Safety",
    17: "Violence & Safety",
    18: "Violence & Safety",
    19: "Sexual Violence",
    20: "Sexual Violence",
    21: "Sexual Violence",
    22: "Sexual Violence",
    88: "Sexual Violence",
    31: "Substance Use",
    32: "Substance Use",
    33: "Substance Use",
    34: "Substance Use",
    35: "Substance Use",
    36: "Substance Use",
    37: "Substance Use",
    38: "Substance Use",
    39: "Substance Use",
    40: "Substance Use",
    41: "Substance Use",
    42: "Substance Use",
    43: "Substance Use",
    44: "Substance Use",
    45: "Substance Use",
    46: "Substance Use",
    47: "Substance Use",
    48: "Substance Use",
    49: "Substance Use",
    50: "Substance Use",
    51: "Substance Use",
    52: "Substance Use",
    53: "Substance Use",
    54: "Substance Use",
    55: "Substance Use",
    92: "Substance Use",
    93: "Substance Use",
    56: "Sexual Behavior",
    57: "Sexual Behavior",
    58: "Sexual Behavior",
    59: "Sexual Behavior",
    60: "Sexual Behavior",
    61: "Sexual Behavior",
    62: "Sexual Behavior",
    94: "Sexual Behavior",
    63: "Sexual Identity & Contacts",
    64: "Sexual Identity & Contacts",
    65: "Sexual Identity & Contacts",
    89: "Family & ACEs",
    90: "Family & ACEs",
    91: "Family & ACEs",
    100: "Family & ACEs",
    101: "Family & ACEs",
    102: "Family & ACEs",
    27: "Mental Health Indicators",
    28: "Mental Health Indicators",
    29: "Mental Health Indicators",
    30: "Mental Health Indicators",
    84: "Mental Health Indicators",
    79: "Other Health Risks",
    98: "Other Health Risks",
    80: "Digital Behavior",
    81: "Health Care & Preventive Services",
    82: "Health Care & Preventive Services",
    83: "Health Care & Preventive Services",
}


CATEGORY_LABELS: Dict[str, Dict[Any, str]] = {
    "q1": {
        1: "<= 12",
        2: "13",
        3: "14",
        4: "15",
        5: "16",
        6: "17",
        7: "18+",
        8: "19-20",
        9: "21-24",
        10: "25-34",
        11: "35+",
    },
    "q2": {
        1: "Female",
        2: "Male",
        3: "Other/Unspecified",
        4: "Other/Unspecified",
        5: "Other/Unspecified",
        6: "Other/Unspecified",
    },
    "q3": {
        1: "9th",
        2: "10th",
        3: "11th",
        4: "12th",
        5: "Ungraded/Other",
        6: "College 1st year",
        7: "College 2nd year",
        8: "College 3rd year",
        9: "College 4th+ year",
        10: "Graduate/Professional",
        11: "Other college",
    },
    "Target": {0: "Lower Risk", 1: "At Risk"},
}

STUDENT_FOCUS_Q1_VALUES = {2, 3, 4, 5, 6, 7, 8, 9}


RESPONSE_LABELS: Dict[int, Dict[Any, str]] = {
    1: CATEGORY_LABELS["q1"],
    2: CATEGORY_LABELS["q2"],
    3: CATEGORY_LABELS["q3"],
    4: {1: "Yes", 2: "No"},
    14: {
        1: "Không nghỉ học vì không an toàn",
        2: "Nghỉ 1 ngày",
        3: "Nghỉ 2-3 ngày",
        4: "Nghỉ 4-5 ngày",
        5: "Nghỉ 6+ ngày",
    },
    15: {
        1: "Không bị đe dọa/bị thương",
        2: "1 lần",
        3: "2-3 lần",
        4: "4-5 lần",
        5: "6-7 lần",
        6: "8-9 lần",
        7: "10-11 lần",
        8: "12+ lần",
    },
    18: {1: "Yes", 2: "No"},
    19: {1: "Yes", 2: "No"},
    20: {
        1: "Không xảy ra",
        2: "1 lần",
        3: "2-3 lần",
        4: "4-5 lần",
        5: "6+ lần",
    },
    21: {
        1: "Không hẹn hò",
        2: "Không xảy ra",
        3: "1 lần",
        4: "2-3 lần",
        5: "4-5 lần",
        6: "6+ lần",
    },
    22: {
        1: "Không hẹn hò",
        2: "Không xảy ra",
        3: "1 lần",
        4: "2-3 lần",
        5: "4-5 lần",
        6: "6+ lần",
    },
    24: {1: "Yes", 2: "No"},
    25: {1: "Yes", 2: "No"},
    26: {1: "Yes", 2: "No"},
    27: {1: "Yes", 2: "No"},
    28: {1: "Yes", 2: "No"},
    29: {1: "Yes", 2: "No"},
    30: {1: "Yes", 2: "No"},
    31: {1: "Yes", 2: "No"},
    33: {
        1: "Không sử dụng",
        2: "1-2 ngày",
        3: "3-5 ngày",
        4: "6-9 ngày",
        5: "10-19 ngày",
        6: "20-29 ngày",
        7: "30 ngày",
    },
    35: {1: "Yes", 2: "No"},
    36: {
        1: "Không sử dụng",
        2: "1-2 ngày",
        3: "3-5 ngày",
        4: "6-9 ngày",
        5: "10-19 ngày",
        6: "20-29 ngày",
        7: "30 ngày",
    },
    42: {
        1: "Không uống rượu",
        2: "1-2 ngày",
        3: "3-5 ngày",
        4: "6-9 ngày",
        5: "10-19 ngày",
        6: "20-29 ngày",
        7: "30 ngày",
    },
    43: {
        1: "Không uống quá mức",
        2: "1 ngày",
        3: "2 ngày",
        4: "3-5 ngày",
        5: "6-9 ngày",
        6: "10+ ngày",
    },
    46: {1: "Yes", 2: "No"},
    48: {
        1: "Không sử dụng",
        2: "1-2 lần",
        3: "3-9 lần",
        4: "10-19 lần",
        5: "20-39 lần",
        6: "40+ lần",
    },
    56: {1: "Yes", 2: "No"},
    65: {1: "Yes", 2: "No", 3: "Not sure"},
    81: {1: "Yes", 2: "No", 3: "Not sure"},
    82: {1: "Yes", 2: "No", 3: "Not sure"},
    75: {
        1: "Không ngày nào",
        2: "1 ngày",
        3: "2 ngày",
        4: "3 ngày",
        5: "4 ngày",
        6: "5 ngày",
        7: "6 ngày",
        8: "7 ngày",
    },
    76: {
        1: "0 ngày",
        2: "1 ngày",
        3: "2 ngày",
        4: "3 ngày",
        5: "4 ngày",
        6: "5 ngày",
        7: "6 ngày",
        8: "7 ngày",
    },
    80: {
        1: "Không dùng",
        2: "< 1 giờ/ngày",
        3: "1 giờ/ngày",
        4: "2 giờ/ngày",
        5: "3 giờ/ngày",
        6: "4+ giờ/ngày",
    },
    84: {1: "Never", 2: "Rarely", 3: "Sometimes", 4: "Most of the time", 5: "Always"},
    85: {
        1: "4 giờ hoặc ít hơn",
        2: "5 giờ",
        3: "6 giờ",
        4: "7 giờ",
        5: "8 giờ",
        6: "9 giờ",
        7: "10+ giờ",
    },
    87: {
        1: "Chủ yếu điểm A",
        2: "Chủ yếu điểm B",
        3: "Chủ yếu điểm C",
        4: "Chủ yếu điểm D",
        5: "Chủ yếu điểm F",
    },
    88: {1: "Yes", 2: "No"},
    89: {1: "Never", 2: "Rarely", 3: "Sometimes", 4: "Most of the time", 5: "Always"},
    90: {1: "Never", 2: "Rarely", 3: "Sometimes", 4: "Most of the time", 5: "Always"},
    91: {1: "Never", 2: "Rarely", 3: "Sometimes", 4: "Most of the time", 5: "Always"},
    92: {
        1: "Không sử dụng",
        2: "1-2 lần",
        3: "3-9 lần",
        4: "10-19 lần",
        5: "20-39 lần",
        6: "40+ lần",
    },
    96: {
        1: "Không uống",
        2: "1 ly/ngày",
        3: "2 ly/ngày",
        4: "3 ly/ngày",
        5: "4 ly/ngày",
        6: "5+ ly/ngày",
    },
    99: {1: "Never", 2: "Rarely", 3: "Sometimes", 4: "Most of the time", 5: "Always"},
    100: {1: "Yes", 2: "No"},
    101: {1: "Yes", 2: "No"},
    102: {1: "Yes", 2: "No"},
    103: {1: "Strongly agree", 2: "Agree", 3: "Not sure", 4: "Disagree", 5: "Strongly disagree"},
    104: {1: "Never", 2: "Rarely", 3: "Sometimes", 4: "Most of the time", 5: "Always"},
    105: {1: "Yes", 2: "No"},
    106: {1: "Yes", 2: "No"},
}


TARGET_DEFINITION = (
    "Nhóm có dấu hiệu nguy cơ được xác định từ cảm giác buồn bã/tuyệt vọng kéo dài "
    "hoặc tần suất ngày sức khỏe tinh thần kém ở mức cao. Với nhóm sinh viên, chỉ báo này được "
    "quy đổi từ các thang đo trầm cảm/lo âu tương ứng. Đây là nhãn phân tích mô tả, "
    "không phải chẩn đoán lâm sàng."
)


RESEARCH_FEATURE_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "Family Pressure Index": {
        "cluster": "Family Pressure",
        "meaning": "Áp lực và bất lợi trong gia đình: bạo hành lời nói/thể chất, chứng kiến bạo lực, thiếu nhu cầu cơ bản, sống cùng người lạm dụng chất hoặc có vấn đề sức khỏe tinh thần, cha/mẹ từng bị giam giữ, thiếu giám sát của phụ huynh.",
        "qnums": [89, 90, 91, 99, 100, 101, 102, 104],
    },
    "Academic Pressure Index": {
        "cluster": "Academic Pressure",
        "meaning": "Áp lực học thuật và khó khăn chức năng ở trường: điểm học tập thấp hơn, thiếu kết nối trường học, kỷ luật không công bằng, khó tập trung/ghi nhớ/ra quyết định.",
        "qnums": [87, 103, 105, 106],
    },
    "Peer & Safety Stress Index": {
        "cluster": "Peer & Safety Stress",
        "meaning": "Căng thẳng từ bạn bè và môi trường an toàn: bị bắt nạt, bị bắt nạt mạng, bị đe dọa bằng vũ khí, thấy bạo lực khu vực sống, nghỉ học vì thấy không an toàn.",
        "qnums": [14, 15, 18, 24, 25],
    },
    "Trauma Exposure Index": {
        "cluster": "Trauma Exposure",
        "meaning": "Phơi nhiễm sang chấn và bạo lực tình dục/hẹn hò, gồm các trải nghiệm bị ép buộc hoặc bị bạo lực trong quan hệ.",
        "qnums": [19, 20, 21, 22, 88],
    },
    "Substance Coping Risk Index": {
        "cluster": "Substance Coping",
        "meaning": "Hành vi dùng chất có thể đi cùng hoặc phản ánh cơ chế đối phó không lành mạnh: thuốc lá, vape, rượu, binge drinking, cần sa, lạm dụng painkiller.",
        "qnums": [33, 36, 42, 43, 48, 92],
    },
    "Lifestyle Recovery Deficit": {
        "cluster": "Recovery Lifestyle",
        "meaning": "Thiếu các yếu tố phục hồi hằng ngày: ngủ chưa phù hợp, ít ăn sáng, ít vận động, uống ít nước, thời gian mạng xã hội cao.",
        "qnums": [75, 76, 80, 85, 96],
    },
}

RESEARCH_FEATURES: List[str] = list(RESEARCH_FEATURE_DEFINITIONS.keys())

# Các nhóm chỉ báo chỉ có ở bộ khảo sát sinh viên, dùng cho phân tích riêng nguồn.
HMS_NATIVE_FEATURE_DEFINITIONS: Dict[str, Dict[str, str]] = {
    "College Financial Strain": {"cluster": "Student Native", "label": "Khó khăn tài chính & nhu cầu cơ bản"},
    "College Academic Adjustment": {"cluster": "Student Native", "label": "Điều chỉnh học tập & áp lực thành tích"},
    "College Belonging Deficit": {"cluster": "Student Native", "label": "Thiếu gắn kết với môi trường học"},
    "College Discrimination Exposure": {"cluster": "Student Native", "label": "Trải nghiệm phân biệt đối xử"},
    "College Campus Safety Stress": {"cluster": "Student Native", "label": "Cảm giác thiếu an toàn trong/ngoài trường"},
    "College Relationship Harm": {"cluster": "Student Native", "label": "Bạo lực và tổn hại trong quan hệ"},
    "College Substance Exposure": {"cluster": "Student Native", "label": "Rượu, thuốc lá và chất kích thích"},
    "College Recovery Deficit": {"cluster": "Student Native", "label": "Thiếu ngủ và vận động"},
}
HMS_NATIVE_FEATURES: List[str] = list(HMS_NATIVE_FEATURE_DEFINITIONS.keys())
DEMOGRAPHIC_MODEL_QNUMS: List[int] = [1, 2, 3]

DATA_SOURCE_COLUMN = "Data Source"
POPULATION_COLUMN = "Population"
STUDY_YEAR_COLUMN = "Study Year"
HMS_AGE_COLUMN = "HMS Age"
HMS_AGE_OUTLIER_COLUMN = "HMS Age Outlier"
HMS_SCOPED_MISSING_PCT_COLUMN = "HMS Scoped Missing (%)"
HARMONIZED_MARKER_COLUMN = "__harmonized_schema"
INTERNAL_SOURCE_TYPE_COLUMN = "__source_type"
INTERNAL_SOURCE_FILE_COLUMN = "__source_file"

MENTAL_SCHOOL_SOURCE_LABEL = "Mental School"
MENTAL_SCHOOL_POPULATION_LABEL = "Middle/High School"
HMS_POPULATION_LABEL = "College/University"

HMS_SCOPED_COLUMNS = {
    "StartDate",
    "Finished",
    "RecordedDate",
    "responseid",
    "schoolnum",
    "inst_hmsyear",
    "age",
    "sex_birth",
    "gender_male",
    "gender_female",
    "gender_queer",
    "gender_nonbin",
    "gender_trans",
    "gender_transm",
    "gender_transf",
    "gender_prefnoresp",
    "gender_selfID",
    "gender_selfid",
    "yr_sch",
    "enroll",
    "deprawsc",
    "anx_score",
    "dep_maj",
    "dep_any",
    "anx_any",
    "dep_or_anx",
    "sui_idea",
    "sui_plan",
    "sui_att",
    "phq9_1",
    "phq9_9",
    "gad7_1",
    "gad7_7",
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
    "fam_support_aca",
    "prof_support_aca",
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
    "abuse_life",
    "abuse_recent",
    "stalk_exp",
    "assault_sex",
    "assault_sex_y",
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
    "alc_any",
    "alc_binge_female",
    "alc_binge_male",
    "alc_binge_othgen",
    "binge_fr",
    "sub_any",
    "sub_cig",
    "smok_freq",
    "smok_vape",
    "risk_alc",
    "risk_cig",
    "risk_mar",
    "risk_presc",
    "risk_vape",
    "drug_mar",
    "mar_freq",
    "sleep_wknight",
    "sleep_wkend",
    "exerc",
    "exerc_range5",
    "exerc_range4",
    "nrweight",
    "weighted_sum",
}


@dataclass
class ProcessedData:
    raw: pd.DataFrame
    raw_analysis: pd.DataFrame
    cleaned: pd.DataFrame
    feature_frame: pd.DataFrame
    target: pd.Series
    feature_cols: List[str]
    col_to_display: Dict[str, str]
    col_to_cluster: Dict[str, str]
    raw_col_to_display: Dict[str, str]
    raw_col_to_cluster: Dict[str, str]
    dropped_meta_cols: List[str]
    target_q26_col: str
    target_q84_col: str
    derived_features: List[str]


def extract_qnum(col_name: str) -> Optional[int]:
    s = str(col_name).strip().lower()
    if s.startswith("q") and s[1:].isdigit():
        return int(s[1:])
    return None


def find_q_col(df: pd.DataFrame, num: int) -> Optional[str]:
    for col in df.columns:
        if extract_qnum(col) == num:
            return col
    return None


def _first_existing_col(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    lookup = {str(col).strip().lower(): col for col in df.columns}
    for name in names:
        col = lookup.get(str(name).strip().lower())
        if col is not None:
            return col
    return None


def _numeric_from_first(df: pd.DataFrame, names: List[str]) -> Optional[pd.Series]:
    col = _first_existing_col(df, names)
    if col is None:
        return None
    return pd.to_numeric(df[col], errors="coerce")


def _ordered_risk_from_first(
    df: pd.DataFrame,
    names: List[str],
    min_value: float,
    max_value: float,
    reverse: bool = False,
) -> Optional[pd.Series]:
    series = _numeric_from_first(df, names)
    if series is None or max_value <= min_value:
        return None
    valid = series.where(series.between(min_value, max_value))
    risk = (valid - min_value) / (max_value - min_value)
    if reverse:
        risk = 1 - risk
    return risk.clip(0, 1)


def _ordered_mean_risk(
    df: pd.DataFrame,
    names: List[str],
    min_value: float,
    max_value: float,
    reverse: bool = False,
) -> Optional[pd.Series]:
    parts = [
        _ordered_risk_from_first(df, [name], min_value, max_value, reverse=reverse)
        for name in names
    ]
    valid_parts = [part for part in parts if part is not None]
    if not valid_parts:
        return None
    frame = pd.concat(valid_parts, axis=1)
    score = frame.mean(axis=1, skipna=True)
    score[frame.notna().sum(axis=1) == 0] = np.nan
    score.index = df.index
    return score.clip(0, 1)


def _binary_risk_from_first(df: pd.DataFrame, names: List[str]) -> Optional[pd.Series]:
    series = _numeric_from_first(df, names)
    if series is None:
        return None
    return series.map({1: 1.0, 0: 0.0, 2: 0.0})


def _binary_any_risk(df: pd.DataFrame, names: List[str]) -> Optional[pd.Series]:
    parts = []
    for name in names:
        part = _binary_risk_from_first(df, [name])
        if part is not None:
            parts.append(part)
    if not parts:
        return None
    frame = pd.concat(parts, axis=1)
    out = frame.max(axis=1, skipna=True)
    out[frame.notna().sum(axis=1) == 0] = np.nan
    return out


def _binary_to_yes_no(series: Optional[pd.Series], index: Optional[pd.Index] = None) -> pd.Series:
    if series is None:
        return pd.Series(np.nan, index=index, dtype=float)
    return series.map({1: 1.0, 0: 2.0, 2: 2.0})


def _hms_age_to_q1(age: pd.Series) -> pd.Series:
    out = pd.Series(np.nan, index=age.index, dtype=float)
    out[(age >= 18) & (age <= 18)] = 7
    out[(age >= 19) & (age <= 20)] = 8
    out[(age >= 21) & (age <= 24)] = 9
    out[(age >= 25) & (age <= 34)] = 10
    out[age >= 35] = 11
    return out


def _hms_gender_to_q2(df: pd.DataFrame) -> pd.Series:
    index = df.index
    out = pd.Series(np.nan, index=index, dtype=float)
    sex_birth = _numeric_from_first(df, ["sex_birth"])
    if sex_birth is not None:
        out[sex_birth == 1] = 1
        out[sex_birth == 2] = 2
        out[sex_birth == 3] = 6

    female = _binary_risk_from_first(df, ["gender_female"])
    male = _binary_risk_from_first(df, ["gender_male"])
    if female is not None:
        out[female == 1] = 1
    if male is not None:
        out[male == 1] = 2

    nonbinary = _binary_any_risk(df, ["gender_nonbin", "gender_queer"])
    transgender = _binary_any_risk(df, ["gender_trans", "gender_transm", "gender_transf"])
    self_described = _binary_any_risk(df, ["gender_selfID", "gender_selfid"])
    prefer_no_response = _binary_risk_from_first(df, ["gender_prefnoresp"])
    if nonbinary is not None:
        out[nonbinary == 1] = 3
    if transgender is not None:
        out[transgender == 1] = 4
    if self_described is not None:
        out[self_described == 1] = 5
    if prefer_no_response is not None:
        out[prefer_no_response == 1] = 6
    return out


def _hms_year_to_q3(df: pd.DataFrame) -> pd.Series:
    yr_sch = _numeric_from_first(df, ["yr_sch"])
    if yr_sch is None:
        return pd.Series(np.nan, index=df.index, dtype=float)
    return yr_sch.map({1: 6, 2: 7, 3: 8, 4: 9, 5: 9, 6: 10, 7: 11}).astype(float)


def _hms_severity_to_q84(df: pd.DataFrame) -> pd.Series:
    severity_parts = []
    for names in [["deprawsc"], ["anx_score"]]:
        score = _numeric_from_first(df, names)
        if score is None:
            continue
        severity = pd.Series(np.nan, index=df.index, dtype=float)
        severity[(score >= 0) & (score <= 4)] = 1
        severity[(score >= 5) & (score <= 9)] = 2
        severity[(score >= 10) & (score <= 14)] = 3
        severity[(score >= 15) & (score <= 19)] = 4
        severity[(score >= 20)] = 5
        severity_parts.append(severity)
    if not severity_parts:
        return pd.Series(np.nan, index=df.index, dtype=float)
    frame = pd.concat(severity_parts, axis=1)
    out = frame.max(axis=1, skipna=True)
    out[frame.notna().sum(axis=1) == 0] = np.nan
    return out


def _hms_q26_proxy(df: pd.DataFrame) -> pd.Series:
    depression = _binary_risk_from_first(df, ["dep_any", "dep_maj"])
    if depression is None:
        score = _numeric_from_first(df, ["deprawsc"])
        depression = pd.Series(np.nan, index=df.index, dtype=float)
        if score is not None:
            depression[score < 10] = 0
            depression[score >= 10] = 1
    return _binary_to_yes_no(depression)


def _hms_sleep_deficit(df: pd.DataFrame) -> Optional[pd.Series]:
    parts = []
    for name in ["sleep_wknight", "sleep_wkend"]:
        hours = _numeric_from_first(df, [name])
        if hours is None:
            continue
        valid = hours.where(hours.between(1, 12))
        deficit = pd.Series(np.nan, index=df.index, dtype=float)
        deficit[valid < 7] = (7 - valid[valid < 7]) / 6
        deficit[(valid >= 7) & (valid <= 9)] = 0
        deficit[valid > 9] = (valid[valid > 9] - 9) / 3
        parts.append(deficit.clip(0, 1))
    if not parts:
        return None
    frame = pd.concat(parts, axis=1)
    out = frame.mean(axis=1, skipna=True)
    out[frame.notna().sum(axis=1) == 0] = np.nan
    return out


def _hms_source_label(source_name: Optional[str]) -> str:
    if not source_name:
        return "HMS"
    base = str(source_name).replace("\\", "/").split("/")[-1].replace(".csv", "")
    parts = base.split("_")
    for part in parts:
        if any(char.isdigit() for char in part) and "-" in part:
            return f"HMS {part}"
    return base.replace("_PUBLIC_instchars", "").replace("_", " ")


def _source_file_for_group(df: pd.DataFrame, fallback: str) -> str:
    col = _first_existing_col(df, [INTERNAL_SOURCE_FILE_COLUMN])
    if col is None:
        return fallback
    values = df[col].dropna().astype(str)
    if values.empty:
        return fallback
    return values.iloc[0]


def is_hms_schema(df: pd.DataFrame) -> bool:
    cols = {str(col).strip().lower() for col in df.columns}
    return bool({"deprawsc", "anx_score", "sui_idea", "yr_sch", "inst_hmsyear"} & cols)


def is_mental_school_schema(df: pd.DataFrame) -> bool:
    return find_q_col(df, 26) is not None and find_q_col(df, 84) is not None


def harmonize_mental_school_dataframe(df_raw: pd.DataFrame, source_name: Optional[str] = None) -> pd.DataFrame:
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()
    q_cols = [col for col in df.columns if extract_qnum(col) is not None]
    df = df[q_cols].copy()
    df[DATA_SOURCE_COLUMN] = _hms_source_label(source_name) if source_name else MENTAL_SCHOOL_SOURCE_LABEL
    df[POPULATION_COLUMN] = MENTAL_SCHOOL_POPULATION_LABEL
    if STUDY_YEAR_COLUMN not in df.columns:
        df[STUDY_YEAR_COLUMN] = MENTAL_SCHOOL_SOURCE_LABEL
    df[HARMONIZED_MARKER_COLUMN] = True
    return df.drop(columns=[INTERNAL_SOURCE_TYPE_COLUMN, INTERNAL_SOURCE_FILE_COLUMN], errors="ignore")


def harmonize_hms_dataframe(df_raw: pd.DataFrame, source_name: Optional[str] = None) -> pd.DataFrame:
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()
    index = df.index
    out = pd.DataFrame(index=index)
    source_label = _hms_source_label(source_name)
    out[DATA_SOURCE_COLUMN] = source_label
    out[POPULATION_COLUMN] = HMS_POPULATION_LABEL

    study_year = _numeric_from_first(df, ["inst_hmsyear"])
    out[STUDY_YEAR_COLUMN] = study_year.astype("Int64").astype(str) if study_year is not None else source_label

    age_raw = _numeric_from_first(df, ["age"])
    if age_raw is None:
        age_raw = pd.Series(np.nan, index=index, dtype=float)
    age_clean = age_raw.where(age_raw.between(18, 80))
    out[HMS_AGE_COLUMN] = age_clean
    out[HMS_AGE_OUTLIER_COLUMN] = (age_raw.notna() & age_clean.isna()).astype(int)
    out["q1"] = _hms_age_to_q1(age_clean)
    out["q2"] = _hms_gender_to_q2(df)
    out["q3"] = _hms_year_to_q3(df)
    out["q26"] = _hms_q26_proxy(df)
    out["q27"] = _binary_to_yes_no(_binary_risk_from_first(df, ["sui_idea"]), index)
    out["q28"] = _binary_to_yes_no(_binary_risk_from_first(df, ["sui_plan"]), index)
    out["q29"] = _binary_to_yes_no(_binary_risk_from_first(df, ["sui_att"]), index)
    out["q84"] = _hms_severity_to_q84(df)

    scoped_cols = [col for col in df.columns if str(col).strip() in HMS_SCOPED_COLUMNS]
    out[HMS_SCOPED_MISSING_PCT_COLUMN] = (
        df[scoped_cols].isna().mean(axis=1) * 100 if scoped_cols else np.nan
    )

    out["Family Pressure Index"] = _mean_index(
        [
            _ordered_risk_from_first(df, ["housing_worry"], 1, 3),
            _ordered_risk_from_first(df, ["food_worry"], 1, 3),
            # HMS codes financial comfort/support items opposite to adverse direction.
            _ordered_risk_from_first(df, ["fincur"], 1, 5, reverse=True),
            _ordered_risk_from_first(df, ["finpast"], 1, 5, reverse=True),
            _ordered_mean_risk(df, ["afford_school", "afford_food", "afford_transp", "afford_hc", "afford_books", "afford_house"], 1, 6),
            _ordered_mean_risk(df, ["pay_worry", "pay_worry1", "pay_worry2", "pay_worry3"], 1, 6, reverse=True),
            _ordered_mean_risk(df, ["fam_support_aca", "prof_support_aca"], 1, 6),
        ],
        index,
    )
    out["Academic Pressure Index"] = _mean_index(
        [
            _ordered_risk_from_first(df, ["aca_impa"], 1, 4),
            _ordered_mean_risk(df, ["stress1", "stress2", "stress3", "stress4"], 1, 5),
            _ordered_risk_from_first(df, ["compet_sch"], 1, 5, reverse=True),
            _ordered_risk_from_first(df, ["grade_curv"], 1, 5),
            _ordered_mean_risk(df, ["imposter_1", "imposter_2", "imposter_3", "imposter_4", "imposter_5"], 1, 5),
            _binary_risk_from_first(df, ["failed"]),
            _ordered_mean_risk(df, ["adjust_aca_1", "adjust_aca_2", "time_manage", "doubt_school_1"], 1, 6),
        ],
        index,
    )
    out["Peer & Safety Stress Index"] = _mean_index(
        [
            _ordered_mean_risk(df, ["belong1", "belong2", "belong8", "belong9"], 1, 6),
            _binary_any_risk(df, ["discrim_race", "discrim_culture", "discrim_gender", "discrim_sexual", "discrim_other"]),
            _ordered_mean_risk(df, ["safe_on_day", "safe_on_night", "safe_off_day", "safe_off_night"], 1, 6),
            _ordered_risk_from_first(df, ["hostcli_distress"], 1, 5),
        ],
        index,
    )
    out["Trauma Exposure Index"] = _mean_index(
        [
            _ordered_risk_from_first(df, ["abuse_life"], 1, 5),
            _ordered_risk_from_first(df, ["abuse_recent"], 1, 5),
            _binary_risk_from_first(df, ["stalk_exp"]),
            _ordered_risk_from_first(df, ["assault_sex", "sa_exp"], 1, 4),
            _binary_any_risk(df, ["IPV_1", "IPV_2", "IPV_3", "IPV_4", "IPV_5", "partner_phys", "partner_insult", "partner_threat", "partner_curse"]),
        ],
        index,
    )
    out["Substance Coping Risk Index"] = _mean_index(
        [
            _binary_risk_from_first(df, ["alc_any"]),
            _ordered_risk_from_first(df, ["binge_fr"], 1, 6),
            _binary_risk_from_first(df, ["sub_any"]),
            _binary_risk_from_first(df, ["sub_cig"]),
            _ordered_risk_from_first(df, ["smok_freq", "smok_vape"], 1, 5),
            _binary_risk_from_first(df, ["drug_mar"]),
            _ordered_risk_from_first(df, ["mar_freq"], 1, 5),
        ],
        index,
    )
    out["Lifestyle Recovery Deficit"] = _mean_index(
        [
            _hms_sleep_deficit(df),
            _ordered_risk_from_first(df, ["exerc", "exerc_range5"], 1, 6, reverse=True),
            _ordered_risk_from_first(df, ["exerc_range4"], 1, 4, reverse=True),
            _ordered_risk_from_first(df, ["food_worry"], 1, 3),
        ],
        index,
    )

    out["College Financial Strain"] = _mean_index(
        [
            _ordered_risk_from_first(df, ["housing_worry"], 1, 3),
            _ordered_risk_from_first(df, ["food_worry"], 1, 3),
            _ordered_risk_from_first(df, ["fincur"], 1, 5, reverse=True),
            _ordered_risk_from_first(df, ["finpast"], 1, 5, reverse=True),
            _ordered_mean_risk(df, ["afford_school", "afford_food", "afford_transp", "afford_hc", "afford_books", "afford_house"], 1, 6),
            _ordered_mean_risk(df, ["pay_worry", "pay_worry1", "pay_worry2", "pay_worry3"], 1, 6, reverse=True),
        ],
        index,
    )
    out["College Academic Adjustment"] = _mean_index(
        [
            _ordered_risk_from_first(df, ["aca_impa"], 1, 4),
            _ordered_mean_risk(df, ["stress1", "stress2", "stress3", "stress4"], 1, 5),
            _ordered_risk_from_first(df, ["compet_sch"], 1, 5, reverse=True),
            _ordered_risk_from_first(df, ["grade_curv"], 1, 5),
            _ordered_mean_risk(df, ["imposter_1", "imposter_2", "imposter_3", "imposter_4", "imposter_5"], 1, 5),
            _binary_risk_from_first(df, ["failed"]),
            _ordered_mean_risk(df, ["adjust_aca_1", "adjust_aca_2", "time_manage", "doubt_school_1"], 1, 6),
        ],
        index,
    )
    out["College Belonging Deficit"] = _mean_index(
        [_ordered_mean_risk(df, ["belong1", "belong2", "belong8", "belong9"], 1, 6)],
        index,
    )
    out["College Discrimination Exposure"] = _mean_index(
        [_binary_any_risk(df, ["discrim_race", "discrim_culture", "discrim_gender", "discrim_sexual", "discrim_other"])],
        index,
    )
    out["College Campus Safety Stress"] = _mean_index(
        [
            _ordered_mean_risk(df, ["safe_on_day", "safe_on_night", "safe_off_day", "safe_off_night"], 1, 6),
            _ordered_risk_from_first(df, ["hostcli_distress"], 1, 5),
        ],
        index,
    )
    out["College Relationship Harm"] = _mean_index(
        [
            _ordered_risk_from_first(df, ["abuse_life"], 1, 5),
            _ordered_risk_from_first(df, ["abuse_recent"], 1, 5),
            _binary_risk_from_first(df, ["stalk_exp"]),
            _ordered_risk_from_first(df, ["assault_sex", "sa_exp"], 1, 4),
            _binary_any_risk(df, ["IPV_1", "IPV_2", "IPV_3", "IPV_4", "IPV_5", "partner_phys", "partner_insult", "partner_threat", "partner_curse"]),
        ],
        index,
    )
    out["College Substance Exposure"] = _mean_index(
        [
            _binary_risk_from_first(df, ["alc_any"]),
            _ordered_risk_from_first(df, ["binge_fr"], 1, 6),
            _binary_risk_from_first(df, ["sub_any"]),
            _binary_risk_from_first(df, ["sub_cig"]),
            _ordered_risk_from_first(df, ["smok_freq", "smok_vape"], 1, 5),
            _binary_risk_from_first(df, ["drug_mar"]),
            _ordered_risk_from_first(df, ["mar_freq"], 1, 5),
        ],
        index,
    )
    out["College Recovery Deficit"] = _mean_index(
        [
            _hms_sleep_deficit(df),
            _ordered_risk_from_first(df, ["exerc", "exerc_range5"], 1, 6, reverse=True),
            _ordered_risk_from_first(df, ["exerc_range4"], 1, 4, reverse=True),
        ],
        index,
    )

    out[HARMONIZED_MARKER_COLUMN] = True
    return out.reset_index(drop=True)


def harmonize_survey_data(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()
    if HARMONIZED_MARKER_COLUMN in df.columns:
        return df.reset_index(drop=True), []

    source_type_col = _first_existing_col(df, [INTERNAL_SOURCE_TYPE_COLUMN])
    frames: List[pd.DataFrame] = []
    if source_type_col is not None:
        source_types = df[source_type_col].fillna("").astype(str).str.lower()
        source_file_col = _first_existing_col(df, [INTERNAL_SOURCE_FILE_COLUMN])
        source_files = (
            df[source_file_col].fillna("").astype(str)
            if source_file_col is not None
            else source_types
        )
        keys = pd.DataFrame({"source_type": source_types, "source_file": source_files}, index=df.index)
        for (source_type, source_name), group_index in keys.groupby(["source_type", "source_file"], dropna=False).groups.items():
            group = df.loc[group_index].copy()
            source_name = source_name or source_type
            if source_type == "hms":
                frames.append(harmonize_hms_dataframe(group, source_name))
            else:
                frames.append(harmonize_mental_school_dataframe(group, source_name))
    elif is_hms_schema(df) and not is_mental_school_schema(df):
        frames.append(harmonize_hms_dataframe(df, "HMS"))
    else:
        frames.append(harmonize_mental_school_dataframe(df, MENTAL_SCHOOL_SOURCE_LABEL))

    harmonized = pd.concat(frames, ignore_index=True, sort=False)
    return harmonized, []


def clean_raw_dataframe(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    df = df_raw.copy()
    df.columns = df.columns.astype(str).str.strip()

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: np.nan if isinstance(x, str) and x.strip() == "" else x)
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass

    df = df.replace([np.inf, -np.inf], np.nan)

    meta_cols = [
        INTERNAL_SOURCE_TYPE_COLUMN,
        INTERNAL_SOURCE_FILE_COLUMN,
        HARMONIZED_MARKER_COLUMN,
        "site",
        "raceeth",
        "q6orig",
        "q7orig",
        "record",
        "orig_rec",
        "BMIPCT",
        "weight",
        "stratum",
        "psu",
        "nrweight",
        "weighted_sum",
    ]
    existing_meta_cols = [c for c in meta_cols if c in df.columns]
    df = df.drop(columns=existing_meta_cols, errors="ignore")
    return df, existing_meta_cols


def build_column_metadata(df: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    col_to_display: Dict[str, str] = {}
    col_to_cluster: Dict[str, str] = {}

    for col in df.columns:
        qnum = extract_qnum(col)
        if col in RESEARCH_FEATURE_DEFINITIONS:
            col_to_display[col] = col
            col_to_cluster[col] = RESEARCH_FEATURE_DEFINITIONS[col]["cluster"]
        elif col in HMS_NATIVE_FEATURE_DEFINITIONS:
            col_to_display[col] = HMS_NATIVE_FEATURE_DEFINITIONS[col]["label"]
            col_to_cluster[col] = HMS_NATIVE_FEATURE_DEFINITIONS[col]["cluster"]
        elif col in {DATA_SOURCE_COLUMN, POPULATION_COLUMN, STUDY_YEAR_COLUMN, HMS_AGE_COLUMN}:
            col_to_display[col] = col
            col_to_cluster[col] = "Demographics"
        elif col == HMS_SCOPED_MISSING_PCT_COLUMN or col == HMS_AGE_OUTLIER_COLUMN:
            col_to_display[col] = col
            col_to_cluster[col] = "Data Quality"
        else:
            col_to_display[col] = QNUM_TO_ENGLISH.get(qnum, str(col).strip())
            col_to_cluster[col] = CLUSTER_MAP.get(qnum, "Other")

    return col_to_display, col_to_cluster


def encode_race_and_categoricals(
    df: pd.DataFrame,
    col_to_display: Dict[str, str],
    col_to_cluster: Dict[str, str],
) -> Tuple[pd.DataFrame, Dict[str, str], Dict[str, str]]:
    df = df.copy()
    col_to_display = dict(col_to_display)
    col_to_cluster = dict(col_to_cluster)

    q5_col = find_q_col(df, 5)
    if q5_col is not None and df[q5_col].dtype == object:
        race_values = df[q5_col].fillna("Missing").astype(str).str.strip()
        race_dummies = pd.get_dummies(race_values, prefix="q5_race", dtype=int)
        df = pd.concat([df.drop(columns=[q5_col]), race_dummies], axis=1)
        col_to_display.pop(q5_col, None)
        col_to_cluster.pop(q5_col, None)
        for dummy_col in race_dummies.columns:
            race_label = dummy_col.replace("q5_race_", "")
            col_to_display[dummy_col] = f"Race response: {race_label}"
            col_to_cluster[dummy_col] = "Demographics"

    categorical_cols = [c for c in df.columns if df[c].dtype == object]
    for col in categorical_cols:
        values = df[col].fillna("Missing").astype(str)
        df[col] = pd.factorize(values, sort=True)[0].astype(int)
        if col in col_to_display:
            col_to_display[col] = f"{col_to_display[col]} (encoded)"

    return df, col_to_display, col_to_cluster


def create_target(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str]:
    q26_col = find_q_col(df, 26)
    q84_col = find_q_col(df, 84)

    if not q26_col or not q84_col:
        raise ValueError("Không tìm thấy q26 hoặc q84 để tạo Target.")

    df = df.copy()
    if df[[q26_col, q84_col]].dropna(how="all").empty:
        raise ValueError("q26 và q84 đều trống nên không thể tạo Target.")

    q26 = pd.to_numeric(df[q26_col], errors="coerce")
    q84 = pd.to_numeric(df[q84_col], errors="coerce")
    target = ((q26 == 1) | (q84 >= 4)).astype(float)
    target[q26.isna() & q84.isna()] = np.nan
    df["Target"] = target
    df = df.dropna(subset=["Target"]).copy()
    if df.empty:
        raise ValueError("q26 va q84 deu thieu trong tat ca ban ghi nen khong the tao Target.")
    df["Target"] = df["Target"].astype(int)
    return df, q26_col, q84_col


def filter_student_focus_ages(df: pd.DataFrame) -> pd.DataFrame:
    q1_col = find_q_col(df, 1)
    if q1_col is None:
        return df.copy()
    age_code = pd.to_numeric(df[q1_col], errors="coerce")
    return df[age_code.isin(STUDENT_FOCUS_Q1_VALUES)].copy()


def _yes_is_risk(df: pd.DataFrame, qnum: int) -> Optional[pd.Series]:
    col = find_q_col(df, qnum)
    if not col:
        return None
    series = pd.to_numeric(df[col], errors="coerce")
    return series.map({1: 1.0, 2: 0.0})


def _ordinal_risk(
    df: pd.DataFrame,
    qnum: int,
    min_value: float,
    max_value: float,
    reverse: bool = False,
) -> Optional[pd.Series]:
    col = find_q_col(df, qnum)
    if not col or max_value <= min_value:
        return None
    series = pd.to_numeric(df[col], errors="coerce")
    risk = (series - min_value) / (max_value - min_value)
    if reverse:
        risk = 1 - risk
    return risk.clip(0, 1)


def _mapped_risk(df: pd.DataFrame, qnum: int, mapping: Dict[Any, float]) -> Optional[pd.Series]:
    col = find_q_col(df, qnum)
    if not col:
        return None
    series = pd.to_numeric(df[col], errors="coerce")
    return series.map(mapping)


def _mean_index(parts: List[Optional[pd.Series]], index: pd.Index) -> Optional[pd.Series]:
    valid_parts = [part for part in parts if part is not None]
    if not valid_parts:
        return None
    frame = pd.concat(valid_parts, axis=1)
    score = frame.mean(axis=1, skipna=True) * 100
    score[frame.notna().sum(axis=1) == 0] = np.nan
    score.index = index
    return score


def _add_research_construct(
    df: pd.DataFrame,
    name: str,
    score: Optional[pd.Series],
    col_to_display: Dict[str, str],
    col_to_cluster: Dict[str, str],
    derived_features: List[str],
) -> None:
    existing = pd.to_numeric(df[name], errors="coerce") if name in df.columns else None
    if score is None:
        if existing is None or not existing.notna().any():
            return
        df[name] = existing
    else:
        score = pd.to_numeric(score, errors="coerce")
        df[name] = score.combine_first(existing) if existing is not None else score
    df[name] = pd.to_numeric(df[name], errors="coerce").clip(0, 100)
    col_to_display[name] = name
    col_to_cluster[name] = RESEARCH_FEATURE_DEFINITIONS[name]["cluster"]
    if name not in derived_features:
        derived_features.append(name)


def add_research_construct_features(
    df: pd.DataFrame,
    col_to_display: Dict[str, str],
    col_to_cluster: Dict[str, str],
    derived_features: List[str],
) -> Tuple[pd.DataFrame, Dict[str, str], Dict[str, str], List[str]]:
    df = df.copy()
    col_to_display = dict(col_to_display)
    col_to_cluster = dict(col_to_cluster)
    derived_features = list(derived_features)

    family_pressure = _mean_index(
        [
            _ordinal_risk(df, 89, 1, 5),
            _ordinal_risk(df, 90, 1, 5),
            _ordinal_risk(df, 91, 1, 5),
            _ordinal_risk(df, 99, 1, 5, reverse=True),
            _yes_is_risk(df, 100),
            _yes_is_risk(df, 101),
            _yes_is_risk(df, 102),
            _ordinal_risk(df, 104, 1, 5, reverse=True),
        ],
        df.index,
    )
    _add_research_construct(df, "Family Pressure Index", family_pressure, col_to_display, col_to_cluster, derived_features)

    academic_pressure = _mean_index(
        [
            _ordinal_risk(df, 87, 1, 5),
            _ordinal_risk(df, 103, 1, 5),
            _yes_is_risk(df, 105),
            _yes_is_risk(df, 106),
        ],
        df.index,
    )
    _add_research_construct(df, "Academic Pressure Index", academic_pressure, col_to_display, col_to_cluster, derived_features)

    peer_safety_stress = _mean_index(
        [
            _ordinal_risk(df, 14, 1, 5),
            _ordinal_risk(df, 15, 1, 8),
            _yes_is_risk(df, 18),
            _yes_is_risk(df, 24),
            _yes_is_risk(df, 25),
        ],
        df.index,
    )
    _add_research_construct(df, "Peer & Safety Stress Index", peer_safety_stress, col_to_display, col_to_cluster, derived_features)

    trauma_exposure = _mean_index(
        [
            _yes_is_risk(df, 19),
            _mapped_risk(df, 20, {1: 0.0, 2: 0.25, 3: 0.5, 4: 0.75, 5: 1.0}),
            _mapped_risk(df, 21, {1: 0.0, 2: 0.0, 3: 0.25, 4: 0.5, 5: 0.75, 6: 1.0}),
            _mapped_risk(df, 22, {1: 0.0, 2: 0.0, 3: 0.25, 4: 0.5, 5: 0.75, 6: 1.0}),
            _yes_is_risk(df, 88),
        ],
        df.index,
    )
    _add_research_construct(df, "Trauma Exposure Index", trauma_exposure, col_to_display, col_to_cluster, derived_features)

    substance_coping = _mean_index(
        [
            _ordinal_risk(df, 33, 1, 7),
            _ordinal_risk(df, 36, 1, 7),
            _ordinal_risk(df, 42, 1, 7),
            _ordinal_risk(df, 43, 1, 6),
            _ordinal_risk(df, 48, 1, 7),
            _ordinal_risk(df, 92, 1, 6),
        ],
        df.index,
    )
    _add_research_construct(df, "Substance Coping Risk Index", substance_coping, col_to_display, col_to_cluster, derived_features)

    sleep_deficit = _mapped_risk(df, 85, {1: 1.0, 2: 0.75, 3: 0.45, 4: 0.05, 5: 0.0, 6: 0.25, 7: 0.55})
    lifestyle_deficit = _mean_index(
        [
            _ordinal_risk(df, 75, 1, 8, reverse=True),
            _ordinal_risk(df, 76, 1, 8, reverse=True),
            _ordinal_risk(df, 80, 1, 6),
            sleep_deficit,
            _ordinal_risk(df, 96, 1, 6, reverse=True),
        ],
        df.index,
    )
    _add_research_construct(df, "Lifestyle Recovery Deficit", lifestyle_deficit, col_to_display, col_to_cluster, derived_features)

    return df, col_to_display, col_to_cluster, derived_features


def add_derived_features(
    df: pd.DataFrame,
    col_to_display: Dict[str, str],
    col_to_cluster: Dict[str, str],
) -> Tuple[pd.DataFrame, Dict[str, str], Dict[str, str], List[str]]:
    df = df.copy()
    col_to_display = dict(col_to_display)
    col_to_cluster = dict(col_to_cluster)
    derived_features: List[str] = []

    c6, c7 = find_q_col(df, 6), find_q_col(df, 7)
    if c6 and c7:
        with np.errstate(divide="ignore", invalid="ignore"):
            df["BMI"] = df[c7] / (df[c6] ** 2)
        df.loc[~np.isfinite(df["BMI"]), "BMI"] = np.nan
        col_to_cluster["BMI"] = "Demographics"
        col_to_display["BMI"] = "BMI (computed)"
        derived_features.append("BMI")

    c87 = find_q_col(df, 87)
    if c87:
        academic_map = {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
        df["Academic Score"] = df[c87].map(academic_map)
        col_to_cluster["Academic Score"] = "School Climate & Academic Context"
        col_to_display["Academic Score"] = "Academic Score (A=5 to F=1)"
        derived_features.append("Academic Score")

    c103 = find_q_col(df, 103)
    if c103:
        df["School Connectedness"] = 6 - df[c103]
        col_to_cluster["School Connectedness"] = "School Climate & Academic Context"
        col_to_display["School Connectedness"] = "School Connectedness"
        derived_features.append("School Connectedness")

    lifestyle_parts: List[pd.Series] = []
    c85 = find_q_col(df, 85)
    if c85:
        sleep_map = {4: 1, 5: 1, 3: 0.7, 6: 0.7, 2: 0.4, 7: 0.4, 1: 0.1}
        lifestyle_parts.append(df[c85].map(sleep_map).fillna(0))

    c75 = find_q_col(df, 75)
    if c75:
        lifestyle_parts.append((df[c75] - 1) / 7)

    c76 = find_q_col(df, 76)
    if c76:
        lifestyle_parts.append((df[c76] - 1) / 7)

    c96 = find_q_col(df, 96)
    if c96:
        lifestyle_parts.append((df[c96] - 1) / 6)

    if lifestyle_parts:
        df["Healthy Lifestyle Score"] = sum(lifestyle_parts) / len(lifestyle_parts)
        col_to_cluster["Healthy Lifestyle Score"] = "Lifestyle Factors"
        col_to_display["Healthy Lifestyle Score"] = "Healthy Lifestyle Score"
        derived_features.append("Healthy Lifestyle Score")

    sub_cols = [find_q_col(df, n) for n in [33, 36, 42, 48, 92]]
    sub_cols = [c for c in sub_cols if c is not None]
    if sub_cols:
        sub_scores: List[pd.Series] = []
        for col in sub_cols:
            max_val = df[col].max()
            if pd.notna(max_val) and max_val > 0:
                sub_scores.append(df[col].fillna(0) / max_val)
        if sub_scores:
            df["Substance Use Risk"] = sum(sub_scores) / len(sub_scores)
            col_to_cluster["Substance Use Risk"] = "Substance Use"
            col_to_display["Substance Use Risk"] = "Substance Use Risk Score"
            derived_features.append("Substance Use Risk")

    ace_parts: List[pd.Series] = []
    for n in [89, 90, 91]:
        col = find_q_col(df, n)
        if col:
            ace_parts.append((df[col] > 1).astype(int))

    for n in [100, 101, 102]:
        col = find_q_col(df, n)
        if col:
            ace_parts.append((df[col] == 1).astype(int))

    if ace_parts:
        df["ACE Score"] = sum(ace_parts)
        col_to_cluster["ACE Score"] = "Family & ACEs"
        col_to_display["ACE Score"] = "ACE Score (0-6)"
        derived_features.append("ACE Score")

    return df, col_to_display, col_to_cluster, derived_features


def select_analysis_features(df: pd.DataFrame) -> List[str]:
    feature_cols: List[str] = []
    for qnum in DEMOGRAPHIC_MODEL_QNUMS:
        col = find_q_col(df, qnum)
        if col and pd.api.types.is_numeric_dtype(df[col]):
            feature_cols.append(col)

    for feature in RESEARCH_FEATURES:
        if feature in df.columns and pd.api.types.is_numeric_dtype(df[feature]):
            feature_cols.append(feature)

    return feature_cols


def preprocess_yrbs_data(df_raw: pd.DataFrame) -> ProcessedData:
    harmonized_raw, harmonized_dropped_cols = harmonize_survey_data(df_raw)
    cleaned_raw, dropped_meta_cols = clean_raw_dataframe(harmonized_raw)
    dropped_meta_cols = list(dict.fromkeys(harmonized_dropped_cols + dropped_meta_cols))
    cleaned_raw = filter_student_focus_ages(cleaned_raw)
    raw_col_to_display, raw_col_to_cluster = build_column_metadata(cleaned_raw)
    raw_analysis, q26_col, q84_col = create_target(cleaned_raw)
    encoded, col_to_display, col_to_cluster = encode_race_and_categoricals(
        raw_analysis,
        raw_col_to_display,
        raw_col_to_cluster,
    )
    engineered = encoded.copy()
    derived_features: List[str] = []
    engineered, col_to_display, col_to_cluster, derived_features = add_research_construct_features(
        engineered,
        col_to_display,
        col_to_cluster,
        derived_features,
    )
    feature_cols = select_analysis_features(engineered)
    feature_frame = engineered[feature_cols].copy()
    target = engineered["Target"].copy()

    return ProcessedData(
        raw=harmonized_raw.copy(),
        raw_analysis=raw_analysis,
        cleaned=engineered,
        feature_frame=feature_frame,
        target=target,
        feature_cols=feature_cols,
        col_to_display=col_to_display,
        col_to_cluster=col_to_cluster,
        raw_col_to_display=raw_col_to_display,
        raw_col_to_cluster=raw_col_to_cluster,
        dropped_meta_cols=dropped_meta_cols,
        target_q26_col=q26_col,
        target_q84_col=q84_col,
        derived_features=derived_features,
    )


def value_to_label(value: Any, qnum: Optional[int] = None) -> str:
    if pd.isna(value):
        return "Missing"
    normalized: Any = value
    if isinstance(value, (np.integer, int)):
        normalized = int(value)
    elif isinstance(value, (np.floating, float)) and float(value).is_integer():
        normalized = int(value)
    mapping = RESPONSE_LABELS.get(qnum or -1, {})
    if normalized in mapping:
        return mapping[normalized]
    if isinstance(normalized, int):
        return f"Mức phản hồi {normalized}"
    return str(normalized)


def values_to_labels(series: pd.Series, qnum: Optional[int] = None) -> pd.Series:
    """Label a survey series once per distinct response instead of once per row."""
    labels = {
        value: value_to_label(value, qnum)
        for value in series.dropna().drop_duplicates().tolist()
    }
    return series.map(labels).fillna("Missing")


def label_category(series: pd.Series, source_col: str) -> pd.Series:
    mapping = CATEGORY_LABELS.get(source_col, {})
    if not mapping:
        qnum = extract_qnum(source_col)
        return values_to_labels(series, qnum)
    return series.map(mapping).fillna(series.astype(str))


def explain_target_counts(df: pd.DataFrame) -> pd.DataFrame:
    counts = df["Target"].value_counts(dropna=False).sort_index()
    total = int(counts.sum())
    rows = []
    for target_value, count in counts.items():
        target_int = int(target_value) if pd.notna(target_value) else target_value
        target_label = CATEGORY_LABELS["Target"].get(target_int, str(target_value))
        rows.append(
            {
                "Target": target_label,
                "Count": int(count),
                "Percentage": round(float(count) / total * 100, 2) if total else 0.0,
            }
        )
    return pd.DataFrame(rows)


def raw_question_catalog(processed: ProcessedData) -> pd.DataFrame:
    rows = []
    df = processed.raw_analysis
    allowed_qnums = {
        26,
        84,
        *[
            qnum
            for definition in RESEARCH_FEATURE_DEFINITIONS.values()
            for qnum in definition["qnums"]
        ],
    }
    for col in df.columns:
        qnum = extract_qnum(col)
        if qnum is None or qnum not in allowed_qnums:
            continue
        series = df[col]
        if (
            POPULATION_COLUMN in df.columns
            and qnum not in {26, 84}
            and (df[POPULATION_COLUMN] == MENTAL_SCHOOL_POPULATION_LABEL).any()
        ):
            school_mask = df[POPULATION_COLUMN] == MENTAL_SCHOOL_POPULATION_LABEL
            if df.loc[school_mask, col].notna().any():
                series = df.loc[school_mask, col]
        total = len(series)
        missing = int(series.isna().sum())
        rows.append(
            {
                "qnum": qnum,
                "column": col,
                "question": processed.raw_col_to_display.get(col, col),
                "cluster": processed.raw_col_to_cluster.get(col, "Other"),
                "non_missing": int(series.notna().sum()),
                "missing": missing,
                "missing_pct": round(missing / total * 100, 2) if total else 0.0,
                "unique_valid_values": int(series.dropna().nunique()),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "qnum",
                "column",
                "question",
                "cluster",
                "non_missing",
                "missing",
                "missing_pct",
                "unique_valid_values",
            ]
        )
    return pd.DataFrame(rows).sort_values("qnum").reset_index(drop=True)


def question_frequency_table(
    df: pd.DataFrame,
    col: str,
    qnum: Optional[int] = None,
) -> pd.DataFrame:
    qnum = qnum if qnum is not None else extract_qnum(col)
    labels = values_to_labels(df[col], qnum)
    labels = labels[labels != "Missing"]
    total = len(labels)
    counts = labels.value_counts(dropna=False)
    result = counts.rename_axis("Response").reset_index(name="Count")
    result["Percentage"] = result["Count"].apply(lambda count: round(float(count) / total * 100, 2) if total else 0.0)
    result["Response"] = result["Response"].astype(str)
    return result.sort_values("Count", ascending=False).reset_index(drop=True)


def target_by_response_table(
    df: pd.DataFrame,
    col: str,
    qnum: Optional[int] = None,
) -> pd.DataFrame:
    if "Target" not in df.columns or col not in df.columns:
        return pd.DataFrame(columns=["Response", "Count", "At Risk Count", "At Risk Rate"])

    qnum = qnum if qnum is not None else extract_qnum(col)
    tmp = pd.DataFrame(
        {
            "Response": values_to_labels(df[col], qnum),
            "Target": df["Target"],
        }
    )
    tmp = tmp[tmp["Response"] != "Missing"].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["Response", "Count", "At Risk Count", "At Risk Rate"])
    result = (
        tmp.groupby("Response", dropna=False)["Target"]
        .agg(Count="size", At_Risk_Count="sum", At_Risk_Rate="mean")
        .reset_index()
    )
    result = result.rename(columns={"At_Risk_Count": "At Risk Count", "At_Risk_Rate": "At Risk Rate"})
    result["At Risk Rate"] = (result["At Risk Rate"] * 100).round(2)
    return result.sort_values(["At Risk Rate", "Count"], ascending=[False, False]).reset_index(drop=True)


def cluster_overview_table(processed: ProcessedData) -> pd.DataFrame:
    catalog = raw_question_catalog(processed)
    if catalog.empty:
        return pd.DataFrame(columns=["cluster", "Questions", "Average Missing (%)", "Average Coverage (%)"])
    result = (
        catalog.groupby("cluster", as_index=False)
        .agg(
            Questions=("column", "count"),
            **{
                "Average Missing (%)": ("missing_pct", "mean"),
                "Average Unique Responses": ("unique_valid_values", "mean"),
            },
        )
        .sort_values("Questions", ascending=False)
    )
    result["Average Missing (%)"] = result["Average Missing (%)"].round(2)
    result["Average Coverage (%)"] = (100 - result["Average Missing (%)"]).round(2)
    result["Average Unique Responses"] = result["Average Unique Responses"].round(2)
    return result.reset_index(drop=True)


def top_missing_questions(processed: ProcessedData, top_n: int = 15) -> pd.DataFrame:
    catalog = raw_question_catalog(processed)
    if catalog.empty:
        return catalog
    return catalog.sort_values("missing_pct", ascending=False).head(top_n).reset_index(drop=True)


def top_target_gap_questions(
    processed: ProcessedData,
    top_n: int = 15,
    min_category_n: int = 40,
    df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    rows = []
    df = processed.raw_analysis if df is None else df
    excluded_qnums = {26, 27, 28, 29, 30, 84}
    allowed_qnums = {
        qnum
        for definition in RESEARCH_FEATURE_DEFINITIONS.values()
        for qnum in definition["qnums"]
    }
    for col in df.columns:
        qnum = extract_qnum(col)
        if qnum is None or qnum in excluded_qnums or qnum not in allowed_qnums:
            continue
        tmp = df[[col, "Target"]].dropna(subset=[col, "Target"]).copy()
        if tmp.empty:
            continue
        grouped = (
            tmp.groupby(col)["Target"]
            .agg(rate="mean", count="size")
            .reset_index()
        )
        grouped = grouped[grouped["count"] >= min_category_n]
        if len(grouped) < 2:
            continue
        max_row = grouped.loc[grouped["rate"].idxmax()]
        min_row = grouped.loc[grouped["rate"].idxmin()]
        gap = float(max_row["rate"] - min_row["rate"]) * 100
        rows.append(
            {
                "qnum": qnum,
                "question": processed.raw_col_to_display.get(col, col),
                "cluster": processed.raw_col_to_cluster.get(col, "Other"),
                "At Risk Gap (%)": round(gap, 2),
                "Highest-risk response": value_to_label(max_row[col], qnum),
                "Highest-risk rate (%)": round(float(max_row["rate"]) * 100, 2),
                "Lowest-risk response": value_to_label(min_row[col], qnum),
                "Lowest-risk rate (%)": round(float(min_row["rate"]) * 100, 2),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "qnum",
                "question",
                "cluster",
                "At Risk Gap (%)",
                "Highest-risk response",
                "Highest-risk rate (%)",
                "Lowest-risk response",
                "Lowest-risk rate (%)",
            ]
        )
    return pd.DataFrame(rows).sort_values("At Risk Gap (%)", ascending=False).head(top_n).reset_index(drop=True)


def cluster_gap_summary(processed: ProcessedData) -> pd.DataFrame:
    gap = top_target_gap_questions(processed, top_n=5000, min_category_n=40)
    if gap.empty:
        return pd.DataFrame(columns=["cluster", "Questions Analysed", "Average Gap (%)", "Max Gap (%)"])
    result = (
        gap.groupby("cluster", as_index=False)
        .agg(
            **{
                "Questions Analysed": ("question", "count"),
                "Average Gap (%)": ("At Risk Gap (%)", "mean"),
                "Max Gap (%)": ("At Risk Gap (%)", "max"),
            }
        )
        .sort_values("Average Gap (%)", ascending=False)
    )
    result["Average Gap (%)"] = result["Average Gap (%)"].round(2)
    result["Max Gap (%)"] = result["Max Gap (%)"].round(2)
    return result.reset_index(drop=True)


def compact_data_quality_summary(df_raw: pd.DataFrame, processed: ProcessedData) -> pd.DataFrame:
    rows = [
        ("Rows loaded", int(df_raw.shape[0])),
        ("Analysable rows after target validation", int(processed.raw_analysis.shape[0])),
        ("Raw columns loaded", int(df_raw.shape[1])),
        ("Columns after preprocessing", int(processed.cleaned.shape[1])),
        ("Research-scope survey questions", int(len(raw_question_catalog(processed)))),
        ("Model features", int(len(processed.feature_cols))),
        ("Research constructs", int(len(processed.derived_features))),
        ("Missing cells after preprocessing", int(processed.cleaned.isna().sum().sum())),
        (
            "Dropped metadata/survey design columns",
            ", ".join(processed.dropped_meta_cols) if processed.dropped_meta_cols else "None",
        ),
    ]
    if POPULATION_COLUMN in processed.raw_analysis.columns:
        counts = processed.raw_analysis[POPULATION_COLUMN].value_counts(dropna=False)
        for population, count in counts.items():
            rows.append((f"Rows - {population}", int(count)))
    if DATA_SOURCE_COLUMN in processed.raw_analysis.columns:
        source_count = int(processed.raw_analysis[DATA_SOURCE_COLUMN].nunique(dropna=True))
        rows.append(("Data sources", source_count))
    if HMS_AGE_OUTLIER_COLUMN in processed.raw_analysis.columns:
        rows.append(("HMS age outliers set to missing", int(processed.raw_analysis[HMS_AGE_OUTLIER_COLUMN].sum())))
    if HMS_SCOPED_MISSING_PCT_COLUMN in processed.raw_analysis.columns:
        hms_mask = (
            processed.raw_analysis[POPULATION_COLUMN] == HMS_POPULATION_LABEL
            if POPULATION_COLUMN in processed.raw_analysis.columns
            else processed.raw_analysis[HMS_SCOPED_MISSING_PCT_COLUMN].notna()
        )
        hms_missing = processed.raw_analysis.loc[hms_mask, HMS_SCOPED_MISSING_PCT_COLUMN]
        if hms_missing.notna().any():
            rows.append(("Average HMS scoped missing (%)", round(float(hms_missing.mean()), 2)))
    return pd.DataFrame(rows, columns=["Metric", "Value"])


def hms_data_quality_summary(processed: ProcessedData) -> pd.DataFrame:
    columns = [
        "Data Source",
        "Rows",
        "At Risk Rate (%)",
        "Average Scoped Missing (%)",
        "Age Coverage (%)",
        "Age Outliers Set Missing",
        "Construct Coverage (%)",
    ]
    if POPULATION_COLUMN not in processed.raw_analysis.columns:
        return pd.DataFrame(columns=columns)

    hms = processed.raw_analysis[processed.raw_analysis[POPULATION_COLUMN] == HMS_POPULATION_LABEL]
    if hms.empty:
        return pd.DataFrame(columns=columns)

    rows = []
    source_col = DATA_SOURCE_COLUMN if DATA_SOURCE_COLUMN in hms.columns else POPULATION_COLUMN
    for source, group in hms.groupby(source_col, dropna=False):
        idx = group.index
        construct_cols = [feature for feature in RESEARCH_FEATURES if feature in processed.cleaned.columns]
        construct_coverage = (
            processed.cleaned.loc[idx, construct_cols].notna().mean().mean() * 100
            if construct_cols
            else np.nan
        )
        rows.append(
            {
                "Data Source": source,
                "Rows": int(len(group)),
                "At Risk Rate (%)": round(float(group["Target"].mean()) * 100, 2) if "Target" in group.columns else np.nan,
                "Average Scoped Missing (%)": round(float(group[HMS_SCOPED_MISSING_PCT_COLUMN].mean()), 2)
                if HMS_SCOPED_MISSING_PCT_COLUMN in group.columns and group[HMS_SCOPED_MISSING_PCT_COLUMN].notna().any()
                else np.nan,
                "Age Coverage (%)": round(float(group[HMS_AGE_COLUMN].notna().mean()) * 100, 2)
                if HMS_AGE_COLUMN in group.columns
                else np.nan,
                "Age Outliers Set Missing": int(group[HMS_AGE_OUTLIER_COLUMN].sum())
                if HMS_AGE_OUTLIER_COLUMN in group.columns
                else 0,
                "Construct Coverage (%)": round(float(construct_coverage), 2) if pd.notna(construct_coverage) else np.nan,
            }
        )

    return pd.DataFrame(rows, columns=columns).sort_values("Data Source").reset_index(drop=True)


def target_prevalence_by_group(
    df: pd.DataFrame,
    qnum: int,
    display_name: str,
) -> pd.DataFrame:
    col = find_q_col(df, qnum)
    if not col or "Target" not in df.columns:
        return pd.DataFrame(columns=[display_name, "At Risk Rate", "n"])

    group = pd.DataFrame(
        {
            display_name: values_to_labels(df[col], qnum),
            "Target": df["Target"],
        }
    )
    group = group[group[display_name] != "Missing"].copy()
    if group.empty:
        return pd.DataFrame(columns=[display_name, "At Risk Rate", "n"])
    out = (
        group.groupby(display_name, as_index=False)
        .agg(**{"At Risk Rate": ("Target", "mean"), "n": ("Target", "size")})
        .sort_values("At Risk Rate", ascending=False)
    )
    out["At Risk Rate"] = (out["At Risk Rate"] * 100).round(2)
    return out.reset_index(drop=True)


def derived_score_summary(df: pd.DataFrame, derived_features: List[str]) -> pd.DataFrame:
    rows = []
    for feature in derived_features:
        if feature not in df.columns:
            continue
        series = df[feature]
        rows.append(
            {
                "Feature": feature,
                "Available n": int(series.notna().sum()),
                "Mean": float(series.mean()) if series.notna().any() else np.nan,
                "Median": float(series.median()) if series.notna().any() else np.nan,
                "Std": float(series.std()) if series.notna().any() else np.nan,
                "Min": float(series.min()) if series.notna().any() else np.nan,
                "Max": float(series.max()) if series.notna().any() else np.nan,
            }
        )
    return pd.DataFrame(rows)


def target_prevalence_by_score_bins(
    df: pd.DataFrame,
    score_col: str,
    bins: int = 5,
) -> pd.DataFrame:
    if score_col not in df.columns or "Target" not in df.columns:
        return pd.DataFrame(columns=["Score Bin", "At Risk Rate", "n"])
    tmp = df[[score_col, "Target"]].dropna(subset=[score_col, "Target"]).copy()
    if tmp.empty:
        return pd.DataFrame(columns=["Score Bin", "At Risk Rate", "n"])
    if tmp[score_col].nunique() <= bins:
        tmp["Score Bin"] = tmp[score_col].round(2).astype(str)
    else:
        try:
            tmp["Score Bin"] = pd.qcut(tmp[score_col], q=bins, duplicates="drop").astype(str)
        except ValueError:
            tmp["Score Bin"] = pd.cut(tmp[score_col], bins=bins, duplicates="drop").astype(str)
    out = (
        tmp.groupby("Score Bin", observed=False, as_index=False)
        .agg(**{"At Risk Rate": ("Target", "mean"), "n": ("Target", "size")})
    )
    out["At Risk Rate"] = (out["At Risk Rate"] * 100).round(2)
    return out


def get_demographic_frame(df: pd.DataFrame) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    for qnum, name in [(1, "Age"), (2, "Gender"), (3, "Grade")]:
        col = find_q_col(df, qnum)
        if col:
            result[name] = values_to_labels(df[col], qnum)
    if "Target" in df.columns:
        result["Target"] = df["Target"].map(CATEGORY_LABELS["Target"]).fillna(df["Target"].astype(str))
        result["TargetValue"] = df["Target"]
    return result


def apply_description_filters(
    df: pd.DataFrame,
    populations: Optional[List[str]] = None,
    sources: Optional[List[str]] = None,
    ages: Optional[List[str]] = None,
    genders: Optional[List[str]] = None,
    grades: Optional[List[str]] = None,
    targets: Optional[List[str]] = None,
) -> pd.DataFrame:
    filtered = df.copy()
    q1 = find_q_col(filtered, 1)
    q2 = find_q_col(filtered, 2)
    q3 = find_q_col(filtered, 3)

    if populations and POPULATION_COLUMN in filtered.columns:
        filtered = filtered[filtered[POPULATION_COLUMN].astype(str).isin(populations)]
    if sources and DATA_SOURCE_COLUMN in filtered.columns:
        filtered = filtered[filtered[DATA_SOURCE_COLUMN].astype(str).isin(sources)]
    if ages and q1:
        filtered = filtered[values_to_labels(filtered[q1], 1).isin(ages)]
    if genders and q2:
        filtered = filtered[values_to_labels(filtered[q2], 2).isin(genders)]
    if grades and q3:
        filtered = filtered[values_to_labels(filtered[q3], 3).isin(grades)]
    if targets and "Target" in filtered.columns:
        target_labels = filtered["Target"].map(CATEGORY_LABELS["Target"]).fillna(filtered["Target"].astype(str))
        filtered = filtered[target_labels.isin(targets)]
    return filtered.copy()
