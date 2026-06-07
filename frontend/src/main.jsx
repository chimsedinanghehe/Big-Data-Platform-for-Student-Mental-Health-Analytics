import "./App.css";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";
const DASHBOARD_URL = import.meta.env.VITE_DASHBOARD_URL || "http://127.0.0.1:8501";
const SESSION_KEY = "student-support-chat-session";
const AUTH_TOKEN_KEY = "student-platform-auth-token";
const SURVEY_DRAFT_KEY_PREFIX = "student-platform-survey-draft-v1";

const state = {
  activeView: "dashboard",
  authMode: "login",
  authChecking: false,
  authToken: "",
  currentUser: null,
  messages: [
    {
      id: "welcome",
      role: "assistant",
      content:
        "Hi. Ask a student wellbeing question and I will answer using the trusted knowledge base when relevant.",
    },
  ],
  input: "",
  isLoading: false,
  error: "",
  sessionId: "",
  auth: {
    email: "",
    password: "",
    displayName: "",
    role: "student",
    status: "",
    error: "",
    fieldErrors: {},
    focusField: "",
    isSaving: false,
    studentProfile: {
      age: "",
      birthDate: "",
      gender: "other",
      learnerType: "university",
    },
  },
  survey: {
    status: null,
    questions: [],
    answers: {},
    currentIndex: 0,
    fieldErrors: {},
    isLoading: false,
    isSubmitting: false,
    error: "",
    statusMessage: "",
    promptDismissed: false,
  },
};

const SURVEY_QUESTION_LABELS = {
  gender: "Giới tính của bạn là gì?",
  school_grade: "Bạn đang học lớp mấy?",
  university_year: "Bạn đang học năm mấy?",
  school_sad_hopeless_2weeks: "Trong 12 tháng qua, bạn có từng cảm thấy buồn bã hoặc tuyệt vọng gần như mỗi ngày trong 2 tuần liên tiếp, đến mức dừng một số hoạt động thường ngày không?",
  school_poor_mental_health_days: "Trong 30 ngày qua, sức khỏe tinh thần của bạn không tốt thường xuyên như thế nào? Bao gồm căng thẳng, lo âu và trầm cảm.",
  school_suicide_ideation: "Trong 12 tháng qua, bạn có từng nghiêm túc nghĩ đến việc tự tử không?",
  school_suicide_plan: "Trong 12 tháng qua, bạn có từng lập kế hoạch về cách bạn sẽ tự tử không?",
  school_suicide_attempt: "Trong 12 tháng qua, bạn đã thực sự cố gắng tự tử bao nhiêu lần?",
  school_unsafe_absence: "Trong 30 ngày qua, có bao nhiêu ngày bạn không đi học vì cảm thấy không an toàn ở trường hoặc trên đường đến/về trường?",
  school_weapon_threat: "Trong 12 tháng qua, có bao nhiêu lần bạn bị đe dọa hoặc làm bị thương bằng vũ khí như súng, dao hoặc gậy ở trường?",
  school_physical_fight: "Trong 12 tháng qua, bạn đã tham gia đánh nhau bao nhiêu lần?",
  school_bullied_school: "Trong 12 tháng qua, bạn có từng bị bắt nạt tại trường không?",
  school_bullied_online: "Trong 12 tháng qua, bạn có từng bị bắt nạt trực tuyến không? Bao gồm qua tin nhắn, Instagram, Facebook hoặc mạng xã hội khác.",
  school_forced_sexual_contact: "Bạn có từng bị ép quan hệ tình dục khi bạn không muốn không?",
  school_dating_sexual_violence: "Trong 12 tháng qua, có bao nhiêu lần người bạn đang hẹn hò ép bạn làm những việc tình dục mà bạn không muốn?",
  school_dating_physical_violence: "Trong 12 tháng qua, có bao nhiêu lần người bạn đang hẹn hò cố ý làm bạn đau về thể chất?",
  school_adult_verbal_abuse: "Trong đời bạn, cha mẹ hoặc người lớn trong nhà có thường xuyên xúc phạm hoặc hạ thấp bạn không?",
  school_adult_physical_abuse: "Trong đời bạn, cha mẹ hoặc người lớn trong nhà có từng đánh, đá, đạp hoặc làm đau bạn về thể chất không?",
  school_family_violence_witness: "Trong đời bạn, cha mẹ hoặc người lớn trong nhà có từng đánh, đá, đạp hoặc đánh nhau với nhau không?",
  school_basic_needs_met: "Trong đời bạn, có người lớn trong nhà cố gắng đảm bảo các nhu cầu cơ bản của bạn như an toàn, quần áo sạch và đủ ăn không?",
  school_parent_monitoring: "Cha mẹ hoặc người lớn trong gia đình có thường biết bạn đi đâu hoặc đi với ai không?",
  school_academic_performance: "Trong 12 tháng qua, bạn mô tả điểm số của mình ở trường như thế nào?",
  school_school_belonging: "Bạn đồng ý hay không đồng ý rằng bạn cảm thấy gần gũi với mọi người ở trường?",
  school_unfair_discipline: "Trong 12 tháng qua, bạn có bị kỷ luật không công bằng ở trường không?",
  school_concentration_difficulty: "Vì vấn đề thể chất, tinh thần hoặc cảm xúc, bạn có gặp khó khăn nghiêm trọng trong việc tập trung, ghi nhớ hoặc ra quyết định không?",
  school_smoking_current: "Trong 30 ngày qua, bạn đã hút thuốc lá vào bao nhiêu ngày?",
  school_vaping_current: "Trong 30 ngày qua, bạn đã dùng sản phẩm vape hoặc thuốc lá điện tử vào bao nhiêu ngày?",
  school_alcohol_current: "Trong 30 ngày qua, bạn đã uống ít nhất một ly rượu hoặc bia vào bao nhiêu ngày?",
  school_sleep_hours: "Trung bình vào một đêm có đi học, bạn ngủ bao nhiêu giờ?",
  school_physical_activity_days: "Trong 7 ngày qua, có bao nhiêu ngày bạn vận động thể chất tổng cộng ít nhất 60 phút mỗi ngày?",
  school_breakfast_frequency: "Trong 7 ngày qua, có bao nhiêu ngày bạn ăn sáng?",
  uni_depression_score: "Trong 2 tuần gần đây, mức độ triệu chứng trầm cảm của bạn như thế nào?",
  uni_anxiety_score: "Trong 2 tuần gần đây, mức độ triệu chứng lo âu của bạn như thế nào?",
  uni_suicide_ideation: "Trong 12 tháng qua, bạn có từng nghĩ đến việc tự tử không?",
  uni_suicide_plan: "Trong 12 tháng qua, bạn có từng lập kế hoạch tự tử không?",
  uni_suicide_attempt: "Trong 12 tháng qua, bạn có từng cố gắng tự tử không?",
  uni_financial_current: "Hiện tại tình hình tài chính của bạn như thế nào?",
  uni_food_worry: "Bạn có lo lắng về việc không đủ tiền mua thực phẩm hoặc đồ ăn không?",
  uni_housing_worry: "Bạn có lo lắng về chỗ ở hoặc tình trạng nhà ở không ổn định không?",
  uni_payment_worry: "Bạn có lo lắng về việc chi trả học phí hoặc chi phí học tập không?",
  uni_academic_impairment: "Việc học ảnh hưởng tiêu cực đến đời sống hoặc sức khỏe của bạn ở mức nào?",
  uni_academic_stress: "Bạn cảm thấy căng thẳng học tập ở mức nào?",
  uni_competition_pressure: "Bạn có cảm thấy áp lực cạnh tranh thành tích với người khác không?",
  uni_imposter_feeling: "Bạn có thường cảm thấy mình không đủ giỏi hoặc không xứng đáng với thành tích hiện tại không?",
  uni_failed_course: "Bạn có từng trượt môn hoặc có nguy cơ trượt môn không?",
  uni_time_management: "Bạn có gặp khó khăn trong quản lý thời gian học tập không?",
  uni_belonging: "Bạn có cảm thấy thuộc về và được kết nối với môi trường đại học không?",
  uni_discrimination: "Bạn có từng bị phân biệt đối xử trong môi trường học không?",
  uni_campus_safety: "Bạn cảm thấy an toàn trong khuôn viên trường ở mức nào?",
  uni_hostile_climate: "Bạn có cảm thấy môi trường học có tính thù địch hoặc gây áp lực xã hội không?",
  uni_abuse_experience: "Bạn có từng trải qua bạo hành hoặc tổn hại từ người khác không?",
  uni_stalking_experience: "Bạn có từng bị theo dõi hoặc quấy rối dai dẳng không?",
  uni_sexual_assault: "Bạn có từng trải qua bạo lực hoặc xâm hại tình dục không?",
  uni_partner_harm: "Bạn có từng bị tổn hại, đe dọa hoặc bạo lực từ người yêu hoặc bạn đời không?",
  uni_binge_drinking_frequency: "Tần suất uống rượu hoặc bia quá mức của bạn như thế nào?",
  uni_substance_any: "Bạn có sử dụng chất kích thích nào khác không?",
  uni_smoking_or_vaping: "Bạn có hút thuốc hoặc sử dụng vape không?",
  uni_weekday_sleep_hours: "Trung bình ngày thường bạn ngủ bao nhiêu giờ mỗi đêm?",
  uni_weekend_sleep_hours: "Trung bình cuối tuần bạn ngủ bao nhiêu giờ mỗi đêm?",
  uni_exercise_frequency: "Bạn vận động thể chất thường xuyên như thế nào?",
};

const SURVEY_OPTION_LABELS = {
  "12 hoac nho hon": "12 tuổi hoặc nhỏ hơn",
  "18 hoac lon hon": "18 tuổi hoặc lớn hơn",
  Nu: "Nữ",
  Nam: "Nam",
  "Khac / khong muon tra loi": "Khác / không muốn trả lời",
  "Phi nhi nguyen / genderqueer": "Phi nhị nguyên / genderqueer",
  "Chuyen gioi": "Chuyển giới",
  "Khac / khong chac": "Khác / không chắc",
  "Nam 1": "Năm 1",
  "Nam 2": "Năm 2",
  "Nam 3": "Năm 3",
  "Nam 4": "Năm 4",
  "Nam 5 tro len": "Năm 5 trở lên",
  "Sau dai hoc / chuyen nghiep": "Sau đại học / chuyên nghiệp",
  "Lop 9": "Lớp 9",
  "Lop 10": "Lớp 10",
  "Lop 11": "Lớp 11",
  "Lop 12": "Lớp 12",
  Yes: "Có",
  No: "Không",
  "0: No": "0: Không",
  "1: Yes": "1: Có",
  Never: "Không bao giờ",
  Rarely: "Hiếm khi",
  Sometimes: "Thỉnh thoảng",
  "Most of the time": "Hầu hết thời gian",
  Always: "Luôn luôn",
  "Not sure": "Không chắc",
  "Strongly agree": "Hoàn toàn đồng ý",
  Agree: "Đồng ý",
  Disagree: "Không đồng ý",
  "Strongly disagree": "Hoàn toàn không đồng ý",
  "Mostly A's": "Chủ yếu điểm A",
  "Mostly B's": "Chủ yếu điểm B",
  "Mostly C's": "Chủ yếu điểm C",
  "Mostly D's": "Chủ yếu điểm D",
  "Mostly F's": "Chủ yếu điểm F",
  "None of these grades": "Không thuộc các mức điểm trên",
  "I did not date or go out with anyone during the past 12 months": "Tôi không hẹn hò với ai trong 12 tháng qua",
  "All 30 days": "Cả 30 ngày",
  "0-4: Rat thap / khong dang ke": "0-4: Rất thấp / không đáng kể",
  "5-9: Nhe": "5-9: Nhẹ",
  "10-14: Vua": "10-14: Vừa",
  "15-19: Kha nang": "15-19: Khá nặng",
  "20-27: Cao": "20-27: Cao",
  "15-21: Cao": "15-21: Cao",
};

const UNIVERSITY_SCALE_LABELS = {
  uni_depression_score: ["Rất thấp / không đáng kể", "Nhẹ", "Vừa", "Khá nặng", "Cao"],
  uni_anxiety_score: ["Rất thấp / không đáng kể", "Nhẹ", "Vừa", "Cao"],
  uni_financial_current: ["Rất khó khăn", "Khá khó khăn", "Tạm đủ chi tiêu", "Khá ổn định", "Rất ổn định"],
  uni_food_worry: ["Không lo lắng", "Đôi khi lo lắng", "Thường xuyên lo lắng"],
  uni_housing_worry: ["Không lo lắng", "Đôi khi lo lắng", "Thường xuyên lo lắng"],
  uni_payment_worry: ["Rất lo lắng", "Khá lo lắng", "Hơi lo lắng", "Ít lo lắng", "Hầu như không lo lắng", "Không lo lắng"],
  uni_academic_impairment: ["Không ảnh hưởng", "Ảnh hưởng nhẹ", "Ảnh hưởng vừa", "Ảnh hưởng nghiêm trọng"],
  uni_academic_stress: ["Không căng thẳng", "Ít căng thẳng", "Căng thẳng vừa", "Khá căng thẳng", "Rất căng thẳng"],
  uni_competition_pressure: ["Rất áp lực", "Khá áp lực", "Áp lực vừa", "Ít áp lực", "Không áp lực"],
  uni_imposter_feeling: ["Không bao giờ", "Hiếm khi", "Thỉnh thoảng", "Thường xuyên", "Gần như luôn luôn"],
  uni_time_management: ["Không khó khăn", "Rất ít khó khăn", "Ít khó khăn", "Khó khăn vừa", "Khá khó khăn", "Rất khó khăn"],
  uni_belonging: ["Rất gắn kết", "Khá gắn kết", "Hơi gắn kết", "Ít gắn kết", "Hầu như không gắn kết", "Hoàn toàn không gắn kết"],
  uni_campus_safety: ["Rất an toàn", "Khá an toàn", "Hơi an toàn", "Hơi không an toàn", "Khá không an toàn", "Rất không an toàn"],
  uni_hostile_climate: ["Không bao giờ", "Hiếm khi", "Thỉnh thoảng", "Thường xuyên", "Gần như luôn luôn"],
  uni_abuse_experience: ["Không bao giờ", "Một lần", "Một vài lần", "Nhiều lần", "Rất nhiều lần"],
  uni_sexual_assault: ["Không bao giờ", "Một lần", "Hai lần", "Ba lần trở lên"],
  uni_binge_drinking_frequency: ["Không bao giờ", "Ít hơn mỗi tháng một lần", "Khoảng mỗi tháng một lần", "Hai đến ba lần mỗi tháng", "Khoảng mỗi tuần một lần", "Nhiều lần mỗi tuần"],
  uni_weekday_sleep_hours: ["Dưới 5 giờ", "Khoảng 5 giờ", "Khoảng 6 giờ", "Khoảng 7 giờ", "Khoảng 8 giờ", "Khoảng 9 giờ", "Khoảng 10 giờ", "Khoảng 11 giờ", "12 giờ trở lên"],
  uni_weekend_sleep_hours: ["Dưới 5 giờ", "Khoảng 5 giờ", "Khoảng 6 giờ", "Khoảng 7 giờ", "Khoảng 8 giờ", "Khoảng 9 giờ", "Khoảng 10 giờ", "Khoảng 11 giờ", "12 giờ trở lên"],
  uni_exercise_frequency: ["Không vận động", "Ít hơn một lần mỗi tuần", "Một lần mỗi tuần", "Hai đến ba lần mỗi tuần", "Bốn đến năm lần mỗi tuần", "Gần như mỗi ngày"],
};

const UNIVERSITY_SMOKING_LABELS = {
  No: "Không sử dụng",
  Yes: "Có sử dụng",
  "Frequency level 1": "Ít hơn mỗi tháng một lần",
  "Frequency level 2": "Khoảng mỗi tháng một lần",
  "Frequency level 3": "Khoảng mỗi tuần một lần",
  "Frequency level 4": "Nhiều lần mỗi tuần",
  "Frequency level 5": "Hằng ngày",
};

function surveyQuestionLabel(question) {
  return SURVEY_QUESTION_LABELS[question.id] || question.question || question.prompt || question.id;
}

function surveyOptionLabel(question, value) {
  const universityScale = UNIVERSITY_SCALE_LABELS[question.id];
  const scalePosition = universityScale
    ? (question.id === "uni_depression_score" || question.id === "uni_anxiety_score"
        ? question.options.indexOf(value) + 1
        : Number.parseInt(String(value).split(":", 1)[0], 10))
    : Number.NaN;
  if (universityScale && Number.isInteger(scalePosition) && universityScale[scalePosition - 1]) {
    return universityScale[scalePosition - 1];
  }
  if (question.id.startsWith("uni_") && (value === "0: No" || value === "No")) {
    return "Không";
  }
  if (question.id.startsWith("uni_") && (value === "1: Yes" || value === "Yes")) {
    return "Có";
  }
  if (question.id === "uni_smoking_or_vaping" && UNIVERSITY_SMOKING_LABELS[value]) {
    return UNIVERSITY_SMOKING_LABELS[value];
  }
  if (SURVEY_OPTION_LABELS[value]) {
    return SURVEY_OPTION_LABELS[value];
  }
  return String(value)
    .replace(/^(\d+) times$/, "$1 lần")
    .replace(/^(\d+) time$/, "$1 lần")
    .replace(/^(\d+) or (\d+) times$/, "$1 hoặc $2 lần")
    .replace(/^(\d+) or more times$/, "$1 lần trở lên")
    .replace(/^(\d+) days$/, "$1 ngày")
    .replace(/^(\d+) day$/, "$1 ngày")
    .replace(/^(\d+) or (\d+) days$/, "$1 hoặc $2 ngày")
    .replace(/^(\d+) or more days$/, "$1 ngày trở lên")
    .replace(/^(\d+) to (\d+) days$/, "$1 đến $2 ngày")
    .replace(/^(\d+) hours$/, "$1 giờ")
    .replace(/^(\d+) hour$/, "$1 giờ")
    .replace(/^(\d+) or more hours$/, "$1 giờ trở lên")
    .replace(/^(\d+) or less hours$/, "$1 giờ hoặc ít hơn")
    .replace(/^Frequency level (\d+)$/, "Mức tần suất $1")
    .replace(/^(\d+): Muc (\d+)$/, "$1: Mức $2");
}

function surveyDraftKey(surveyType = state.survey.status?.survey_type) {
  if (!state.currentUser?.id || !surveyType) {
    return "";
  }
  return `${SURVEY_DRAFT_KEY_PREFIX}:${state.currentUser.id}:${surveyType}`;
}

function persistSurveyDraft() {
  const key = surveyDraftKey();
  if (!key || isSurveyCompleted() || !state.survey.questions.length) {
    return;
  }
  localStorage.setItem(key, JSON.stringify({
    surveyType: state.survey.status?.survey_type,
    answers: state.survey.answers,
    currentIndex: state.survey.currentIndex,
    updatedAt: new Date().toISOString(),
  }));
}

function restoreSurveyDraft() {
  const key = surveyDraftKey();
  if (!key) {
    return;
  }
  try {
    const draft = JSON.parse(localStorage.getItem(key) || "null");
    if (!draft || draft.surveyType !== state.survey.status?.survey_type) {
      return;
    }
    const questionsById = new Map(state.survey.questions.map((question) => [question.id, question]));
    const restoredAnswers = {};
    for (const [questionId, answer] of Object.entries(draft.answers || {})) {
      const question = questionsById.get(questionId);
      if (!question) {
        continue;
      }
      if (question.input_type === "date" || (question.options || []).includes(answer)) {
        restoredAnswers[questionId] = answer;
      }
    }
    state.survey.answers = restoredAnswers;
    state.survey.currentIndex = Math.min(
      Math.max(Number(draft.currentIndex) || 0, 0),
      Math.max(state.survey.questions.length - 1, 0),
    );
  } catch {
    localStorage.removeItem(key);
  }
}

function clearSurveyDraft(surveyType = state.survey.status?.survey_type) {
  const key = surveyDraftKey(surveyType);
  if (key) {
    localStorage.removeItem(key);
  }
}

function createSessionId() {
  const browserCrypto = globalThis.crypto;
  if (browserCrypto?.randomUUID) {
    return browserCrypto.randomUUID();
  }
  return `session-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function initializeSession() {
  const stored = localStorage.getItem(SESSION_KEY);
  state.sessionId = stored || createSessionId();
  localStorage.setItem(SESSION_KEY, state.sessionId);

  state.authToken = localStorage.getItem(AUTH_TOKEN_KEY) || "";
  if (state.authToken) {
    state.authChecking = true;
    loadCurrentUser();
  }
}

function render() {
  const root = document.getElementById("root");
  root.innerHTML = "";

  const main = el("main", {
    className: state.currentUser ? "app-shell app-main-shell" : "app-shell auth-main-shell",
  });
  if (!state.currentUser) {
    main.appendChild(renderAuthWorkspace());
  } else {
    main.appendChild(renderWorkspace());
  }

  root.appendChild(main);
  if (shouldShowSurveyPrompt()) {
    root.appendChild(renderSurveyPrompt());
  }
  if (state.survey.statusMessage) {
    root.appendChild(renderToast(state.survey.statusMessage));
  }

  const messageList = root.querySelector(".message-list");
  if (messageList) {
    messageList.scrollTop = messageList.scrollHeight;
  }

  if (state.auth.focusField) {
    const field = root.querySelector(`[data-field="${state.auth.focusField}"]`);
    if (field) {
      field.focus();
      field.scrollIntoView({ block: "center", behavior: "smooth" });
    }
    state.auth.focusField = "";
  }
}

function renderAuthWorkspace() {
  const workspace = el("section", { className: "workspace auth-workspace" });
  const header = el("header", { className: "app-header auth-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h1", {}, "Student Mental Health Platform"));
  titleWrap.appendChild(el("p", {}, "Login first to use the dashboard and chatbot."));
  header.appendChild(titleWrap);
  workspace.appendChild(header);

  if (state.authChecking) {
    const panel = el("section", { className: "auth-panel" });
    panel.appendChild(el("div", { className: "empty-state" }, "Checking session..."));
    workspace.appendChild(panel);
    return workspace;
  }

  workspace.appendChild(renderAuth());
  return workspace;
}

function renderWorkspace() {
  normalizeActiveView();

  if (state.activeView === "survey") {
    return renderSurveyWorkspace();
  }

  const workspace = el("section", { className: `workspace app-workspace role-${state.currentUser.role}` });
  workspace.appendChild(renderTopBar());

  const body = el("div", { className: "app-body" });
  body.appendChild(renderSidebar());

  const content = el("section", { className: "main-content" });
  content.appendChild(renderActiveView());
  body.appendChild(content);
  workspace.appendChild(body);
  return workspace;
}

function renderSurveyWorkspace() {
  const workspace = el("section", { className: "workspace app-workspace survey-workspace" });
  const header = el("header", { className: "survey-focus-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h1", {}, "Student Mental Health Platform"));
  titleWrap.appendChild(el("p", {}, "Tiến độ khảo sát được lưu tự động trên thiết bị này."));
  header.appendChild(titleWrap);

  const actions = el("div", { className: "header-actions" });
  const pauseButton = el("button", { type: "button", className: "secondary-button" }, "Tạm dừng và quay lại Chatbot");
  pauseButton.addEventListener("click", () => {
    persistSurveyDraft();
    state.activeView = "chat";
    render();
  });
  actions.appendChild(pauseButton);
  const logoutButton = el("button", { type: "button", className: "secondary-button" }, "Logout");
  logoutButton.addEventListener("click", logout);
  actions.appendChild(logoutButton);
  header.appendChild(actions);
  workspace.appendChild(header);

  const content = el("main", { className: "survey-focus-content" });
  content.appendChild(renderSurvey());
  workspace.appendChild(content);
  return workspace;
}

function renderTopBar() {
  const header = el("header", { className: "topbar" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h1", {}, "Student Mental Health Platform"));
  titleWrap.appendChild(el("p", {}, state.currentUser.role === "student" ? "Chatbot, khảo sát tâm lý và dashboard." : "Analytics dashboard and account profile."));
  header.appendChild(titleWrap);

  const right = el("div", { className: "header-actions" });
  right.appendChild(el("div", { className: "account-chip" }, `${state.currentUser.display_name} | ${state.currentUser.role}`));
  const logoutButton = el("button", { type: "button", className: "secondary-button" }, "Logout");
  logoutButton.addEventListener("click", logout);
  right.appendChild(logoutButton);
  header.appendChild(right);
  return header;
}

function renderSidebar() {
  const sidebar = el("aside", { className: "sidebar", "aria-label": "Workspace navigation" });
  const label = el("div", { className: "sidebar-label" }, "Workspace");
  sidebar.appendChild(label);

  const nav = el("nav", { className: "side-nav" });
  for (const item of getNavItems()) {
    nav.appendChild(renderSideNavItem(item));
  }
  sidebar.appendChild(nav);
  return sidebar;
}

function renderSideNavItem(item) {
  const { view, label, locked = false } = item;
  const button = el(
    "button",
    {
      type: "button",
      className: `${state.activeView === view ? "side-nav-item active" : "side-nav-item"}${locked ? " locked" : ""}`,
      "data-view": view,
      title: locked ? "Hoàn thành khảo sát tâm lý để mở Dashboard" : label,
    },
  );
  button.appendChild(el("span", {}, label));
  if (locked) {
    button.disabled = true;
    button.appendChild(el("small", {}, "Hoàn thành khảo sát để mở"));
  }
  button.addEventListener("click", () => {
    if (locked) {
      return;
    }
    if (state.activeView === "survey") {
      persistSurveyDraft();
    }
    state.activeView = view;
    if (view === "survey") {
      loadSurveyQuestions();
    }
    render();
  });
  return button;
}

function getNavItems() {
  if (state.currentUser.role === "student") {
    const surveyRequired = isSurveyRequired();
    const surveyCompleted = isSurveyCompleted();
    const items = [{ view: "chat", label: "Chatbot" }];
    if (surveyRequired && !surveyCompleted) {
      items.push({ view: "survey", label: "Khảo sát tâm lý" });
    }
    items.push({ view: "dashboard", label: "Dashboard", locked: surveyRequired && !surveyCompleted });
    items.push({ view: "profile", label: "Profile" });
    return items;
  }
  return [
    { view: "dashboard", label: "Dashboard" },
    { view: "profile", label: "Profile" },
  ];
}

function getAllowedViews() {
  const views = getNavItems().filter((item) => !item.locked).map((item) => item.view);
  if (
    state.currentUser?.role === "student" &&
    state.survey.status?.survey_required &&
    !state.survey.status?.survey_completed &&
    !views.includes("survey")
  ) {
    views.push("survey");
  }
  return views;
}

function isSurveyRequired() {
  return Boolean(state.survey.status?.survey_required ?? state.currentUser?.profile?.survey_required);
}

function isSurveyCompleted() {
  return Boolean(state.survey.status?.survey_completed ?? state.currentUser?.profile?.survey_completed);
}

function normalizeActiveView() {
  const allowed = getAllowedViews();
  if (!allowed.includes(state.activeView)) {
    state.activeView = allowed[0];
  }
}

function renderActiveView() {
  if (state.activeView === "chat") {
    return renderChat();
  }
  if (state.activeView === "profile") {
    return renderProfile();
  }
  if (state.activeView === "survey") {
    return renderSurvey();
  }
  return renderDashboard();
}

function shouldShowSurveyPrompt() {
  return Boolean(
    state.currentUser?.role === "student" &&
      state.survey.status?.show_survey_prompt &&
      !state.survey.promptDismissed,
  );
}

function renderSurveyPrompt() {
  const overlay = el("div", { className: "modal-overlay", role: "presentation" });
  const dialog = el("section", {
    className: "survey-modal",
    role: "dialog",
    "aria-modal": "true",
    "aria-label": "Survey required",
  });
  dialog.appendChild(el("h2", {}, "Khảo sát tâm lý"));
  dialog.appendChild(
    el(
      "p",
      {},
      "Bạn cần hoàn thành khảo sát tâm lý trước khi xem Dashboard. Bạn có thể tạm dừng để dùng Chatbot và quay lại sau.",
    ),
  );
  const actions = el("div", { className: "modal-actions" });
  const startButton = el("button", { type: "button", className: "primary-button" }, "Đồng ý làm khảo sát");
  startButton.addEventListener("click", () => {
    state.survey.promptDismissed = true;
    state.activeView = "survey";
    loadSurveyQuestions();
    render();
  });
  const laterButton = el("button", { type: "button", className: "secondary-button" }, "Làm sau");
  laterButton.disabled = state.survey.isSubmitting;
  laterButton.addEventListener("click", postponeSurvey);
  actions.appendChild(startButton);
  actions.appendChild(laterButton);
  dialog.appendChild(actions);
  if (state.survey.error) {
    dialog.appendChild(el("div", { className: "error-banner inline" }, state.survey.error));
  }
  overlay.appendChild(dialog);
  return overlay;
}

function renderToast(message) {
  const toast = el("div", { className: "toast success-banner" }, message);
  toast.addEventListener("click", () => {
    state.survey.statusMessage = "";
    render();
  });
  return toast;
}

function renderAuth() {
  const panel = el("section", {
    className: `auth-panel auth-mode-${state.authMode}`,
    "aria-label": "Authentication",
  });

  const modeTabs = el("div", { className: "mode-tabs" });
  modeTabs.appendChild(renderModeButton("login", "Login"));
  modeTabs.appendChild(renderModeButton("register", "Register"));
  panel.appendChild(modeTabs);

  const form = el("form", { className: `user-form wide auth-form auth-form-${state.authMode}` });
  form.appendChild(renderInput("Email", "email", state.auth.email, "email", "you@example.com", (value) => {
    updateAuthField("email", value);
  }));
  form.appendChild(renderInput("Password", "password", state.auth.password, "password", state.authMode === "login" ? "Enter your password" : "At least 8 characters", (value) => {
    updateAuthField("password", value);
  }));

  if (state.authMode === "register") {
    form.appendChild(renderInput("Display name", "text", state.auth.displayName, "displayName", "Name shown in the app", (value) => {
      updateAuthField("displayName", value);
    }));
    form.appendChild(renderRoleField());
    if (state.auth.role === "student") {
      appendStudentFields(form);
    }
  }

  const buttonText = state.auth.isSaving ? "Working..." : state.authMode === "login" ? "Login" : "Register";
  const button = el("button", { type: "submit", className: "primary-button" }, buttonText);
  button.disabled = state.auth.isSaving;
  form.appendChild(button);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    state.authMode === "login" ? login() : register();
  });
  panel.appendChild(form);

  appendStatus(panel);
  return panel;
}

function renderModeButton(mode, label) {
  const button = el("button", { type: "button", className: state.authMode === mode ? "tab active" : "tab" }, label);
  button.addEventListener("click", () => {
    state.authMode = mode;
    state.auth.status = "";
    state.auth.error = "";
    state.auth.fieldErrors = {};
    state.auth.focusField = "";
    render();
  });
  return button;
}

function renderDashboard() {
  const panel = el("section", { className: "dashboard-panel", "aria-label": "Analytics dashboard" });
  panel.appendChild(el("iframe", {
    className: "dashboard-frame",
    title: "Mental school analytics dashboard",
    src: DASHBOARD_URL,
  }));
  return panel;
}

function renderChat() {
  const panel = el("section", {
    className: "chat-panel",
    "aria-label": "Student support chatbot",
  });

  panel.appendChild(renderChatHeader());
  panel.appendChild(renderMessages());

  if (state.error) {
    panel.appendChild(el("div", { className: "error-banner" }, state.error));
  }

  panel.appendChild(renderComposer());
  return panel;
}

function renderChatHeader() {
  const header = el("header", { className: "panel-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h2", {}, "Student Support Chat"));
  titleWrap.appendChild(el("p", {}, "Grounded wellbeing guidance from the project knowledge base."));
  header.appendChild(titleWrap);
  header.appendChild(el("div", { className: "status-pill" }, "Qdrant RAG"));
  return header;
}

function renderProfile() {
  const panel = el("section", { className: "user-panel", "aria-label": "User profile" });
  const header = el("header", { className: "panel-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h2", {}, "Profile"));
  titleWrap.appendChild(el("p", {}, "Only account fields required for the app are shown here."));
  header.appendChild(titleWrap);
  header.appendChild(el("div", { className: "status-pill" }, "PostgreSQL"));
  panel.appendChild(header);

  const form = el("form", { className: "user-form wide" });
  form.appendChild(renderReadonlyField("Email", state.currentUser.email));
  form.appendChild(renderInput("Display name", "text", state.auth.displayName, "displayName", "Name shown in the app", (value) => {
    updateAuthField("displayName", value);
  }));
  form.appendChild(renderRoleField());
  if (state.auth.role === "student") {
    appendStudentFields(form);
  }

  const button = el("button", { type: "submit", className: "primary-button" }, state.auth.isSaving ? "Saving..." : "Save profile");
  button.disabled = state.auth.isSaving;
  form.appendChild(button);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    saveProfile();
  });

  panel.appendChild(form);
  appendStatus(panel);
  return panel;
}

function renderSurvey() {
  if (!state.survey.questions.length && !state.survey.isLoading && !state.survey.error) {
    loadSurveyQuestions();
  }

  const panel = el("section", { className: "survey-panel", "aria-label": "Khảo sát tâm lý" });
  const header = el("header", { className: "panel-header" });
  const titleWrap = el("div");
  titleWrap.appendChild(el("h2", {}, "Khảo sát tâm lý"));
  titleWrap.appendChild(el("p", {}, "Hãy chọn đáp án phù hợp nhất với trải nghiệm của bạn."));
  header.appendChild(titleWrap);
  panel.appendChild(header);

  if (state.survey.isLoading) {
    panel.appendChild(el("div", { className: "empty-state" }, "Đang tải câu hỏi..."));
    return panel;
  }
  if (state.survey.error) {
    panel.appendChild(el("div", { className: "error-banner" }, state.survey.error));
  }
  if (state.survey.statusMessage) {
    panel.appendChild(el("div", { className: "success-banner" }, state.survey.statusMessage));
  }
  if (!state.survey.questions.length) {
    panel.appendChild(el("div", { className: "empty-state" }, "Chưa có bộ câu hỏi khả dụng."));
    return panel;
  }

  const totalQuestions = state.survey.questions.length;
  state.survey.currentIndex = Math.min(Math.max(state.survey.currentIndex, 0), totalQuestions - 1);
  const currentQuestion = state.survey.questions[state.survey.currentIndex];
  const questionNumber = state.survey.currentIndex + 1;
  const isLastQuestion = questionNumber === totalQuestions;
  const progressPercent = Math.round((questionNumber / totalQuestions) * 100);

  const progress = el("div", { className: "survey-progress" });
  const progressTop = el("div", { className: "survey-progress-top" });
  progressTop.appendChild(el("strong", {}, `Câu ${questionNumber} / ${totalQuestions}`));
  progressTop.appendChild(el("span", {}, `${progressPercent}% hoàn thành`));
  progress.appendChild(progressTop);
  const progressTrack = el("div", {
    className: "survey-progress-track",
    role: "progressbar",
    "aria-valuemin": "0",
    "aria-valuemax": "100",
    "aria-valuenow": String(progressPercent),
  });
  const progressBar = el("div", { className: "survey-progress-bar" });
  progressBar.style.width = `${progressPercent}%`;
  progressTrack.appendChild(progressBar);
  progress.appendChild(progressTrack);
  panel.appendChild(progress);

  const form = el("form", { className: "survey-form survey-step-form" });
  form.appendChild(renderSurveyQuestion(currentQuestion));

  const actions = el("div", { className: "survey-step-actions" });
  const previous = el("button", { type: "button", className: "secondary-button" }, "Câu trước");
  previous.disabled = state.survey.currentIndex === 0 || state.survey.isSubmitting;
  previous.addEventListener("click", () => {
    state.survey.currentIndex -= 1;
    state.survey.error = "";
    persistSurveyDraft();
    render();
  });
  actions.appendChild(previous);

  const nextLabel = state.survey.isSubmitting
    ? "Đang gửi..."
    : isLastQuestion
      ? "Hoàn thành khảo sát"
      : "Câu tiếp theo";
  const next = el("button", { type: "button", className: "primary-button" }, nextLabel);
  next.disabled = state.survey.isSubmitting || !hasSurveyAnswer(currentQuestion);
  next.addEventListener("click", () => advanceSurvey(currentQuestion, isLastQuestion));
  actions.appendChild(next);
  form.appendChild(actions);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    advanceSurvey(currentQuestion, isLastQuestion);
  });
  panel.appendChild(form);
  return panel;
}

function advanceSurvey(currentQuestion, isLastQuestion) {
  if (!validateCurrentSurveyQuestion()) {
    render();
    return;
  }
  if (isLastQuestion) {
    submitSurvey();
    return;
  }
  state.survey.currentIndex += 1;
  state.survey.error = "";
  persistSurveyDraft();
  render();
}

function renderSurveyQuestion(question) {
  const fieldName = `survey_${question.id}`;
  const error = state.survey.fieldErrors[question.id];
  const wrap = el("fieldset", { className: error ? "field survey-question has-error" : "field survey-question" });
  wrap.appendChild(el("legend", { className: "survey-question-label" }, surveyQuestionLabel(question)));
  if (question.input_type === "date") {
    const input = el("input", {
      type: "date",
      "data-field": fieldName,
      "aria-invalid": error ? "true" : "false",
    });
    input.value = state.survey.answers[question.id] || new Date().toISOString().slice(0, 10);
    if (!state.survey.answers[question.id]) {
      state.survey.answers[question.id] = input.value;
    }
    input.addEventListener("input", (event) => updateSurveyAnswer(question.id, event.target.value));
    wrap.appendChild(input);
  } else {
    const choices = el("div", { className: "survey-choice-list" });
    for (const optionValue of question.options || []) {
      const selected = state.survey.answers[question.id] === optionValue;
      const choice = el("label", { className: selected ? "survey-choice selected" : "survey-choice" });
      const input = el("input", {
        type: "radio",
        name: fieldName,
        value: optionValue,
        "data-field": fieldName,
        "aria-invalid": error ? "true" : "false",
      });
      input.checked = selected;
      input.addEventListener("change", (event) => updateSurveyAnswer(question.id, event.target.value));
      choice.appendChild(input);
      choice.appendChild(el("span", {}, surveyOptionLabel(question, optionValue)));
      choices.appendChild(choice);
    }
    wrap.appendChild(choices);
  }
  if (error) {
    wrap.appendChild(el("span", { className: "field-error" }, error));
  }
  return wrap;
}

function renderRoleField() {
  const error = state.auth.fieldErrors.role;
  const wrap = el("label", { className: error ? "field has-error" : "field" });
  wrap.appendChild(el("span", {}, "Role"));
  const select = el("select", { "data-field": "role" });
  for (const [value, label] of [["student", "Student"], ["researcher", "Researcher"]]) {
    const option = el("option", { value }, label);
    if (state.auth.role === value) {
      option.selected = true;
    }
    select.appendChild(option);
  }
  select.addEventListener("change", (event) => {
    state.auth.role = event.target.value;
    clearFieldError("role");
    state.auth.fieldErrors = {};
    state.auth.focusField = "";
    render();
  });
  wrap.appendChild(select);
  appendFieldError(wrap, "role");
  return wrap;
}

function appendStudentFields(form) {
  form.appendChild(renderInput("Ngày tháng năm sinh", "date", state.auth.studentProfile.birthDate, "birthDate", "", (value) => {
    state.auth.studentProfile.birthDate = value;
    state.auth.studentProfile.age = calculateAgeFromBirthDate(value) ?? "";
    clearFieldError("birthDate");
  }));
  const calculatedAge = calculateAgeFromBirthDate(state.auth.studentProfile.birthDate);
  if (calculatedAge !== null) {
    form.appendChild(renderReadonlyField(
      "Nhóm khảo sát",
      calculatedAge <= 18 ? `Học sinh (${calculatedAge} tuổi)` : `Sinh viên (${calculatedAge} tuổi)`,
    ));
  }
  form.appendChild(renderSelect("Giới tính", state.auth.studentProfile.gender, [
    ["male", "Nam"],
    ["female", "Nữ"],
    ["other", "Khác / không muốn trả lời"],
  ], (value) => {
    state.auth.studentProfile.gender = value;
    clearFieldError("gender");
  }));
  form.appendChild(renderSelect("Bậc học", state.auth.studentProfile.learnerType, [
    ["elementary", "Tiểu học"],
    ["middle_school", "Trung học cơ sở"],
    ["high_school", "Trung học phổ thông"],
    ["college", "Cao đẳng"],
    ["university", "Đại học"],
    ["graduate", "Sau đại học"],
    ["other", "Khác"],
  ], (value) => {
    state.auth.studentProfile.learnerType = value;
    clearFieldError("learnerType");
  }));
}

function appendStatus(panel) {
  if (state.auth.status) {
    panel.appendChild(el("div", { className: "success-banner" }, state.auth.status));
  }
  if (state.auth.error) {
    panel.appendChild(el("div", { className: "error-banner" }, state.auth.error));
  }
}

function renderInput(label, type, value, fieldName, placeholder, onInput) {
  const error = state.auth.fieldErrors[fieldName];
  const wrap = el("label", { className: error ? "field has-error" : "field" });
  wrap.appendChild(el("span", {}, label));
  const input = el("input", {
    type,
    placeholder,
    "data-field": fieldName,
    "aria-invalid": error ? "true" : "false",
  });
  input.value = value ?? "";
  input.addEventListener("input", (event) => onInput(event.target.value));
  wrap.appendChild(input);
  appendFieldError(wrap, fieldName);
  return wrap;
}

function renderReadonlyField(label, value) {
  const wrap = el("label", { className: "field" });
  wrap.appendChild(el("span", {}, label));
  const input = el("input", { type: "text", readonly: "readonly" });
  input.value = value ?? "";
  wrap.appendChild(input);
  return wrap;
}

function renderSelect(label, value, options, onInput) {
  const fieldName = label === "Giới tính" ? "gender" : "learnerType";
  const error = state.auth.fieldErrors[fieldName];
  const wrap = el("label", { className: error ? "field has-error" : "field" });
  wrap.appendChild(el("span", {}, label));
  const select = el("select", {
    "data-field": fieldName,
    "aria-invalid": error ? "true" : "false",
  });
  for (const [optionValue, optionLabel] of options) {
    const option = el("option", { value: optionValue }, optionLabel);
    if (value === optionValue) {
      option.selected = true;
    }
    select.appendChild(option);
  }
  select.addEventListener("change", (event) => onInput(event.target.value));
  wrap.appendChild(select);
  appendFieldError(wrap, fieldName);
  return wrap;
}

function renderMessages() {
  const list = el("div", { className: "message-list" });

  for (const message of state.messages) {
    list.appendChild(renderMessage(message));
  }

  if (state.isLoading) {
    const node = renderMessage({ id: "loading", role: "assistant", content: "Thinking..." });
    node.querySelector(".message-body").classList.add("loading");
    list.appendChild(node);
  }

  return list;
}

function renderMessage(message) {
  const article = el("article", { className: `message ${message.role}` });
  const avatar = el("div", { className: "avatar", "aria-hidden": "true" }, message.role === "assistant" ? "AI" : "You");
  const body = el("div", { className: "message-body" });
  body.appendChild(el("p", {}, message.content));

  article.appendChild(avatar);
  article.appendChild(body);
  return article;
}

function renderComposer() {
  const form = el("form", { className: "composer" });
  const textarea = el("textarea", {
    "aria-label": "Message",
    placeholder: "Type your question...",
    rows: "2",
  });
  textarea.value = state.input;
  textarea.addEventListener("input", (event) => {
    state.input = event.target.value;
    updateSendButton(form);
  });
  textarea.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      handleSubmit();
    }
  });

  const button = el("button", { type: "submit", "aria-label": "Send message" }, state.isLoading ? "..." : ">");
  button.disabled = state.isLoading || !state.input.trim();

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    handleSubmit();
  });

  form.appendChild(textarea);
  form.appendChild(button);
  return form;
}

function updateSendButton(form) {
  const button = form.querySelector("button");
  button.disabled = state.isLoading || !state.input.trim();
}

async function handleSubmit() {
  const question = state.input.trim();
  if (!question || state.isLoading) {
    return;
  }

  const chatHistory = state.messages
    .filter((message) => message.id !== "welcome")
    .map((message) => ({ role: message.role, content: message.content }))
    .slice(-10);

  state.messages.push({ id: createSessionId(), role: "user", content: question });
  state.input = "";
  state.error = "";
  state.isLoading = true;
  render();

  try {
    const response = await fetch(`${API_BASE_URL}/api/rag/ask`, {
      method: "POST",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        question,
        session_id: state.sessionId || createSessionId(),
        chat_history: chatHistory,
      }),
    });

    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.detail?.message || "The chat service returned an error.");
    }

    if (payload.session_id) {
      state.sessionId = payload.session_id;
      localStorage.setItem(SESSION_KEY, state.sessionId);
    }

    state.messages.push({ id: createSessionId(), role: "assistant", content: payload.answer });
  } catch (error) {
    state.error = error instanceof Error ? error.message : "Unable to send the message.";
  } finally {
    state.isLoading = false;
    render();
  }
}

async function register() {
  if (!validateAuthForm("register")) {
    render();
    return;
  }

  const payload = {
    email: state.auth.email,
    password: state.auth.password,
    display_name: state.auth.displayName,
    role: state.auth.role,
  };
  if (state.auth.role === "student") {
    payload.student_profile = buildStudentProfilePayload();
  } else {
    payload.researcher_profile = {};
  }
  await authenticate("/api/auth/register", payload, "Registration complete.");
}

async function login() {
  if (!validateAuthForm("login")) {
    render();
    return;
  }

  await authenticate("/api/auth/login", {
    email: state.auth.email,
    password: state.auth.password,
  }, "Login complete.");
}

async function authenticate(path, body, successMessage) {
  state.auth.isSaving = true;
  state.auth.status = "";
  state.auth.error = "";
  state.auth.fieldErrors = {};
  render();

  try {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.detail?.message || "Authentication failed.");
    }
    applyAuth(payload);
    state.auth.status = successMessage;
    state.activeView = defaultViewForRole(payload.user.role);
    await loadSurveyStatus(false);
  } catch (error) {
    state.auth.error = error instanceof Error ? error.message : "Authentication failed.";
  } finally {
    state.auth.isSaving = false;
    render();
  }
}

async function loadCurrentUser() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/auth/me`, {
      headers: authHeaders(),
    });
    if (!response.ok) {
      throw new Error("Session expired.");
    }
    state.currentUser = await response.json();
    syncAuthFormFromUser(state.currentUser);
    await loadSurveyStatus(false);
    normalizeActiveView();
  } catch {
    logout(false);
  } finally {
    state.authChecking = false;
    render();
  }
}

async function saveProfile() {
  if (!validateProfileForm()) {
    render();
    return;
  }

  state.auth.isSaving = true;
  state.auth.status = "";
  state.auth.error = "";
  state.auth.fieldErrors = {};
  render();

  const payload = {
    display_name: state.auth.displayName,
    role: state.auth.role,
  };
  if (state.auth.role === "student") {
    payload.student_profile = buildStudentProfilePayload();
  } else {
    payload.researcher_profile = {};
  }

  try {
    const response = await fetch(`${API_BASE_URL}/api/users/me`, {
      method: "PUT",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result?.detail?.message || "Unable to save profile.");
    }
    state.currentUser = result;
    syncAuthFormFromUser(result);
    await loadSurveyStatus(false);
    normalizeActiveView();
    state.auth.status = "Profile saved.";
  } catch (error) {
    state.auth.error = error instanceof Error ? error.message : "Unable to save profile.";
  } finally {
    state.auth.isSaving = false;
    render();
  }
}

async function loadSurveyStatus(shouldRender = true) {
  if (!state.authToken || !state.currentUser || state.currentUser.role !== "student") {
    resetSurveyState();
    if (shouldRender) {
      render();
    }
    return;
  }
  try {
    const response = await fetch(`${API_BASE_URL}/api/survey/status`, {
      headers: authHeaders(),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.detail?.message || "Unable to load survey status.");
    }
    state.survey.status = payload;
    if (payload.survey_completed) {
      clearSurveyDraft(payload.survey_type);
      state.survey.questions = [];
      state.survey.answers = {};
      state.survey.currentIndex = 0;
      state.survey.fieldErrors = {};
    }
  } catch (error) {
    state.survey.error = error instanceof Error ? error.message : "Unable to load survey status.";
  } finally {
    normalizeActiveView();
    if (shouldRender) {
      render();
    }
  }
}

async function loadSurveyQuestions() {
  if (state.survey.isLoading || state.survey.questions.length || state.survey.status?.survey_completed) {
    return;
  }
  state.survey.isLoading = true;
  state.survey.error = "";
  render();
  try {
    const response = await fetch(`${API_BASE_URL}/api/survey/questions`, {
      headers: authHeaders(),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.detail?.message || "Unable to load survey questions.");
    }
    state.survey.questions = payload.questions || [];
    state.survey.currentIndex = 0;
    state.survey.status = {
      ...(state.survey.status || {}),
      survey_type: payload.survey_type,
    };
    restoreSurveyDraft();
    prefillSurveyAnswersFromProfile();
    persistSurveyDraft();
  } catch (error) {
    state.survey.error = error instanceof Error ? error.message : "Unable to load survey questions.";
  } finally {
    state.survey.isLoading = false;
    render();
  }
}

async function postponeSurvey() {
  state.survey.isSubmitting = true;
  state.survey.error = "";
  render();
  try {
    const response = await fetch(`${API_BASE_URL}/api/survey/postpone`, {
      method: "POST",
      headers: authHeaders(),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.detail?.message || "Unable to postpone survey.");
    }
    state.survey.status = payload;
    state.survey.promptDismissed = true;
    normalizeActiveView();
  } catch (error) {
    state.survey.error = error instanceof Error ? error.message : "Unable to postpone survey.";
  } finally {
    state.survey.isSubmitting = false;
    render();
  }
}

async function submitSurvey() {
  if (!validateSurveyForm()) {
    render();
    return;
  }
  state.survey.isSubmitting = true;
  state.survey.error = "";
  state.survey.statusMessage = "";
  render();
  try {
    const response = await fetch(`${API_BASE_URL}/api/survey/submit`, {
      method: "POST",
      headers: {
        ...authHeaders(),
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        survey_type: state.survey.status?.survey_type,
        answers: state.survey.answers,
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      if (payload?.detail?.fields) {
        state.survey.fieldErrors = payload.detail.fields;
      }
      throw new Error(payload?.detail?.message || "Unable to submit survey.");
    }
    state.survey.status = payload.status;
    clearSurveyDraft(payload.status?.survey_type);
    state.survey.questions = [];
    state.survey.answers = {};
    state.survey.currentIndex = 0;
    state.survey.fieldErrors = {};
    state.survey.promptDismissed = true;
    state.survey.statusMessage = "Khảo sát đã hoàn thành.";
    state.activeView = "dashboard";
  } catch (error) {
    state.survey.error = error instanceof Error ? error.message : "Unable to submit survey.";
  } finally {
    state.survey.isSubmitting = false;
    normalizeActiveView();
    render();
  }
}

function validateSurveyForm() {
  const errors = {};
  for (const question of state.survey.questions) {
    if (question.required && !String(state.survey.answers[question.id] || "").trim()) {
      errors[question.id] = "Câu hỏi này là bắt buộc.";
    }
  }
  state.survey.fieldErrors = errors;
  return Object.keys(errors).length === 0;
}

function hasSurveyAnswer(question) {
  return !question.required || Boolean(String(state.survey.answers[question.id] || "").trim());
}

function validateCurrentSurveyQuestion() {
  const question = state.survey.questions[state.survey.currentIndex];
  if (!question || hasSurveyAnswer(question)) {
    return true;
  }
  state.survey.fieldErrors[question.id] = "Vui lòng trả lời câu hỏi này trước khi tiếp tục.";
  return false;
}

function updateSurveyAnswer(questionId, value) {
  state.survey.answers[questionId] = value;
  if (state.survey.fieldErrors[questionId]) {
    delete state.survey.fieldErrors[questionId];
  }
  persistSurveyDraft();
  render();
}

function prefillSurveyAnswersFromProfile() {
  const profile = state.currentUser?.profile || {};
  for (const question of state.survey.questions) {
    if (state.survey.answers[question.id]) {
      continue;
    }
    if (question.id === "gender" && profile.gender) {
      const option = findGenderOption(question.options || [], profile.gender);
      if (option) {
        state.survey.answers[question.id] = option;
      }
    }
  }
}

function findGenderOption(options, gender) {
  const normalizedGender = normalizeOptionText(gender);
  if (normalizedGender === "male") {
    return options.find((option) => normalizeOptionText(option) === "nam") || "";
  }
  if (normalizedGender === "female") {
    return options.find((option) => normalizeOptionText(option) === "nu") || "";
  }
  return (
    options.find((option) => {
      const normalized = normalizeOptionText(option);
      return normalized.includes("khac") || normalized.includes("khong muon");
    }) || ""
  );
}

function normalizeOptionText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();
}

function applyAuth(payload) {
  state.authToken = payload.access_token;
  localStorage.setItem(AUTH_TOKEN_KEY, state.authToken);
  state.currentUser = payload.user;
  syncAuthFormFromUser(payload.user);
}

function logout(shouldRender = true) {
  persistSurveyDraft();
  state.authToken = "";
  state.currentUser = null;
  resetAuthForm();
  resetSurveyState();
  state.activeView = "dashboard";
  state.authChecking = false;
  localStorage.removeItem(AUTH_TOKEN_KEY);
  if (shouldRender) {
    render();
  }
}

function syncAuthFormFromUser(user) {
  state.auth.email = user.email || "";
  state.auth.displayName = user.display_name || "";
  state.auth.role = user.role || "student";
  const profile = user.profile || {};
  state.auth.studentProfile = {
    age: profile.age ?? "",
    birthDate: profile.birth_date || "",
    gender: profile.gender || "other",
    learnerType: profile.learner_type || "university",
  };
}

function defaultViewForRole(role) {
  return role === "student" ? "chat" : "dashboard";
}

function resetAuthForm() {
  state.authMode = "login";
  state.auth.email = "";
  state.auth.password = "";
  state.auth.displayName = "";
  state.auth.role = "student";
  state.auth.status = "";
  state.auth.error = "";
  state.auth.fieldErrors = {};
  state.auth.focusField = "";
  state.auth.isSaving = false;
  state.auth.studentProfile = {
    age: "",
    birthDate: "",
    gender: "other",
    learnerType: "university",
  };
}

function resetSurveyState() {
  state.survey.status = null;
  state.survey.questions = [];
  state.survey.answers = {};
  state.survey.currentIndex = 0;
  state.survey.fieldErrors = {};
  state.survey.isLoading = false;
  state.survey.isSubmitting = false;
  state.survey.error = "";
  state.survey.statusMessage = "";
  state.survey.promptDismissed = false;
}

function updateAuthField(fieldName, value) {
  state.auth[fieldName] = value;
  clearFieldError(fieldName);
}

function clearFieldError(fieldName) {
  if (state.auth.fieldErrors[fieldName]) {
    delete state.auth.fieldErrors[fieldName];
  }
}

function appendFieldError(wrap, fieldName) {
  const message = state.auth.fieldErrors[fieldName];
  if (message) {
    wrap.appendChild(el("span", { className: "field-error" }, message));
  }
}

function validateAuthForm(mode) {
  const errors = {};
  if (!state.auth.email.trim()) {
    errors.email = "Email is required.";
  } else if (!isValidEmail(state.auth.email)) {
    errors.email = "Enter a valid email address.";
  }

  if (!state.auth.password.trim()) {
    errors.password = "Password is required.";
  } else if (mode === "register" && state.auth.password.length < 8) {
    errors.password = "Password must contain at least 8 characters.";
  }

  if (mode === "register") {
    if (!state.auth.displayName.trim()) {
      errors.displayName = "Display name is required.";
    }
    if (!state.auth.role) {
      errors.role = "Role is required.";
    }
    if (state.auth.role === "student") {
      validateStudentFields(errors);
    }
  }

  return applyValidationErrors(errors);
}

function validateProfileForm() {
  const errors = {};
  if (!state.auth.displayName.trim()) {
    errors.displayName = "Display name is required.";
  }
  if (!state.auth.role) {
    errors.role = "Role is required.";
  }
  if (state.auth.role === "student") {
    validateStudentFields(errors);
  }
  return applyValidationErrors(errors);
}

function validateStudentFields(errors) {
  const age = calculateAgeFromBirthDate(state.auth.studentProfile.birthDate);
  if (!state.auth.studentProfile.birthDate) {
    errors.birthDate = "Vui lòng nhập ngày tháng năm sinh.";
  } else if (age === null || age < 5 || age > 100) {
    errors.birthDate = "Ngày sinh phải tương ứng độ tuổi từ 5 đến 100.";
  }
  if (!state.auth.studentProfile.gender) {
    errors.gender = "Gender is required.";
  }
  if (!state.auth.studentProfile.learnerType) {
    errors.learnerType = "Learner type is required.";
  }
}

function applyValidationErrors(errors) {
  state.auth.fieldErrors = errors;
  state.auth.error = "";
  state.auth.status = "";
  const firstField = Object.keys(errors)[0];
  state.auth.focusField = firstField || "";
  return !firstField;
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

function buildStudentProfilePayload() {
  const age = calculateAgeFromBirthDate(state.auth.studentProfile.birthDate);
  return {
    birth_date: state.auth.studentProfile.birthDate || null,
    age,
    gender: state.auth.studentProfile.gender,
    learner_type: state.auth.studentProfile.learnerType,
  };
}

function calculateAgeFromBirthDate(value) {
  if (!value) {
    return null;
  }
  const birthDate = new Date(`${value}T00:00:00`);
  if (Number.isNaN(birthDate.getTime())) {
    return null;
  }
  const today = new Date();
  let age = today.getFullYear() - birthDate.getFullYear();
  const monthDelta = today.getMonth() - birthDate.getMonth();
  if (monthDelta < 0 || (monthDelta === 0 && today.getDate() < birthDate.getDate())) {
    age -= 1;
  }
  return age;
}

function authHeaders() {
  return {
    Authorization: `Bearer ${state.authToken}`,
  };
}

function optionalNumber(value) {
  if (value === null || value === undefined || String(value).trim() === "") {
    return null;
  }
  return Number(value);
}

function el(tagName, attributes = {}, text = "") {
  const node = document.createElement(tagName);
  for (const [name, value] of Object.entries(attributes)) {
    if (name === "className") {
      node.className = value;
    } else if (name === "readonly") {
      node.readOnly = true;
    } else {
      node.setAttribute(name, value);
    }
  }
  if (text) {
    node.textContent = displayText(text);
  }
  return node;
}

function displayText(value) {
  const text = String(value);
  if (!/[ÃÄÂáºá»]/.test(text)) {
    return text;
  }
  try {
    return decodeURIComponent(escape(text));
  } catch {
    return text;
  }
}

initializeSession();
render();
