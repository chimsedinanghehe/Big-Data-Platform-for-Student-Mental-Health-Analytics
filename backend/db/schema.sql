CREATE TABLE IF NOT EXISTS app_users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT app_users_role_check CHECK (role = 'user')
);

ALTER TABLE app_users ADD COLUMN IF NOT EXISTS password_hash TEXT;
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS survey_required BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS survey_completed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS survey_type TEXT;
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS survey_postponed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS survey_postponed_at TIMESTAMPTZ;
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS survey_completed_at TIMESTAMPTZ;

DO $$
BEGIN
    ALTER TABLE app_users
        ADD CONSTRAINT app_users_survey_type_check
        CHECK (survey_type IS NULL OR survey_type IN ('school', 'university'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_app_users_role ON app_users(role);

CREATE TABLE IF NOT EXISTS student_profiles (
    user_id UUID PRIMARY KEY REFERENCES app_users(id) ON DELETE CASCADE,
    age INTEGER CHECK (age IS NULL OR age BETWEEN 5 AND 100),
    birth_date DATE,
    gender TEXT CHECK (gender IS NULL OR gender IN ('male', 'female', 'other')),
    learner_type TEXT CHECK (
        learner_type IS NULL OR learner_type IN (
            'elementary',
            'middle_school',
            'high_school',
            'college',
            'university',
            'graduate',
            'other'
        )
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS age INTEGER CHECK (age IS NULL OR age BETWEEN 5 AND 100);
ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS birth_date DATE;
ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS survey_required BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS survey_completed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS survey_type TEXT;
ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS survey_completed_at TIMESTAMPTZ;
ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS survey_postponed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS survey_postponed_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'student_profiles_survey_type_check'
    ) THEN
        ALTER TABLE student_profiles
        ADD CONSTRAINT student_profiles_survey_type_check
        CHECK (survey_type IS NULL OR survey_type IN ('school', 'university'));
    END IF;
END
$$;

UPDATE student_profiles
SET survey_type = CASE WHEN age <= 18 THEN 'school' ELSE 'university' END,
    survey_required = age IS NOT NULL
WHERE survey_type IS NULL
  AND age IS NOT NULL;

UPDATE app_users u
SET survey_required = sp.survey_required,
    survey_completed = sp.survey_completed,
    survey_type = sp.survey_type,
    survey_completed_at = sp.survey_completed_at,
    survey_postponed = sp.survey_postponed,
    survey_postponed_at = sp.survey_postponed_at
FROM student_profiles sp
WHERE sp.user_id = u.id;

CREATE TABLE IF NOT EXISTS researcher_profiles (
    user_id UUID PRIMARY KEY REFERENCES app_users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE student_profiles DROP COLUMN IF EXISTS institution_name;
ALTER TABLE student_profiles DROP COLUMN IF EXISTS grade_or_year;
ALTER TABLE student_profiles DROP COLUMN IF EXISTS field_of_study;
ALTER TABLE student_profiles DROP COLUMN IF EXISTS region;
ALTER TABLE student_profiles DROP COLUMN IF EXISTS stress_level;
ALTER TABLE student_profiles DROP COLUMN IF EXISTS sleep_hours;
ALTER TABLE student_profiles DROP COLUMN IF EXISTS consent_for_research;

CREATE TABLE IF NOT EXISTS app_sessions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_app_sessions_user_id ON app_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_app_sessions_expires_at ON app_sessions(expires_at);

CREATE TABLE IF NOT EXISTS survey_responses (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    survey_type TEXT NOT NULL CHECK (survey_type IN ('school', 'university')),
    answers JSONB NOT NULL,
    question_count INTEGER NOT NULL DEFAULT 0,
    answer_version TEXT NOT NULL DEFAULT 'v1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exported_at TIMESTAMPTZ,
    export_batch_id TEXT,
    export_object_uri TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_survey_responses_user_id ON survey_responses(user_id);
CREATE INDEX IF NOT EXISTS idx_survey_responses_survey_type ON survey_responses(survey_type);
CREATE INDEX IF NOT EXISTS idx_survey_responses_exported_at ON survey_responses(exported_at);

CREATE TABLE IF NOT EXISTS chat_session_user_map (
    anonymous_session_id TEXT PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    survey_type TEXT CHECK (survey_type IS NULL OR survey_type IN ('school', 'university')),
    age INTEGER CHECK (age IS NULL OR age BETWEEN 5 AND 100),
    gender TEXT CHECK (gender IS NULL OR gender IN ('male', 'female', 'other')),
    learner_type TEXT CHECK (
        learner_type IS NULL OR learner_type IN (
            'elementary',
            'middle_school',
            'high_school',
            'college',
            'university',
            'graduate',
            'other'
        )
    ),
    user_group TEXT CHECK (user_group IS NULL OR user_group IN ('school', 'university')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE chat_session_user_map ADD COLUMN IF NOT EXISTS gender TEXT;
ALTER TABLE chat_session_user_map ADD COLUMN IF NOT EXISTS learner_type TEXT;

CREATE INDEX IF NOT EXISTS idx_chat_session_user_map_user_id ON chat_session_user_map(user_id);
