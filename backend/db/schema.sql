CREATE TABLE IF NOT EXISTS app_users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'student',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT app_users_role_check CHECK (role IN ('student', 'researcher'))
);

ALTER TABLE app_users ADD COLUMN IF NOT EXISTS password_hash TEXT;
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_app_users_role ON app_users(role);

CREATE TABLE IF NOT EXISTS student_profiles (
    user_id UUID PRIMARY KEY REFERENCES app_users(id) ON DELETE CASCADE,
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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
ALTER TABLE researcher_profiles DROP COLUMN IF EXISTS organization;
ALTER TABLE researcher_profiles DROP COLUMN IF EXISTS position_title;
ALTER TABLE researcher_profiles DROP COLUMN IF EXISTS research_area;

CREATE TABLE IF NOT EXISTS app_sessions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_app_sessions_user_id ON app_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_app_sessions_expires_at ON app_sessions(expires_at);
