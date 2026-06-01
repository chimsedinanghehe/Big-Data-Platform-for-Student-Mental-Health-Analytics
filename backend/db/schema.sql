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
ALTER TABLE app_users DROP CONSTRAINT IF EXISTS app_users_role_check;
ALTER TABLE app_users ALTER COLUMN role SET DEFAULT 'user';
UPDATE app_users SET role = 'user' WHERE role <> 'user';
ALTER TABLE app_users ADD CONSTRAINT app_users_role_check CHECK (role = 'user');

CREATE INDEX IF NOT EXISTS idx_app_users_role ON app_users(role);

CREATE TABLE IF NOT EXISTS student_profiles (
    user_id UUID PRIMARY KEY REFERENCES app_users(id) ON DELETE CASCADE,
    birthday DATE,
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

ALTER TABLE student_profiles ADD COLUMN IF NOT EXISTS birthday DATE;
ALTER TABLE student_profiles DROP COLUMN IF EXISTS age;
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
