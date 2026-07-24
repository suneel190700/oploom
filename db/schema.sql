create table if not exists events (
    id          uuid primary key default gen_random_uuid(),
    pipeline    text not null,
    payload     jsonb not null,
    status      text not null default 'received',
    -- received | running | pending_approval | completed | rejected | dead_lettered
    classification text,
    route       text,
    action      text,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);
create index if not exists events_pipeline_status_idx on events (pipeline, status);

create table if not exists stage_runs (
    id          bigint generated always as identity primary key,
    event_id    uuid not null references events (id) on delete cascade,
    stage       text not null,
    attempt     int  not null default 1,
    status      text not null,          -- started | succeeded | failed
    detail      jsonb,
    started_at  timestamptz not null default now(),
    finished_at timestamptz
);
create index if not exists stage_runs_event_idx on stage_runs (event_id);

create table if not exists dlq (
    id          bigint generated always as identity primary key,
    event_id    uuid not null references events (id) on delete cascade,
    stage       text not null,
    attempts    int  not null,
    error       text not null,
    payload     jsonb not null,
    created_at  timestamptz not null default now(),
    replayed_at timestamptz
);

create table if not exists approvals (
    id          bigint generated always as identity primary key,
    event_id    uuid not null references events (id) on delete cascade,
    reason      text not null,
    status      text not null default 'pending',  -- pending | approved | rejected
    decided_by  text,
    decided_at  timestamptz,
    created_at  timestamptz not null default now()
);
create index if not exists approvals_status_idx on approvals (status);
