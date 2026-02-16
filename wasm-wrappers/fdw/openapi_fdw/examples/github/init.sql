-- OpenAPI FDW example: GitHub API
-- Requires a GitHub personal access token (set GITHUB_TOKEN env var).
-- See: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens
-- Note: fdw_package_url uses file:// for local Docker testing. In production, use the
-- GitHub release URL: https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm

-- Create supabase_admin role if it doesn't exist (required by wrappers extension)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'supabase_admin') THEN
    CREATE ROLE supabase_admin WITH SUPERUSER CREATEDB CREATEROLE LOGIN PASSWORD 'postgres';
  END IF;
END
$$;

create schema if not exists extensions;
create extension if not exists wrappers with schema extensions;

set search_path to public, extensions;

create foreign data wrapper wasm_wrapper
  handler wasm_fdw_handler
  validator wasm_fdw_validator;

-- ============================================================
-- Server 1: github — Main GitHub API server
-- Bearer token auth via Authorization header (default behavior)
-- Custom headers for GitHub API versioning
-- ============================================================
create server github
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.github.com',
    api_key 'placeholder',
    user_agent 'openapi-fdw-example/0.2.0',
    accept 'application/vnd.github+json',
    headers '{"X-GitHub-Api-Version": "2022-11-28"}',
    page_size '30',
    page_size_param 'per_page'
  );

-- ============================================================
-- Server 2: github_debug — Same API with debug output
-- Emits HTTP request details as INFO messages
-- ============================================================
create server github_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.github.com',
    api_key 'placeholder',
    user_agent 'openapi-fdw-example/0.2.0',
    accept 'application/vnd.github+json',
    headers '{"X-GitHub-Api-Version": "2022-11-28"}',
    page_size '30',
    page_size_param 'per_page',
    debug 'true'
  );

-- ============================================================
-- Table 1: my_profile
-- Authenticated user's profile — GET /user
-- Features: single object response, bearer token auth, custom headers
-- ============================================================
create foreign table my_profile (
  login text,
  id bigint,
  name text,
  email text,
  bio text,
  public_repos integer,
  public_gists integer,
  followers integer,
  following integer,
  created_at timestamptz,
  avatar_url text,
  company text,
  location text,
  blog text,
  attrs jsonb
)
  server github
  options (
    endpoint '/user'
  );

-- ============================================================
-- Table 2: my_repos
-- Authenticated user's repositories — GET /user/repos
-- Features: page-based pagination (auto-detected), query pushdown (type, sort)
-- ============================================================
create foreign table my_repos (
  id bigint,
  name text,
  full_name text,
  description text,
  private boolean,
  fork boolean,
  language text,
  stargazers_count integer,
  forks_count integer,
  open_issues_count integer,
  created_at timestamptz,
  updated_at timestamptz,
  pushed_at timestamptz,
  html_url text,
  default_branch text,
  archived boolean,
  type text,
  sort text,
  attrs jsonb
)
  server github
  options (
    endpoint '/user/repos',
    rowid_column 'id'
  );

-- ============================================================
-- Table 3: repo_detail
-- Full repository metadata — GET /repos/{owner}/{repo}
-- Features: two path parameters, single object response
-- ============================================================
create foreign table repo_detail (
  id bigint,
  name text,
  full_name text,
  description text,
  private boolean,
  stargazers_count integer,
  forks_count integer,
  open_issues_count integer,
  watchers_count integer,
  language text,
  default_branch text,
  created_at timestamptz,
  updated_at timestamptz,
  license jsonb,
  topics jsonb,
  owner text,
  repo text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}'
  );

-- ============================================================
-- Table 4: repo_issues
-- Repository issues — GET /repos/{owner}/{repo}/issues
-- Features: two path parameters, page-based pagination,
--           query pushdown (state), timestamptz coercion
-- ============================================================
create foreign table repo_issues (
  id bigint,
  number integer,
  title text,
  state text,
  body text,
  created_at timestamptz,
  updated_at timestamptz,
  closed_at timestamptz,
  comments integer,
  user_col jsonb,
  labels jsonb,
  html_url text,
  owner text,
  repo text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}/issues',
    rowid_column 'id'
  );

-- ============================================================
-- Table 5: repo_pulls
-- Repository pull requests — GET /repos/{owner}/{repo}/pulls
-- Features: two path parameters, page-based pagination,
--           query pushdown (state), boolean + timestamptz coercion
-- ============================================================
create foreign table repo_pulls (
  id bigint,
  number integer,
  title text,
  state text,
  draft boolean,
  created_at timestamptz,
  updated_at timestamptz,
  merged_at timestamptz,
  user_col jsonb,
  head jsonb,
  base jsonb,
  html_url text,
  owner text,
  repo text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}/pulls',
    rowid_column 'id'
  );

-- ============================================================
-- Table 6: repo_releases
-- Repository releases — GET /repos/{owner}/{repo}/releases
-- Features: two path parameters, page-based pagination
-- ============================================================
create foreign table repo_releases (
  id bigint,
  tag_name text,
  name text,
  body text,
  draft boolean,
  prerelease boolean,
  created_at timestamptz,
  published_at timestamptz,
  author jsonb,
  html_url text,
  owner text,
  repo text,
  attrs jsonb
)
  server github
  options (
    endpoint '/repos/{owner}/{repo}/releases',
    rowid_column 'id'
  );

-- ============================================================
-- Table 7: search_repos
-- Search repositories — GET /search/repositories
-- Features: query pushdown (q), auto-detected "items" wrapper key
-- ============================================================
create foreign table search_repos (
  id bigint,
  name text,
  full_name text,
  description text,
  stargazers_count integer,
  forks_count integer,
  language text,
  open_issues_count integer,
  created_at timestamptz,
  html_url text,
  topics jsonb,
  license jsonb,
  q text,
  attrs jsonb
)
  server github
  options (
    endpoint '/search/repositories',
    rowid_column 'id'
  );

-- ============================================================
-- Table 8: search_repos_debug
-- Same as search_repos but on the debug server
-- Features: debug output in INFO messages
-- ============================================================
create foreign table search_repos_debug (
  id bigint,
  name text,
  full_name text,
  stargazers_count integer,
  q text,
  attrs jsonb
)
  server github_debug
  options (
    endpoint '/search/repositories',
    rowid_column 'id'
  );
