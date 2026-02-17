# GitHub API Example

Query the [GitHub REST API](https://docs.github.com/en/rest) using SQL. This example demonstrates bearer token authentication, page-based pagination, path parameter substitution, query parameter pushdown, and custom HTTP headers.

## Server Configuration

```sql
create server github
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'https://github.com/supabase/wrappers/releases/download/wasm_openapi_fdw_v0.2.0/openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://api.github.com',
    api_key '<YOUR_GITHUB_TOKEN>',
    user_agent 'openapi-fdw-example/0.2.0',
    accept 'application/vnd.github+json',
    headers '{"X-GitHub-Api-Version": "2022-11-28"}',
    page_size '30',
    page_size_param 'per_page'
  );
```

---

## 1. Quick Start with IMPORT FOREIGN SCHEMA

The `github_import` server has a `spec_url` pointing to the GitHub REST API OpenAPI spec, so tables can be auto-generated:

> **Note:** The GitHub OpenAPI spec is large (~15 MB). The initial import may take a few seconds to fetch and parse.

```sql
CREATE SCHEMA IF NOT EXISTS github_auto;

IMPORT FOREIGN SCHEMA "unused"
FROM SERVER github_import
INTO github_auto;
```

See what was generated:

```sql
SELECT foreign_table_name FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'github_auto';
```

Pick a generated table and query it:

```sql
SELECT * FROM github_auto.user LIMIT 1;
```

The rest of this example uses manually defined tables to demonstrate specific features (path parameters, query pushdown, custom headers, etc.).

---

## 2. Your Profile

Single object response. The FDW returns one row with your GitHub profile info.

```sql
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
```

```sql
SELECT login, name, public_repos, followers
FROM my_profile;
```

| login | name | public_repos | followers |
| --- | --- | --- | --- |
| youruser | Your Name | 42 | 150 |

> Your results will reflect your own GitHub profile.

Full profile with bio, company, and timestamps:

```sql
SELECT login, name, email, bio, company, location, blog,
       public_repos, public_gists, followers, following,
       created_at
FROM my_profile;
```

## 3. Your Repositories

Paginated list of your repos. The FDW auto-detects page-based pagination via `Link` headers.

```sql
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
```

```sql
SELECT name, language, stargazers_count, fork
FROM my_repos
LIMIT 5;
```

| name | language | stargazers_count | fork |
| --- | --- | --- | --- |
| my-project | TypeScript | 24 | f |
| dotfiles | Shell | 3 | f |
| cool-app | Rust | 12 | f |
| some-fork | | 0 | t |
| api-client | Python | 8 | f |

> Your results will reflect your own repositories.

Filter with query pushdown:

```sql
-- Pushes down to: GET /user/repos?type=owner&sort=updated
SELECT name, language, updated_at
FROM my_repos
WHERE type = 'owner' AND sort = 'updated'
LIMIT 5;
```

Full repo details with descriptions, URLs, and activity timestamps:

```sql
SELECT name, description, language, private, archived,
       stargazers_count, forks_count, open_issues_count,
       default_branch, html_url,
       created_at, updated_at, pushed_at
FROM my_repos
LIMIT 5;
```

## 4. Repository Detail (Path Parameters)

Look up a specific repository. The `{owner}` and `{repo}` placeholders in the endpoint are replaced with values from your WHERE clause.

```sql
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
```

```sql
SELECT name, stargazers_count, forks_count, language
FROM repo_detail
WHERE owner = 'supabase' AND repo = 'wrappers';
```

| name | stargazers_count | forks_count | language |
| --- | --- | --- | --- |
| wrappers | 811 | 92 | Rust |

Full detail with license, topics, and watcher count:

```sql
SELECT name, description, language, default_branch,
       stargazers_count, forks_count, watchers_count, open_issues_count,
       license->>'name' AS license,
       topics,
       created_at, updated_at
FROM repo_detail
WHERE owner = 'supabase' AND repo = 'wrappers';
```

## 5. Repository Issues

Issues for a repository. Two path parameters plus query pushdown for state filtering:

```sql
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
```

```sql
SELECT number, title, state
FROM repo_issues
WHERE owner = 'supabase' AND repo = 'wrappers'
LIMIT 5;
```

| number | title | state |
| --- | --- | --- |
| 571 | chore(deps): bump aws-sdk-s3 from 1.109.0 to 1.112.0 in the cargo group across 1 directory | open |
| 549 | feat: add aggregate pushdown support via GetForeignUpperPaths | open |
| 472 | AWS Cognito wrapper, ERROR: HV000: unhandled error | open |
| 461 | Hubspot FDW requires API Keys which are deprecated | open |
| 459 | Auth0 FDW API Key | open |

Filter by state:

```sql
SELECT number, title, state
FROM repo_issues
WHERE owner = 'supabase' AND repo = 'wrappers' AND state = 'closed'
LIMIT 5;
```

Full issue details with body, timestamps, labels, and comment count:

```sql
SELECT number, title, state, comments,
       body,
       user_col->>'login' AS author,
       labels,
       html_url,
       created_at, updated_at, closed_at
FROM repo_issues
WHERE owner = 'supabase' AND repo = 'wrappers'
LIMIT 3;
```

## 6. Pull Requests

Pull requests with state filtering via query pushdown:

```sql
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
```

```sql
SELECT number, title, state
FROM repo_pulls
WHERE owner = 'supabase' AND repo = 'wrappers' AND state = 'closed'
LIMIT 5;
```

| number | title | state |
| --- | --- | --- |
| 572 | docs(openapi): update wasm module checksum and improve docs | closed |
| 570 | chore(deps): bump time from 0.3.44 to 0.3.47 in the cargo group across 1 directory | closed |
| 569 | feat: add comprehensive AI assistant guide for Wrappers project | closed |
| 568 | chore(deps): bump bytes from 1.10.1 to 1.11.1 in the cargo group across 1 directory | closed |
| 567 | chore(deps): bump wasmtime from 36.0.3 to 36.0.5 in the cargo group across 1 directory | closed |

PR details with draft status, branch info, and merge timestamp:

```sql
SELECT number, title, state, draft,
       user_col->>'login' AS author,
       head->>'ref' AS source_branch,
       base->>'ref' AS target_branch,
       html_url,
       created_at, merged_at
FROM repo_pulls
WHERE owner = 'supabase' AND repo = 'wrappers' AND state = 'closed'
LIMIT 5;
```

Open PRs only:

```sql
SELECT number, title, draft,
       user_col->>'login' AS author,
       created_at
FROM repo_pulls
WHERE owner = 'supabase' AND repo = 'wrappers' AND state = 'open'
LIMIT 5;
```

## 7. Releases

Paginated list of releases for a repository:

```sql
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
```

```sql
SELECT tag_name, name, prerelease
FROM repo_releases
WHERE owner = 'supabase' AND repo = 'wrappers'
LIMIT 5;
```

| tag_name | name | prerelease |
| --- | --- | --- |
| wasm_openapi_fdw_v0.1.4 | wasm_openapi_fdw_v0.1.4 | f |
| wasm_snowflake_fdw_v0.2.1 | wasm_snowflake_fdw_v0.2.1 | f |
| wasm_infura_fdw_v0.1.0 | wasm_infura_fdw_v0.1.0 | f |
| wasm_clerk_fdw_v0.2.1 | wasm_clerk_fdw_v0.2.1 | f |
| v0.5.7 | v0.5.7 | f |

Full release info with author, publish date, and release notes:

```sql
SELECT tag_name, name, draft, prerelease,
       author->>'login' AS author,
       published_at,
       left(body, 200) AS release_notes,
       html_url
FROM repo_releases
WHERE owner = 'supabase' AND repo = 'wrappers'
LIMIT 3;
```

## 8. Search Repositories (Query Pushdown)

When a WHERE clause references `q`, the FDW sends it as a query parameter to the `/search/repositories` endpoint. The FDW auto-detects the `items` wrapper key in the response.

```sql
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
```

```sql
-- Pushes down to: GET /search/repositories?q=openapi+foreign+data+wrapper
SELECT name, full_name, stargazers_count
FROM search_repos
WHERE q = 'openapi foreign data wrapper'
LIMIT 5;
```

| name | full_name | stargazers_count |
| --- | --- | --- |
| openapi_fdw | sabino/openapi_fdw | 2 |
| openapi-fdw | user/openapi-fdw | 1 |
| fdw-api | user/fdw-api | 0 |

Search with full detail — description, license, topics, and timestamps:

```sql
SELECT name, full_name, description, language,
       stargazers_count, forks_count, open_issues_count,
       license->>'name' AS license,
       topics,
       html_url, created_at
FROM search_repos
WHERE q = 'postgres foreign data wrapper rust'
LIMIT 5;
```

## 9. Debug Mode

The `search_repos_debug` table uses the `github_debug` server which has `debug 'true'`. This emits HTTP request details as PostgreSQL INFO messages.

```sql
SELECT id FROM search_repos_debug WHERE q = 'supabase' LIMIT 1;
```

Look for INFO output like:

```log
INFO:  [openapi_fdw] HTTP GET https://api.github.com/search/repositories?per_page=30&q=supabase -> 200 (176333 bytes)
INFO:  [openapi_fdw] Scan complete: 1 rows, 2 columns
```

## 10. The `attrs` Column

Every table includes an `attrs jsonb` column that captures all fields not mapped to named columns:

```sql
SELECT name, attrs->>'visibility' AS visibility,
       attrs->>'has_wiki' AS has_wiki
FROM my_repos
LIMIT 3;
```

| name | visibility | has_wiki |
| --- | --- | --- |
| my-project | public | true |
| dotfiles | public | false |
| cool-app | public | true |

## Features Demonstrated

| Feature | Table(s) |
| --- | --- |
| IMPORT FOREIGN SCHEMA | `github_import` server |
| Bearer token auth (Authorization header) | All tables |
| Custom HTTP headers (X-GitHub-Api-Version) | All tables |
| Page-based pagination (auto-detected) | `my_repos`, `repo_issues`, `repo_pulls`, `repo_releases`, `search_repos` |
| Path parameter substitution | `repo_detail`, `repo_issues`, `repo_pulls`, `repo_releases` |
| Query parameter pushdown | `my_repos` (`type`, `sort`), `repo_issues` (`state`), `repo_pulls` (`state`), `search_repos` (`q`) |
| Single object response | `my_profile`, `repo_detail` |
| Auto-detected wrapper key (`items`) | `search_repos`, `search_repos_debug` |
| Type coercion (timestamptz, boolean, bigint) | All tables |
| Debug mode | `search_repos_debug` |
| `attrs` catch-all column | All tables |
| `rowid_column` | `my_repos`, `repo_issues`, `repo_pulls`, `repo_releases`, `search_repos` |
