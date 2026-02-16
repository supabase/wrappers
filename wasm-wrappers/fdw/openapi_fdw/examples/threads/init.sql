-- OpenAPI FDW example: Threads API (Meta)
-- Requires a Threads access token (set THREADS_ACCESS_TOKEN env var).
-- See: https://developers.facebook.com/docs/threads
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
-- Server 1: threads — Main Threads API server
-- Auth via access_token query parameter
-- ============================================================
create server threads
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://graph.threads.net',
    api_key 'placeholder',
    api_key_header 'access_token',
    api_key_location 'query'
  );

-- ============================================================
-- Server 2: threads_debug — Same API with debug output
-- ============================================================
create server threads_debug
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://graph.threads.net',
    api_key 'placeholder',
    api_key_header 'access_token',
    api_key_location 'query',
    debug 'true'
  );

-- ============================================================
-- Server 3: threads_import — With inline spec for IMPORT FOREIGN SCHEMA
-- ============================================================
create server threads_import
  foreign data wrapper wasm_wrapper
  options (
    fdw_package_url 'file:///openapi_fdw.wasm',
    fdw_package_name 'supabase:openapi-fdw',
    fdw_package_version '0.2.0',
    base_url 'https://graph.threads.net',
    api_key 'placeholder',
    api_key_header 'access_token',
    api_key_location 'query',
    spec_json '{"openapi":"3.0.0","info":{"title":"Threads API","version":"1.0.0"},"servers":[{"url":"https://graph.threads.net"}],"paths":{"/me":{"get":{"parameters":[{"name":"fields","in":"query","schema":{"type":"string"}}],"responses":{"default":{"content":{"application/json":{"schema":{"type":"object","properties":{"id":{"type":"string"},"username":{"type":"string"},"name":{"type":"string"},"threads_profile_picture_url":{"type":"string"},"threads_biography":{"type":"string"}}}}}}}}},"/me/threads":{"get":{"parameters":[{"name":"fields","in":"query","schema":{"type":"string"}},{"name":"since","in":"query","schema":{"type":"string"}},{"name":"until","in":"query","schema":{"type":"string"}},{"name":"limit","in":"query","schema":{"type":"integer"}},{"name":"before","in":"query","schema":{"type":"string"}},{"name":"after","in":"query","schema":{"type":"string"}}],"responses":{"default":{"content":{"application/json":{"schema":{"type":"object","properties":{"data":{"type":"array","items":{"type":"object","properties":{"id":{"type":"string"},"media_product_type":{"type":"string"},"media_type":{"type":"string"},"permalink":{"type":"string"},"owner":{"type":"object","properties":{"id":{"type":"string"}}},"username":{"type":"string"},"text":{"type":"string"},"timestamp":{"type":"string","format":"date-time"},"shortcode":{"type":"string"},"is_quote_post":{"type":"boolean"},"has_replies":{"type":"boolean"},"poll_attachment":{"type":"object","properties":{"option_a":{"type":"string"},"option_b":{"type":"string"},"option_c":{"type":"string"},"option_d":{"type":"string"},"option_a_votes_percentage":{"type":"number"},"option_b_votes_percentage":{"type":"number"},"option_c_votes_percentage":{"type":"number"},"option_d_votes_percentage":{"type":"number"},"expiration_timestamp":{"type":"string","format":"date-time"}}}}}},"paging":{"type":"object","properties":{"cursors":{"type":"object","properties":{"before":{"type":"string"},"after":{"type":"string"}}}}}}}}}}}}},"/me/replies":{"get":{"parameters":[{"name":"fields","in":"query","schema":{"type":"string"}},{"name":"since","in":"query","schema":{"type":"string"}},{"name":"until","in":"query","schema":{"type":"string"}},{"name":"limit","in":"query","schema":{"type":"integer"}},{"name":"before","in":"query","schema":{"type":"string"}},{"name":"after","in":"query","schema":{"type":"string"}}],"responses":{"default":{"content":{"application/json":{"schema":{"type":"object","properties":{"data":{"type":"array","items":{"type":"object","properties":{"id":{"type":"string"},"media_product_type":{"type":"string"},"media_type":{"type":"string"},"permalink":{"type":"string"},"username":{"type":"string"},"text":{"type":"string"},"timestamp":{"type":"string","format":"date-time"},"shortcode":{"type":"string"},"is_quote_post":{"type":"boolean"},"has_replies":{"type":"boolean"}}}},"paging":{"type":"object","properties":{"cursors":{"type":"object","properties":{"before":{"type":"string"},"after":{"type":"string"}}}}}}}}}}}}},"/{thread_id}/replies":{"get":{"parameters":[{"name":"fields","in":"query","schema":{"type":"string"}},{"name":"reverse","in":"query","schema":{"type":"boolean"}}],"responses":{"default":{"content":{"application/json":{"schema":{"type":"object","properties":{"data":{"type":"array","items":{"type":"object","properties":{"id":{"type":"string"},"text":{"type":"string"},"timestamp":{"type":"string","format":"date-time"},"media_product_type":{"type":"string"},"media_type":{"type":"string"},"permalink":{"type":"string"},"shortcode":{"type":"string"},"username":{"type":"string"},"is_quote_post":{"type":"boolean"},"has_replies":{"type":"boolean"},"is_reply":{"type":"boolean"},"is_reply_owned_by_me":{"type":"boolean"},"root_post":{"type":"object","properties":{"id":{"type":"string"}}},"replied_to":{"type":"object","properties":{"id":{"type":"string"}}}}}},"paging":{"type":"object","properties":{"cursors":{"type":"object","properties":{"before":{"type":"string"},"after":{"type":"string"}}}}}}}}}}}}},"/{thread_id}/conversation":{"get":{"parameters":[{"name":"fields","in":"query","schema":{"type":"string"}},{"name":"reverse","in":"query","schema":{"type":"boolean"}}],"responses":{"default":{"content":{"application/json":{"schema":{"type":"object","properties":{"data":{"type":"array","items":{"type":"object","properties":{"id":{"type":"string"},"text":{"type":"string"},"timestamp":{"type":"string","format":"date-time"},"media_product_type":{"type":"string"},"media_type":{"type":"string"},"permalink":{"type":"string"},"shortcode":{"type":"string"},"username":{"type":"string"},"is_quote_post":{"type":"boolean"},"has_replies":{"type":"boolean"},"is_reply":{"type":"boolean"},"is_reply_owned_by_me":{"type":"boolean"},"root_post":{"type":"object","properties":{"id":{"type":"string"}}},"replied_to":{"type":"object","properties":{"id":{"type":"string"}}}}}},"paging":{"type":"object","properties":{"cursors":{"type":"object","properties":{"before":{"type":"string"},"after":{"type":"string"}}}}}}}}}}}}},"/keyword_search":{"get":{"parameters":[{"name":"q","in":"query","schema":{"type":"string"}},{"name":"search_type","in":"query","schema":{"type":"string"}},{"name":"fields","in":"query","schema":{"type":"string"}},{"name":"search_mode","in":"query","schema":{"type":"string"}},{"name":"limit","in":"query","schema":{"type":"integer"}},{"name":"since","in":"query","schema":{"type":"string"}},{"name":"until","in":"query","schema":{"type":"string"}}],"responses":{"default":{"content":{"application/json":{"schema":{"type":"object","properties":{"data":{"type":"array","items":{"type":"object","properties":{"id":{"type":"string"},"media_product_type":{"type":"string"},"media_type":{"type":"string"},"permalink":{"type":"string"},"username":{"type":"string"},"text":{"type":"string"},"timestamp":{"type":"string","format":"date-time"},"shortcode":{"type":"string"},"is_quote_post":{"type":"boolean"},"has_replies":{"type":"boolean"},"is_reply":{"type":"boolean"},"topic_tag":{"type":"string"}}}},"paging":{"type":"object","properties":{"cursors":{"type":"object","properties":{"before":{"type":"string"},"after":{"type":"string"}}}}}}}}}}}}},"/profile_lookup":{"get":{"parameters":[{"name":"username","in":"query","schema":{"type":"string"}}],"responses":{"default":{"content":{"application/json":{"schema":{"type":"object","properties":{"username":{"type":"string"},"name":{"type":"string"},"threads_profile_picture_url":{"type":"string"},"threads_biography":{"type":"string"},"follower_count":{"type":"integer"},"is_verified":{"type":"boolean"},"likes_count":{"type":"integer"},"quotes_count":{"type":"integer"},"replies_count":{"type":"integer"},"reposts_count":{"type":"integer"},"views_count":{"type":"integer"}}}}}}}}},"/me/threads_publishing_limit":{"get":{"parameters":[{"name":"fields","in":"query","schema":{"type":"string"}}],"responses":{"default":{"content":{"application/json":{"schema":{"type":"object","properties":{"data":{"type":"array","items":{"type":"object","properties":{"quota_usage":{"type":"integer"},"config":{"type":"object","properties":{"quota_total":{"type":"integer"},"quota_duration":{"type":"integer"}}},"reply_quota_usage":{"type":"integer"},"reply_config":{"type":"object","properties":{"quota_total":{"type":"integer"},"quota_duration":{"type":"integer"}}}}}}}}}}}}}}}}'
  );

-- ============================================================
-- Table 1: my_profile
-- Single object response — GET /me with profile fields
-- Features: api_key in query param, field selection via endpoint query string
-- ============================================================
create foreign table my_profile (
  id text,
  username text,
  name text,
  threads_profile_picture_url text,
  threads_biography text,
  is_verified boolean,
  attrs jsonb
)
  server threads
  options (
    endpoint '/me?fields=id,username,name,threads_profile_picture_url,threads_biography,is_verified',
    rowid_column 'id'
  );

-- ============================================================
-- Table 2: my_threads
-- Paginated list of the authenticated user's posts
-- Features: cursor-based pagination (auto-detected via data/paging),
--           timestamptz coercion, boolean coercion, attrs catch-all
-- ============================================================
create foreign table my_threads (
  id text,
  media_type text,
  text text,
  permalink text,
  username text,
  timestamp timestamptz,
  shortcode text,
  is_quote_post boolean,
  topic_tag text,
  link_attachment_url text,
  is_verified boolean,
  attrs jsonb
)
  server threads
  options (
    endpoint '/me/threads?fields=id,media_type,media_product_type,text,permalink,username,timestamp,shortcode,is_quote_post,topic_tag,link_attachment_url,is_verified',
    rowid_column 'id'
  );

-- ============================================================
-- Table 3: my_replies
-- Paginated list of the authenticated user's replies
-- Features: same pagination as my_threads, reply-specific fields
-- ============================================================
create foreign table my_replies (
  id text,
  media_type text,
  text text,
  permalink text,
  username text,
  timestamp timestamptz,
  shortcode text,
  is_quote_post boolean,
  has_replies boolean,
  is_reply boolean,
  attrs jsonb
)
  server threads
  options (
    endpoint '/me/replies?fields=id,media_type,text,permalink,username,timestamp,shortcode,is_quote_post,has_replies,is_reply',
    rowid_column 'id'
  );

-- ============================================================
-- Table 4: thread_detail
-- Single media object by ID — path parameter substitution
-- Features: path param {thread_id}, single object response
-- ============================================================
create foreign table thread_detail (
  id text,
  media_type text,
  text text,
  permalink text,
  username text,
  timestamp timestamptz,
  is_quote_post boolean,
  has_replies boolean,
  topic_tag text,
  link_attachment_url text,
  reply_audience text,
  thread_id text,
  attrs jsonb
)
  server threads
  options (
    endpoint '/{thread_id}?fields=id,media_type,text,permalink,username,timestamp,is_quote_post,has_replies,topic_tag,link_attachment_url,reply_audience',
    rowid_column 'id'
  );

-- ============================================================
-- Table 5: thread_replies
-- Top-level replies to a specific thread — path param + pagination
-- Features: path param, reverse chronological order, reply metadata
-- ============================================================
create foreign table thread_replies (
  id text,
  text text,
  username text,
  permalink text,
  timestamp timestamptz,
  media_type text,
  has_replies boolean,
  is_reply boolean,
  hide_status text,
  is_verified boolean,
  thread_id text,
  attrs jsonb
)
  server threads
  options (
    endpoint '/{thread_id}/replies?fields=id,text,username,permalink,timestamp,media_type,has_replies,is_reply,hide_status,is_verified',
    rowid_column 'id'
  );

-- ============================================================
-- Table 6: thread_conversation
-- Full flattened conversation (all reply depths)
-- Features: path param, all-depth replies, chronological ordering
-- ============================================================
create foreign table thread_conversation (
  id text,
  text text,
  username text,
  permalink text,
  timestamp timestamptz,
  media_type text,
  has_replies boolean,
  is_reply boolean,
  hide_status text,
  thread_id text,
  attrs jsonb
)
  server threads
  options (
    endpoint '/{thread_id}/conversation?fields=id,text,username,permalink,timestamp,media_type,has_replies,is_reply,hide_status&reverse=false',
    rowid_column 'id'
  );

-- ============================================================
-- Table 7: keyword_search
-- Search for public threads by keyword or topic tag
-- Features: query param pushdown (q, search_type, search_mode)
-- ============================================================
create foreign table keyword_search (
  id text,
  text text,
  media_type text,
  permalink text,
  username text,
  timestamp timestamptz,
  has_replies boolean,
  is_quote_post boolean,
  is_reply boolean,
  topic_tag text,
  q text,
  attrs jsonb
)
  server threads
  options (
    endpoint '/keyword_search?fields=id,text,media_type,permalink,username,timestamp,has_replies,is_quote_post,is_reply,topic_tag',
    rowid_column 'id'
  );

-- ============================================================
-- Table 8: profile_lookup
-- Look up a public profile by username
-- Features: query param pushdown (username), single object response
-- ============================================================
create foreign table profile_lookup (
  username text,
  name text,
  biography text,
  profile_picture_url text,
  follower_count bigint,
  is_verified boolean,
  likes_count bigint,
  quotes_count bigint,
  reposts_count bigint,
  views_count bigint,
  attrs jsonb
)
  server threads
  options (
    endpoint '/profile_lookup',
    rowid_column 'username'
  );

-- ============================================================
-- Table 9: publishing_limit
-- Check the user's current publishing rate limit
-- Features: nested data array response
-- ============================================================
create foreign table publishing_limit (
  quota_usage integer,
  config jsonb,
  reply_quota_usage integer,
  reply_config jsonb,
  attrs jsonb
)
  server threads
  options (
    endpoint '/me/threads_publishing_limit?fields=quota_usage,config,reply_quota_usage,reply_config'
  );

-- ============================================================
-- Table 10: keyword_search_debug
-- Same as keyword_search but on the debug server
-- Features: debug output in INFO messages
-- ============================================================
create foreign table keyword_search_debug (
  id text,
  text text,
  username text,
  timestamp timestamptz,
  q text,
  attrs jsonb
)
  server threads_debug
  options (
    endpoint '/keyword_search?fields=id,text,username,timestamp',
    rowid_column 'id'
  );
