# Security

## Remote Servers

Your FDWs will inherit the security model of the credentials provided when you create the remote server. For example, take this Stripe Wrapper:

```sql
create server stripe_server
foreign data wrapper stripe_wrapper
options (
    api_key '<Stripe API Key>',  -- Stripe API key, required
    api_url 'https://api.stripe.com/v1/',
    api_version '2024-06-20'
);
```

The Wrapper will be able to access any data that the `api_key` has permission to access.

## Row Level Security

Foreign Data Wrappers do not provide Row Level Security. Wrappers should _always_ be stored in a private schema. For example, if you are connecting to your Stripe account, you should create a `stripe` schema to store all of your foreign tables inside. This schema should have a restrictive set of grants.

## Exposing foreign data

If you want to expose any of the foreign table columns through a public API, we recommend using a [Postgres Function with `security definer`](https://supabase.com/docs/guides/database/functions#security-definer-vs-invoker). For better access control, the function should have appropriate filters on the foreign table to apply security rules based on your business needs.

For example, if you wanted to expose all of your Stripe Products through an API:

Create a Stripe Products foreign table:

```sql
create foreign table stripe.stripe_products (
    id text,
    name text,
    active bool,
    default_price text,
    description text
)
server stripe_fdw_server
options (
    object 'products',
    rowid_column 'id'
);
```

Create a `security definer` function that queries the foreign table and filters on the name prefix parameter:

```sql
create function public.get_stripe_products(name_prefix text)
returns table (
    id text,
    name text,
    active boolean,
    default_price text,
    description text
)
language plpgsql
security definer set search_path = ''
as $$
begin
    return query
    select
        id,
        name,
        active,
        default_price,
        description
    from
        stripe.stripe_products
    where
        name like name_prefix || '%'
    ;
end;
$$;
```

Restrict the function execution to a specific role only. For example, if you have an `authenticated` role in Postgres, revoke access to everything except that one role:

```sql
-- revoke public execute permission
revoke execute on function public.get_stripe_products from public;
revoke execute on function public.get_stripe_products from anon;

-- grant execute permission to a specific role only
grant execute on function public.get_stripe_products to authenticated;
```
