create schema if not exists mev;

create table if not exists mev.v2_factories (
    address bytea primary key
);

create table if not exists mev.v3_factories (
    address bytea primary key
);

create table if not exists mev.v4_factories (
    address bytea primary key
);

create table if not exists mev.v2_pools (
    factory bytea not null references mev.v2_factories(address),
    pair_address bytea primary key,
    token0 bytea not null,
    token1 bytea not null
);

create table if not exists mev.v3_pools (
    factory bytea not null references mev.v3_factories(address),
    pool_address bytea primary key,
    token0 bytea not null,
    token1 bytea not null,
    fee integer not null,
    tick_spacing integer not null
);

create table if not exists mev.v4_pools (
    factory bytea not null references mev.v4_factories(address),
    currency0 bytea not null,
    currency1 bytea not null,
    fee integer not null,
    tick_spacing integer not null,
    hooks bytea not null,
    sqrt_price_x96 numeric not null,
    primary key (currency0, currency1, fee, tick_spacing, hooks)
);

create table if not exists mev.backfill_checkpoint (
    job_name text primary key,
    last_scanned_block bigint not null
);
