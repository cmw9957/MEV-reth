use alloy_primitives::Address;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct V2PoolMeta {
    pub(crate) factory: Address,
    pub(crate) pair_address: Address,
    pub(crate) token0: Address,
    pub(crate) token1: Address,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct V3PoolMeta {
    pub(crate) factory: Address,
    pub(crate) pool_address: Address,
    pub(crate) token0: Address,
    pub(crate) token1: Address,
    pub(crate) fee: u32,
    pub(crate) tick_spacing: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct V4PoolKey {
    pub(crate) currency0: Address,
    pub(crate) currency1: Address,
    pub(crate) fee: u32,
    pub(crate) tick_spacing: i32,
    pub(crate) hooks: Address,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V4PoolMeta {
    pub(crate) factory: Address,
    pub(crate) key: V4PoolKey,
    pub(crate) sqrt_price_x96: String,
}
