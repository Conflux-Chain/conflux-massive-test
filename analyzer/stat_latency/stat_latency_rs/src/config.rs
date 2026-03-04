use std::collections::HashSet;

pub fn default_latency_key_names() -> HashSet<&'static str> {
    let mut set = HashSet::new();
    set.insert("Receive");
    set.insert("Sync");
    set.insert("Cons");

    set.insert("HeaderReady");
    set.insert("BodyReady");
    set.insert("SyncGraph");
    set.insert("ConsensusGraphStart");
    set.insert("ConsensusGraphReady");
    set.insert("ComputeEpoch");
    set.insert("NotifyTxPool");
    set.insert("TxPoolUpdated");
    set
}

pub fn pivot_event_key_names() -> HashSet<&'static str> {
    let mut set = HashSet::new();
    set.insert("ComputeEpoch");
    set.insert("NotifyTxPool");
    set.insert("TxPoolUpdated");
    set
}
