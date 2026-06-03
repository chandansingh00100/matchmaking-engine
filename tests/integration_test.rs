use matchmaking_engine::{EngineConfig, MatchmakingEngine};

#[test]
fn enqueue_players() {
    let config = EngineConfig {
        enable_stats_reporter: false,
        ..Default::default()
    };

    let engine = MatchmakingEngine::new(config);

    for i in 0..10 {
        assert!(engine.enqueue(i, 1000));
    }

    assert_eq!(engine.queue_size(), 10);
}
