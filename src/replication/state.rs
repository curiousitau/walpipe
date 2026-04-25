//! Replication state management
//!
//! Provides state tracking for PostgreSQL logical replication connections,
//! including schema information, LSN positions, and feedback timing.


// Re-export the ReplicationState from protocol messages for convenience
pub use crate::protocol::messages::ReplicationState;


#[cfg(test)]
mod tests {
    use crate::protocol::messages::RelationInfo;
    use crate::utils::binary::Oid;

    use super::*;

    #[test]
    fn test_replication_state_creation() {
        let state = ReplicationState::new();
        assert_eq!(state.received_lsn, 0);
        assert_eq!(state.applied_lsn, 0);
        assert_eq!(state.relations.len(), 0);
    }

    #[test]
    fn test_lsn_updates() {
        let mut state = ReplicationState::new();

        // Test received LSN updates
        state.update_lsn(100);
        assert_eq!(state.received_lsn, 100);

        // Test that lower LSN doesn't override higher one
        state.update_lsn(50);
        assert_eq!(state.received_lsn, 100);

        // Test applied LSN updates
        state.update_applied_lsn(80);
        assert_eq!(state.applied_lsn, 80);

        // Zero should be ignored
        state.update_lsn(0);
        assert_eq!(state.received_lsn, 100);
    }

    #[test]
    fn test_feedback_timing() {
        let mut state = ReplicationState::new();
        let initial = state.last_feedback_time;

        std::thread::sleep(std::time::Duration::from_millis(10));
        state.update_feedback_time();
        assert!(state.last_feedback_time > initial);
    }

    #[test]
    fn test_relation_management() {
        let mut state = ReplicationState::new();

        let relation = RelationInfo {
            oid: 12345 as Oid,
            namespace: "public".to_string(),
            relation_name: "test_table".to_string(),
            replica_identity: 'd',
            column_count: 2,
            columns: vec![],
        };

        // Add relation
        state.add_relation(relation.clone());

        // Retrieve relation
        let retrieved = state.get_relation(12345 as Oid);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().relation_name, "test_table");

        // Non-existent relation should return None
        assert!(state.get_relation(99999 as Oid).is_none());
    }
}
