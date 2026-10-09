use chrono::{DateTime, Utc};
use uuid::Uuid;

pub struct Job<S> {
    id: Uuid,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    fail_count: u16,
    max_failures: u16,
    payload: String,
    state: S,
}

pub struct Pending;

pub struct Running {
    lease_expiration: DateTime<Utc>,
    worker_id: Uuid,
}

pub struct Completed {
    worker_id: Uuid,
}

pub struct Dead {
    last_worker_id: Uuid,
    last_error: JobError,
}

pub type PendingJob = Job<Pending>;
pub type RunningJob = Job<Running>;
pub type CompletedJob = Job<Completed>;
pub type DeadJob = Job<Dead>;

impl PendingJob {
    pub fn new(payload: String, max_failures: u16, creation_time: DateTime<Utc>) -> Self {
        PendingJob {
            id: Uuid::new_v4(),
            created_at: creation_time,
            updated_at: creation_time,
            fail_count: 0,
            max_failures,
            payload,
            state: Pending,
        }
    }

    //helper to create job with now creation time
    pub fn new_now(payload: String, max_failures: u16) -> Self {
        Self::new(payload, max_failures, Utc::now())
    }

    /// Claims the job for `worker_id`
    pub fn run(self, worker_id: Uuid, lease_duration: std::time::Duration) -> RunningJob {
        let now = Utc::now();
        RunningJob {
            id: self.id,
            created_at: self.created_at,
            updated_at: now,
            fail_count: self.fail_count,
            max_failures: self.max_failures,
            payload: self.payload,
            state: Running {
                lease_expiration: now + lease_duration,
                worker_id,
            },
        }
    }
}

/// Result of a failed attempt: retried while attempts remain, dead-lettered after.
pub enum FailOutcome {
    Retry(PendingJob),
    Dead(DeadJob),
}

impl RunningJob {
    pub fn complete(self) -> CompletedJob {
        Job {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            fail_count: self.fail_count,
            max_failures: self.max_failures,
            payload: self.payload,
            state: Completed {
                worker_id: self.state.worker_id,
            },
        }
    }

    pub fn fail(self, error: JobError) -> FailOutcome {
        let fail_count = self.fail_count + 1;
        if self.fail_count < self.max_failures {
            FailOutcome::Retry(Job {
                id: self.id,
                created_at: self.created_at,
                updated_at: self.updated_at,
                fail_count,
                max_failures: self.max_failures,
                payload: self.payload,
                state: Pending,
            })
        } else {
            let last_worker_id = self.state.worker_id;
            FailOutcome::Dead(Job {
                id: self.id,
                created_at: self.created_at,
                updated_at: self.updated_at,
                fail_count,
                max_failures: self.max_failures,
                payload: self.payload,
                state: Dead {
                    last_worker_id,
                    last_error: error,
                },
            })
        }
    }

    /// Treats a lease that ran out as a failed attempt.
    pub fn expire(self) -> FailOutcome {
        self.fail(JobError::LeaseExpired)
    }

    pub fn is_lease_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.state.lease_expiration
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobError {
    LeaseExpired,
    Failed(String),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const LEASE: Duration = Duration::from_millis(500);

    fn running(max_failures: u16) -> RunningJob {
        PendingJob::new("foo".to_owned(), max_failures, Utc::now()).run(Uuid::new_v4(), LEASE)
    }

    #[test]
    fn transition_pending_to_running() {
        let p = PendingJob::new("foo".to_owned(), 3, Utc::now());
        let id = p.id;
        let wid = Uuid::new_v4();
        let r = p.run(wid, LEASE);
        assert_eq!(r.id, id);
        assert_eq!(r.fail_count, 0);
        assert_eq!(r.max_failures, 3);
        assert_eq!(r.payload, "foo");
        assert_eq!(r.state.worker_id, wid);
        assert!(!r.is_lease_expired(r.updated_at));
        assert!(r.is_lease_expired(r.state.lease_expiration));
    }

    #[test]
    fn transition_running_to_completed() {
        let r = running(3);
        let (id, wid) = (r.id, r.state.worker_id);
        let c = r.complete();
        assert_eq!(c.id, id);
        assert_eq!(c.fail_count, 0);
        assert_eq!(c.state.worker_id, wid);
    }

    #[test]
    fn fail_with_failures_left_retries() {
        let r = running(2);
        let id = r.id;
        match r.fail(JobError::Failed("boom".into())) {
            FailOutcome::Retry(p) => {
                assert_eq!(p.id, id);
                assert_eq!(p.fail_count, 1);
            }
            FailOutcome::Dead(_) => panic!("expected retry"),
        }
    }

    #[test]
    fn fail_with_zero_max_failures_dead_letters() {
        let r = running(0);
        let wid = r.state.worker_id;
        match r.fail(JobError::Failed("boom".into())) {
            FailOutcome::Dead(d) => {
                assert_eq!(d.fail_count, 1);
                assert_eq!(d.state.last_worker_id, wid);
                assert_eq!(d.state.last_error, JobError::Failed("boom".into()));
            }
            FailOutcome::Retry(_) => panic!("expected dead"),
        }
    }

    #[test]
    fn retries_until_max_failures_exceeded() {
        let max_failures = 3;
        let mut job = PendingJob::new("foo".to_owned(), max_failures, Utc::now());
        for failure in 1..=max_failures + 1 {
            match job.run(Uuid::new_v4(), LEASE).expire() {
                FailOutcome::Retry(p) => {
                    assert_eq!(p.fail_count, failure);
                    job = p;
                }
                FailOutcome::Dead(d) => {
                    assert_eq!(failure, max_failures + 1);
                    assert_eq!(d.fail_count, failure);
                    assert_eq!(d.state.last_error, JobError::LeaseExpired);
                    return;
                }
            }
        }
        panic!("job never dead-lettered");
    }
}
