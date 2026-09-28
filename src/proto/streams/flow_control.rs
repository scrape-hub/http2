use crate::frame::Reason;
use crate::proto::{WindowSize, MAX_WINDOW_SIZE};
use crate::tracing;

use std::fmt;

// tokio::time::Instant wraps std::time::Instant and behaves identically to it
// outside of a paused tokio-test clock; using it here (instead of
// std::time::Instant) is what lets a test control this policy's timing with
// tokio::time::pause()/advance() instead of a real sleep.
use tokio::time::{Duration, Instant};

// We don't want to send WINDOW_UPDATE frames for tiny changes, but instead
// aggregate them when the changes are significant. Many implementations do
// this by keeping a "ratio" of the update version the allowed window size.
//
// While some may wish to represent this ratio as percentage, using a f32,
// we skip having to deal with float math and stick to integers. To do so,
// the "ratio" is represented by 2 i32s, split into the numerator and
// denominator. For example, a 50% ratio is simply represented as 1/2.
//
// An example applying this ratio: If a stream has an allowed window size of
// 100 bytes, WINDOW_UPDATE frames are scheduled when the unclaimed change
// becomes greater than 1/2, or 50 bytes.
const UNCLAIMED_NUMERATOR: i32 = 1;
const UNCLAIMED_DENOMINATOR: i32 = 2;

#[test]
#[allow(clippy::assertions_on_constants)]
fn sanity_unclaimed_ratio() {
    assert!(UNCLAIMED_NUMERATOR < UNCLAIMED_DENOMINATOR);
    assert!(UNCLAIMED_NUMERATOR >= 0);
    assert!(UNCLAIMED_DENOMINATOR > 0);
}

/// When a receive window is topped up with a WINDOW_UPDATE frame.
///
/// By default a WINDOW_UPDATE goes out once the capacity the application
/// released reaches half of the window the peer still has. A policy replaces
/// that rule: an update goes out once the released capacity reaches the
/// [`threshold`](Self::threshold), once the peer's window falls to the
/// [`low_window`](Self::low_window), or when capacity is released after the
/// [`interval`](Self::interval) has passed since the last update. Each update
/// returns all released capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowUpdatePolicy {
    threshold: WindowSize,
    low_window: Option<WindowSize>,
    interval: Option<Duration>,
}

impl WindowUpdatePolicy {
    /// Send a WINDOW_UPDATE once `bytes` of released capacity add up.
    pub fn threshold(bytes: u32) -> Self {
        WindowUpdatePolicy {
            threshold: bytes,
            low_window: None,
            interval: None,
        }
    }

    /// Also send one as soon as the window the peer knows is down to `bytes`
    /// or less (and some capacity was released).
    pub fn low_window(mut self, bytes: u32) -> Self {
        self.low_window = Some(bytes);
        self
    }

    /// Also send one when capacity is released and `interval` has passed
    /// since the last WINDOW_UPDATE of the window (since its creation before
    /// the first).
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }
}

#[derive(Copy, Clone, Debug)]
pub struct FlowControl {
    /// Window the peer knows about.
    ///
    /// This can go negative if a SETTINGS_INITIAL_WINDOW_SIZE is received.
    ///
    /// For example, say the peer sends a request and uses 32kb of the window.
    /// We send a SETTINGS_INITIAL_WINDOW_SIZE of 16kb. The peer has to adjust
    /// its understanding of the capacity of the window, and that would be:
    ///
    /// ```notrust
    /// default (64kb) - used (32kb) - settings_diff (64kb - 16kb): -16kb
    /// ```
    window_size: Window,

    /// Window that we know about.
    ///
    /// This can go negative if a user declares a smaller target window than
    /// the peer knows about.
    available: Window,

    /// When a receive window sends WINDOW_UPDATE frames; `None` for the
    /// default ratio.
    update_policy: Option<WindowUpdatePolicy>,

    /// When the last WINDOW_UPDATE went out, or when the policy was set.
    last_update: Option<Instant>,

    /// A WINDOW_UPDATE is due whatever the threshold says.
    update_due: bool,
}

impl FlowControl {
    pub fn new() -> FlowControl {
        FlowControl {
            window_size: Window(0),
            available: Window(0),
            update_policy: None,
            last_update: None,
            update_due: false,
        }
    }

    /// Replace the default WINDOW_UPDATE rule of a receive window.
    pub fn set_update_policy(&mut self, policy: WindowUpdatePolicy) {
        self.update_policy = Some(policy);
        self.last_update = Some(Instant::now());
    }

    /// Capacity was released: with an interval policy, an update is due if
    /// the interval has passed.
    pub fn note_release(&mut self) {
        let interval = self.update_policy.and_then(|policy| policy.interval);
        if let (Some(interval), Some(last)) = (interval, self.last_update) {
            if last.elapsed() >= interval {
                self.update_due = true;
            }
        }
    }

    /// With a policy, send the next WINDOW_UPDATE regardless of its
    /// threshold. The default rule is left as it is.
    pub fn force_update(&mut self) {
        if self.update_policy.is_some() {
            self.update_due = true;
        }
    }

    /// A WINDOW_UPDATE for this window went out.
    pub fn window_update_sent(&mut self) {
        self.update_due = false;
        if self.update_policy.is_some() {
            self.last_update = Some(Instant::now());
        }
    }

    /// Returns the window size as known by the peer
    pub fn window_size(&self) -> WindowSize {
        self.window_size.as_size()
    }

    /// Returns the window size available to the consumer
    pub fn available(&self) -> Window {
        self.available
    }

    /// Returns true if there is unavailable window capacity
    pub fn has_unavailable(&self) -> bool {
        if self.window_size < 0 {
            return false;
        }

        self.window_size > self.available
    }

    pub fn claim_capacity(&mut self, capacity: WindowSize) -> Result<(), Reason> {
        self.available.decrease_by(capacity)
    }

    pub fn assign_capacity(&mut self, capacity: WindowSize) -> Result<(), Reason> {
        self.available.increase_by(capacity)
    }

    /// If a WINDOW_UPDATE frame should be sent, returns a positive number
    /// representing the increment to be used.
    ///
    /// If there is no available bytes to be reclaimed, or the number of
    /// available bytes does not reach the threshold, this returns `None`.
    ///
    /// This represents pending outbound WINDOW_UPDATE frames.
    pub fn unclaimed_capacity(&self) -> Option<WindowSize> {
        let available = self.available;

        if self.window_size >= available {
            return None;
        }

        let unclaimed = available.0 - self.window_size.0;
        let due = match self.update_policy {
            None => unclaimed >= self.window_size.0 / UNCLAIMED_DENOMINATOR * UNCLAIMED_NUMERATOR,
            Some(policy) => {
                self.update_due
                    || unclaimed as WindowSize >= policy.threshold
                    || policy
                        .low_window
                        .map_or(false, |low| self.window_size.0 <= low as i32)
            }
        };

        if due {
            Some(unclaimed as WindowSize)
        } else {
            None
        }
    }

    /// Increase the window size.
    ///
    /// This is called after receiving a WINDOW_UPDATE frame
    pub fn inc_window(&mut self, sz: WindowSize) -> Result<(), Reason> {
        let (val, overflow) = self.window_size.0.overflowing_add(sz as i32);

        if overflow {
            return Err(Reason::FLOW_CONTROL_ERROR);
        }

        if val > MAX_WINDOW_SIZE as i32 {
            return Err(Reason::FLOW_CONTROL_ERROR);
        }

        tracing::trace!(
            "inc_window; sz={}; old={}; new={}",
            sz,
            self.window_size,
            val
        );

        self.window_size = Window(val);
        Ok(())
    }

    /// Decrement the send-side window size.
    ///
    /// This is called after receiving a SETTINGS frame with a lower
    /// INITIAL_WINDOW_SIZE value.
    pub fn dec_send_window(&mut self, sz: WindowSize) -> Result<(), Reason> {
        tracing::trace!(
            "dec_window; sz={}; window={}, available={}",
            sz,
            self.window_size,
            self.available
        );
        // ~~This should not be able to overflow `window_size` from the bottom.~~ wrong. it can.
        self.window_size.decrease_by(sz)?;
        Ok(())
    }

    /// Decrement the recv-side window size.
    ///
    /// This is called after receiving a SETTINGS ACK frame with a lower
    /// INITIAL_WINDOW_SIZE value.
    pub fn dec_recv_window(&mut self, sz: WindowSize) -> Result<(), Reason> {
        tracing::trace!(
            "dec_recv_window; sz={}; window={}, available={}",
            sz,
            self.window_size,
            self.available
        );
        // This should not be able to overflow `window_size` from the bottom.
        self.window_size.decrease_by(sz)?;
        self.available.decrease_by(sz)?;
        Ok(())
    }

    /// Decrements the window reflecting data has actually been sent. The caller
    /// must ensure that the window has capacity.
    pub fn send_data(&mut self, sz: WindowSize) -> Result<(), Reason> {
        tracing::trace!(
            "send_data; sz={}; window={}; available={}",
            sz,
            self.window_size,
            self.available
        );

        // If send size is zero it's meaningless to update flow control window
        if sz > 0 {
            // Ensure that the argument is correct
            assert!(self.window_size.0 >= sz as i32);

            // Update values
            self.window_size.decrease_by(sz)?;
            self.available.decrease_by(sz)?;
        }
        Ok(())
    }
}

/// The current capacity of a flow-controlled Window.
///
/// This number can go negative when either side has used a certain amount
/// of capacity when the other side advertises a reduction in size.
///
/// This type tries to centralize the knowledge of addition and subtraction
/// to this capacity, instead of having integer casts throughout the source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd)]
pub struct Window(i32);

impl Window {
    pub fn as_size(&self) -> WindowSize {
        if self.0 < 0 {
            0
        } else {
            self.0 as WindowSize
        }
    }

    pub fn checked_size(&self) -> WindowSize {
        assert!(self.0 >= 0, "negative Window");
        self.0 as WindowSize
    }

    pub fn decrease_by(&mut self, other: WindowSize) -> Result<(), Reason> {
        if let Some(v) = self.0.checked_sub(other as i32) {
            self.0 = v;
            Ok(())
        } else {
            Err(Reason::FLOW_CONTROL_ERROR)
        }
    }

    pub fn increase_by(&mut self, other: WindowSize) -> Result<(), Reason> {
        let other = self.add(other)?;
        self.0 = other.0;
        Ok(())
    }

    pub fn add(&self, other: WindowSize) -> Result<Self, Reason> {
        if let Some(v) = self.0.checked_add(other as i32) {
            Ok(Self(v))
        } else {
            Err(Reason::FLOW_CONTROL_ERROR)
        }
    }
}

impl PartialEq<usize> for Window {
    fn eq(&self, other: &usize) -> bool {
        if self.0 < 0 {
            false
        } else {
            (self.0 as usize).eq(other)
        }
    }
}

impl PartialOrd<usize> for Window {
    fn partial_cmp(&self, other: &usize) -> Option<::std::cmp::Ordering> {
        if self.0 < 0 {
            Some(::std::cmp::Ordering::Less)
        } else {
            (self.0 as usize).partial_cmp(other)
        }
    }
}

impl fmt::Display for Window {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<Window> for isize {
    fn from(w: Window) -> isize {
        w.0 as isize
    }
}
