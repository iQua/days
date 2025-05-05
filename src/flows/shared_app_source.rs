//! A shared application-layer byte buffer used for TCP-based collective communication
//! This buffer defines a fixed-size byte stream
//! and provides a consistent view to each participating flow.

#[derive(Debug, Clone)]
pub struct BufferedAppSource {
    /// Total number of bytes to send in this shared buffer.
    total_size: usize,
}

impl BufferedAppSource {
    /// Constructs a new buffered source with a fixed number of total bytes.
    ///
    /// # Arguments
    /// * `total_size` - The total number of bytes to be transmitted by each flow.
    pub fn new(total_size: usize) -> Self {
        BufferedAppSource { total_size }
    }

    /// Returns the total number of bytes in the buffer.
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// Returns a copy of the total size, which each TCP flow can use independently.
    pub fn clone_total_size(&self) -> usize {
        self.total_size
    }
}
