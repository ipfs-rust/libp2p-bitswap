#![allow(missing_docs)]
use prometheus::{Histogram, HistogramOpts, IntCounter};

#[derive(Clone, Debug)]
pub struct BitswapStats {
    // get request
    pub num_get: IntCounter,
    pub num_get_ok: IntCounter,
    pub num_get_err: IntCounter,
    pub num_get_cancel: IntCounter,
    pub hist_get_latency: Histogram, // TODO

    // sync request
    pub num_sync: IntCounter,
    pub num_sync_ok: IntCounter,
    pub num_sync_err: IntCounter,
    pub num_sync_cancel: IntCounter,
    pub hist_sync_latency: Histogram, // TODO

    // get providers event
    pub num_get_providers: IntCounter,
    pub hist_get_providers_latency: Histogram, // TODO
    pub hist_num_providers: Histogram,         // TODO

    // missing blocks event
    pub num_missing_blocks: IntCounter,
    pub hist_missing_blocks_latency: Histogram, // TODO
    pub hist_num_missing_blocks: Histogram,     // TODO

    // throttled events
    pub num_throttled_too_many_inbound: IntCounter,
    pub num_throttled_resume_send: IntCounter,

    // request response outbound failure
    pub num_outbound_timeout: IntCounter,
    pub num_outbound_connection_closed: IntCounter,
    pub num_outbound_unsupported_protocols: IntCounter,
    pub num_outbound_dial_failure: IntCounter,

    // request response inbound failure
    pub num_inbound_timeout: IntCounter,
    pub num_inbound_connection_closed: IntCounter,
    pub num_inbound_unsupported_protocols: IntCounter,
    pub num_inbound_response_omission: IntCounter,

    // received data
    pub num_tx_have: IntCounter,
    pub num_tx_block: IntCounter,
    pub hist_tx_have_latency: Histogram,  // TODO
    pub hist_tx_block_latency: Histogram, // TODO
    pub num_rx_block_count: IntCounter,
    pub num_rx_block_count_invalid: IntCounter,
    pub num_rx_block_bytes: IntCounter,
    pub num_rx_block_bytes_invalid: IntCounter,
    pub num_rx_have_yes: IntCounter,
    pub num_rx_have_no: IntCounter,

    // sent data
    pub num_rx_have: IntCounter,
    pub num_rx_block: IntCounter,
    pub hist_rx_have_latency: Histogram,  // TODO
    pub hist_rx_block_latency: Histogram, // TODO
    pub num_tx_block_count: IntCounter,
    pub num_tx_block_bytes: IntCounter,
    pub num_tx_have_yes: IntCounter,
    pub num_tx_have_no: IntCounter,
}

impl Default for BitswapStats {
    fn default() -> Self {
        Self {
            num_get: IntCounter::new("num_get", "Number of user initiated get requests.").unwrap(),
            num_get_ok: IntCounter::new("num_get_ok", "Number of successful get requests.")
                .unwrap(),
            num_get_err: IntCounter::new("num_get_err", "Number of unsuccessful get requests.")
                .unwrap(),
            num_get_cancel: IntCounter::new("num_get_cancel", "Number of canceled get requests.")
                .unwrap(),
            hist_get_latency: Histogram::with_opts(HistogramOpts::new(
                "hist_get_latency",
                "Histogram of get latency.",
            ))
            .unwrap(),

            num_sync: IntCounter::new("num_sync", "Number of user initiated sync requests.")
                .unwrap(),
            num_sync_ok: IntCounter::new("num_sync_ok", "Number of successful sync requests.")
                .unwrap(),
            num_sync_err: IntCounter::new("num_sync_err", "Number of unsuccessful sync requests.")
                .unwrap(),
            num_sync_cancel: IntCounter::new(
                "num_sync_cancel",
                "Number of canceled sync requests.",
            )
            .unwrap(),
            hist_sync_latency: Histogram::with_opts(HistogramOpts::new(
                "hist_sync_latency",
                "Histogram of sync latency.",
            ))
            .unwrap(),

            num_get_providers: IntCounter::new(
                "num_get_providers",
                "Number of get provider events.",
            )
            .unwrap(),
            hist_get_providers_latency: Histogram::with_opts(HistogramOpts::new(
                "hist_get_providers_latency",
                "Histogram of latency to process get providers.",
            ))
            .unwrap(),
            hist_num_providers: Histogram::with_opts(HistogramOpts::new(
                "hist_num_providers",
                "Histogram of number of providers found.",
            ))
            .unwrap(),

            num_missing_blocks: IntCounter::new(
                "num_missing_blocks",
                "Number of missing blocks events.",
            )
            .unwrap(),
            hist_missing_blocks_latency: Histogram::with_opts(HistogramOpts::new(
                "hist_missing_blocks_latency",
                "Histogram of latency to process missing blocks events.",
            ))
            .unwrap(),
            hist_num_missing_blocks: Histogram::with_opts(HistogramOpts::new(
                "hist_num_missing_blocks",
                "Histogram of number of missing blocks.",
            ))
            .unwrap(),

            num_throttled_too_many_inbound: IntCounter::new(
                "num_throttled_too_many_inbound",
                "Number of too many inbound events.",
            )
            .unwrap(),
            num_throttled_resume_send: IntCounter::new(
                "num_throttled_resume_send",
                "Number of resume send events.",
            )
            .unwrap(),

            num_outbound_timeout: IntCounter::new(
                "num_outbound_timeout",
                "Number of outbound timeout events.",
            )
            .unwrap(),
            num_outbound_connection_closed: IntCounter::new(
                "num_outbound_connection_closed",
                "Number of outbound connection closed events.",
            )
            .unwrap(),
            num_outbound_unsupported_protocols: IntCounter::new(
                "num_outbound_unsupported_protocols",
                "Number of outbound unsupported protocols events.",
            )
            .unwrap(),
            num_outbound_dial_failure: IntCounter::new(
                "num_outbound_dial_failure",
                "Number of outbound dial failure events.",
            )
            .unwrap(),

            num_inbound_timeout: IntCounter::new(
                "num_inbound_timeout",
                "Number of inbound timeout events.",
            )
            .unwrap(),
            num_inbound_connection_closed: IntCounter::new(
                "num_inbound_connection_closed",
                "Number of inbound connection closed events.",
            )
            .unwrap(),
            num_inbound_unsupported_protocols: IntCounter::new(
                "num_inbound_unsupported_protocols",
                "Number of inbound unsupported protocols events.",
            )
            .unwrap(),
            num_inbound_response_omission: IntCounter::new(
                "num_inbound_response_omission",
                "Number of inbound response omission events.",
            )
            .unwrap(),

            num_tx_have: IntCounter::new("num_tx_have", "Number of sent have requests.").unwrap(),
            num_tx_block: IntCounter::new("num_tx_block", "Number of sent block requests.")
                .unwrap(),
            hist_tx_have_latency: Histogram::with_opts(HistogramOpts::new(
                "hist_tx_have_latency",
                "Histogram of the time it took to get a have response.",
            ))
            .unwrap(),
            hist_tx_block_latency: Histogram::with_opts(HistogramOpts::new(
                "hist_tx_block_latency",
                "Histogram of the time it took to get a block response.",
            ))
            .unwrap(),
            num_rx_block_count: IntCounter::new("num_rx_block_count", "Number of received blocks.")
                .unwrap(),
            num_rx_block_count_invalid: IntCounter::new(
                "num_rx_block_count_invalid",
                "Number of received invalid blocks.",
            )
            .unwrap(),
            num_rx_block_bytes: IntCounter::new("num_rx_block_bytes", "Number of received bytes.")
                .unwrap(),
            num_rx_block_bytes_invalid: IntCounter::new(
                "num_rx_block_bytes_invalid",
                "Number of received invalid bytes.",
            )
            .unwrap(),
            num_rx_have_yes: IntCounter::new(
                "num_rx_have_yes",
                "Number of received yes responses.",
            )
            .unwrap(),
            num_rx_have_no: IntCounter::new("num_rx_have_no", "Number of received no responses.")
                .unwrap(),

            num_rx_have: IntCounter::new("num_rx_have", "Number of received have requests.")
                .unwrap(),
            num_rx_block: IntCounter::new("num_rx_block", "Number of received block requests.")
                .unwrap(),
            hist_rx_have_latency: Histogram::with_opts(HistogramOpts::new(
                "hist_rx_have_latency",
                "Histogram of the time it took to send a have response.",
            ))
            .unwrap(),
            hist_rx_block_latency: Histogram::with_opts(HistogramOpts::new(
                "hist_rx_block_latency",
                "Histogram of the time it took to send a block response.",
            ))
            .unwrap(),
            num_tx_block_count: IntCounter::new("num_tx_block_count", "Number of sent blocks.")
                .unwrap(),
            num_tx_block_bytes: IntCounter::new("num_tx_block_bytes", "Number of sent bytes.")
                .unwrap(),
            num_tx_have_yes: IntCounter::new("num_tx_have_yes", "Number of sent yes responses.")
                .unwrap(),
            num_tx_have_no: IntCounter::new("num_tx_have_no", "Number of sent no responses.")
                .unwrap(),
        }
    }
}
