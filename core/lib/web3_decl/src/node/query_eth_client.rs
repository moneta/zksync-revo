use std::fmt;
use anyhow::Context;
use zksync_node_framework::wiring_layer::{WiringError, WiringLayer};
use zksync_types::{url::SensitiveUrl, L1ChainId};

use crate::client::{Client, DynClient, L1};
use http::{HeaderMap, HeaderName, HeaderValue};

/// Wiring layer for Ethereum client.
#[derive(Debug)]
pub struct QueryEthClientLayer {
    l1_chain_id: L1ChainId,
    l1_rpc_url: SensitiveUrl,
    google_api_key: Option<String>, 
}

impl QueryEthClientLayer {
    pub fn new(l1_chain_id: L1ChainId, l1_rpc_url: SensitiveUrl, google_api_key: Option<String>) -> Self {
        Self {
            l1_chain_id,
            l1_rpc_url,
            google_api_key,
        }
    }

    pub fn with_google_api_key(mut self, key: impl Into<String>) -> Self {
        self.google_api_key = Some(key.into());
        self
    }
}

#[async_trait::async_trait]
impl WiringLayer for QueryEthClientLayer {
    type Input = ();
    type Output = Box<DynClient<L1>>;

    fn layer_name(&self) -> &'static str {
        "query_eth_client_layer"
    }

    async fn wire(self, (): Self::Input) -> Result<Self::Output, WiringError> {
        let client = if let Some(key) = self.google_api_key.as_deref() {
            let mut headers = HeaderMap::new();
            // Send *both* common variants to satisfy different Google gateways.
            headers.insert(HeaderName::from_static("x-goog-api-key"), HeaderValue::from_str(key).map_err(WiringError::internal)?);
            headers.insert(HeaderName::from_static("x-api-key"),      HeaderValue::from_str(key).map_err(WiringError::internal)?);

            Client::http_with_headers(self.l1_rpc_url.clone(), headers)
                .context("Client::http_with_headers()")?
                .for_network(self.l1_chain_id.into())
                .build()
        } else {
            Client::http(self.l1_rpc_url.clone())
                .context("Client::http()")?
                .for_network(self.l1_chain_id.into())
                .build()
        };
        Ok(Box::new(client))
    }
}
