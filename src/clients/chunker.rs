use std::{collections::HashMap, pin::Pin};

use futures::{Stream, StreamExt};
use ginepro::LoadBalancedChannel;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tracing::info;

use super::{create_grpc_clients, Error};
use crate::{
    config::ServiceConfig,
    pb::{
        caikit::runtime::chunkers::{
            chunkers_service_client::ChunkersServiceClient, BidiStreamingTokenizationTaskRequest,
            TokenizationTaskRequest,
        },
        caikit_data_model::nlp::{Token, TokenizationResults, TokenizationStreamResult},
    },
};

const MODEL_ID_HEADER_NAME: &str = "mm-model-id";
const DEFAULT_MODEL_ID: &str = "whole_doc_chunker";

#[derive(Clone)]
pub enum ChunkerMapValue {
    ChunkersServiceClient(ChunkersServiceClient<LoadBalancedChannel>),
    String,
}

#[cfg_attr(test, derive(Default))]
#[derive(Clone)]
pub struct ChunkerClient {
    clients: HashMap<String, ChunkerMapValue>,
}

impl ChunkerClient {
    pub async fn new(default_port: u16, config: &[(String, ServiceConfig)]) -> Self {
        let orig_clients =
            create_grpc_clients(default_port, config, ChunkersServiceClient::new).await;
        let mut clients: HashMap<String, ChunkerMapValue> = HashMap::new();
        for (key, value) in orig_clients.into_iter() {
            clients.insert(key, ChunkerMapValue::ChunkersServiceClient(value));
        }
        // TODO: add "default" client for "default model"
        Self { clients }
    }

    // Expect this to be called only in non-default chunker "client" case
    fn client(&self, model_id: &str) -> Result<ChunkersServiceClient<LoadBalancedChannel>, Error> {
        // Could this potentially be collapsed?
        let client = self.clients.get(model_id);
        match client {
            Some(value) => {
                if let ChunkerMapValue::ChunkersServiceClient(client) = value {
                    Ok(client.clone())
                } else {
                    Err(Error::ModelNotFound {
                        model_id: model_id.to_string(),
                    })
                }
            }
            None => Err(Error::ModelNotFound {
                model_id: model_id.to_string(),
            }),
        }
    }

    pub async fn tokenization_task_predict(
        &self,
        model_id: &str,
        request: TokenizationTaskRequest,
    ) -> Result<TokenizationResults, Error> {
        // Handle "default" separately first
        if model_id == DEFAULT_MODEL_ID {
            info!("Using default whole doc chunker");
            return Ok(tokenize_whole_doc(request));
        }
        let request = request_with_model_id(request, model_id);
        Ok(self
            .client(model_id)?
            .tokenization_task_predict(request)
            .await?
            .into_inner())
    }

    // NOTE: probably untested through o8 so far
    pub async fn bidi_streaming_tokenization_task_predict(
        &self,
        model_id: &str,
        request: Pin<Box<dyn Stream<Item = BidiStreamingTokenizationTaskRequest> + Send + 'static>>,
    ) -> Result<ReceiverStream<TokenizationStreamResult>, Error> {
        let (tx, rx) = mpsc::channel(128);
        // Handle "default" separately first
        // if model_id == DEFAULT_MODEL_ID {
        //     let response_stream = bidi_streaming_tokenize_whole_doc(request);
        //     tokio::spawn(async move {
        //         while let Ok(message) = response_stream.await {
        //             let _ = tx.send(message).await;
        //         }
        //     });
        //     return Ok(ReceiverStream::new(rx));
        // }
        let request = request_with_model_id(request, model_id);
        let mut response_stream = self
            .client(model_id)?
            .bidi_streaming_tokenization_task_predict(request)
            .await?
            .into_inner();
        tokio::spawn(async move {
            while let Some(Ok(message)) = response_stream.next().await {
                let _ = tx.send(message).await;
            }
        });
        Ok(ReceiverStream::new(rx))
    }
}

fn request_with_model_id<T>(request: T, model_id: &str) -> Request<T> {
    let mut request = Request::new(request);
    request
        .metadata_mut()
        .insert(MODEL_ID_HEADER_NAME, model_id.parse().unwrap());
    request
}

fn tokenize_whole_doc(request: TokenizationTaskRequest) -> TokenizationResults {
    let token_count = request.text.chars().count();
    TokenizationResults {
        results: vec![Token {
            start: 0,
            end: token_count as i64,
            text: request.text,
        }],
        token_count: token_count as i64,
    }
}

async fn bidi_streaming_tokenize_whole_doc(
    _request: Pin<Box<dyn Stream<Item = BidiStreamingTokenizationTaskRequest> + Send + 'static>>,
) -> Result<TokenizationStreamResult, Error> {
    // TODO: this will have to do aggregation and actually process the request
    let token_count = 120; // FIXME
    let accumulated_text = "hi"; // FIXME
                                 // TODO: error handling
                                 // TODO: Return a stream?
    Ok(TokenizationStreamResult {
        results: vec![Token {
            start: 0,
            end: token_count,
            text: accumulated_text.to_string(),
        }],
        token_count,
        processed_index: token_count,
        start_index: 0,
    })
}
