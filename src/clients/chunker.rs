use std::{collections::HashMap, pin::Pin};

use futures::{Future, Stream, StreamExt};
use ginepro::LoadBalancedChannel;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
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
/// Default chunker that returns span for entire text
const DEFAULT_MODEL_ID: &str = "whole_doc_chunker";

type StreamingTokenizationResult = Result<Response<Streaming<TokenizationStreamResult>>, Status>;

#[cfg_attr(test, derive(Default))]
#[derive(Clone)]
pub struct ChunkerClient {
    clients: HashMap<String, ChunkersServiceClient<LoadBalancedChannel>>,
}

impl ChunkerClient {
    pub async fn new(default_port: u16, config: &[(String, ServiceConfig)]) -> Self {
        let clients = create_grpc_clients(default_port, config, ChunkersServiceClient::new).await;
        Self { clients }
    }

    fn client(&self, model_id: &str) -> Result<ChunkersServiceClient<LoadBalancedChannel>, Error> {
        Ok(self
            .clients
            .get(model_id)
            .ok_or_else(|| Error::ModelNotFound {
                model_id: model_id.to_string(),
            })?
            .clone())
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
        Ok(self
            .client(model_id)?
            .tokenization_task_predict(request_with_model_id(request, model_id))
            .await?
            .into_inner())
    }

    pub async fn bidi_streaming_tokenization_task_predict(
        &self,
        model_id: &str,
        request_stream: Pin<Box<dyn Stream<Item = BidiStreamingTokenizationTaskRequest> + Send>>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<TokenizationStreamResult, Status>> + Send>>, Error>
    {
        // Handle "default" separately first
        if model_id == DEFAULT_MODEL_ID {
            info!("Using default whole doc chunker");
            let (tx, rx) = mpsc::channel(128);
            let whole_response_stream = bidi_streaming_tokenize_whole_doc(request_stream).await;
            tokio::spawn(async move {
                let _ = tx.send(whole_response_stream).await;
            });
            return Ok(ReceiverStream::new(rx).boxed());
        }
        let mut client = self.client(model_id)?;
        let request: Request<
            Pin<Box<dyn Stream<Item = BidiStreamingTokenizationTaskRequest> + Send>>,
        > = request_with_model_id(request_stream, model_id);
        // NOTE: this is an ugly workaround to avoid bogus higher-ranked lifetime errors.
        // https://github.com/rust-lang/rust/issues/110338
        let response_stream_fut: Pin<Box<dyn Future<Output = StreamingTokenizationResult> + Send>> =
            Box::pin(client.bidi_streaming_tokenization_task_predict(request));
        Ok(response_stream_fut.await?.into_inner().boxed())
    }
}

fn request_with_model_id<T>(request: T, model_id: &str) -> Request<T> {
    let mut request = Request::new(request);
    request
        .metadata_mut()
        .insert(MODEL_ID_HEADER_NAME, model_id.parse().unwrap());
    request
}

/// Unary tokenization result of the entire doc
fn tokenize_whole_doc(request: TokenizationTaskRequest) -> TokenizationResults {
    let codepoint_count = request.text.chars().count();
    TokenizationResults {
        results: vec![Token {
            start: 0,
            end: codepoint_count as i64,
            text: request.text,
        }],
        token_count: 1, // entire doc
    }
}

/// Streaming tokenization result for an entire stream
// Note: This doesn't return an actual "stream" because the entire input text stream
// to the chunker has to be accumulated and processed. Only one result for the whole
// stream doc is provided. Depending on stream size, this can be memory intensive.
async fn bidi_streaming_tokenize_whole_doc(
    mut request_stream: Pin<Box<dyn Stream<Item = BidiStreamingTokenizationTaskRequest> + Send>>,
) -> Result<TokenizationStreamResult, Status> {
    let mut total_codepoint_count = 0;
    let mut accumulated_text: String = "".to_owned();
    while let Some(stream_request) = request_stream.next().await {
        let codepoint_count = stream_request.text_stream.chars().count();
        total_codepoint_count += codepoint_count;
        accumulated_text.push_str(stream_request.text_stream.as_str());
    }
    Ok(TokenizationStreamResult {
        results: vec![Token {
            start: 0,
            end: total_codepoint_count as i64,
            text: accumulated_text,
        }],
        token_count: 1, // entire doc/stream
        processed_index: total_codepoint_count as i64,
        start_index: 0,
    })
}
