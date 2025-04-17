/*
 Copyright FMS Guardrails Orchestrator Authors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

*/
use crate::{config::ChunkerType, pb::caikit_data_model::nlp as pb};

/// A chunk.
#[derive(Default, Debug, Clone)]
pub struct Chunk {
    /// Index of message where chunk begins
    pub input_start_index: usize,
    /// Index of message where chunk ends
    pub input_end_index: usize,
    /// Index of char where chunk begins
    pub start: usize,
    /// Index of char where chunk ends
    pub end: usize,
    /// Text
    pub text: String,
    /// Chunk type, optional? TBD [mostly needed for streamed chunks]
    pub chunker_type: ChunkerType,
}

impl PartialOrd for Chunk {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Chunk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            self.input_start_index,
            self.input_end_index,
            self.start,
            self.end,
        )
            .cmp(&(
                other.input_start_index,
                other.input_end_index,
                other.start,
                other.end,
            ))
    }
}

impl PartialEq for Chunk {
    fn eq(&self, other: &Self) -> bool {
        (
            self.input_start_index,
            self.input_end_index,
            self.start,
            self.end,
            self.chunker_type,
        ) == (
            other.input_start_index,
            other.input_end_index,
            other.start,
            other.end,
            other.chunker_type,
        )
    }
} // TODO: extend to compare types

impl Eq for Chunk {}

impl std::hash::Hash for Chunk {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input_start_index.hash(state);
        self.input_end_index.hash(state);
        self.start.hash(state);
        self.end.hash(state);
        self.chunker_type.hash(state);
    }
}

/// An array of chunks.
#[derive(Default, Debug, Clone)]
pub struct Chunks(Vec<Chunk>);

impl Chunks {
    pub fn new() -> Self {
        Self::default()
    }
}

impl std::ops::Deref for Chunks {
    type Target = Vec<Chunk>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Chunks {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for Chunks {
    type Item = Chunk;
    type IntoIter = <Vec<Chunk> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<Chunk> for Chunks {
    fn from_iter<T: IntoIterator<Item = Chunk>>(iter: T) -> Self {
        let mut chunks = Chunks::new();
        for value in iter {
            chunks.push(value);
        }
        chunks
    }
}

impl From<Vec<Chunk>> for Chunks {
    fn from(value: Vec<Chunk>) -> Self {
        Self(value)
    }
}

// Conversions

impl From<(ChunkerType, pb::ChunkerTokenizationStreamResult)> for Chunk {
    fn from(value: (ChunkerType, pb::ChunkerTokenizationStreamResult)) -> Self {
        let chunker_type = value.0;
        let stream_result = value.1;
        let text = stream_result
            .results
            .into_iter()
            .map(|token| token.text)
            .collect::<String>();
        Chunk {
            input_start_index: stream_result.input_start_index as usize,
            input_end_index: stream_result.input_end_index as usize,
            start: stream_result.start_index as usize,
            end: stream_result.processed_index as usize,
            text,
            chunker_type,
        }
    }
}

impl From<(ChunkerType, pb::TokenizationResults)> for Chunks {
    fn from(value: (ChunkerType, pb::TokenizationResults)) -> Self {
        let chunker_type = value.0;
        value.1
            .results
            .into_iter()
            .map(|token| Chunk {
                start: token.start as usize,
                end: token.end as usize,
                text: token.text,
                chunker_type,
                ..Default::default()
            })
            .collect()
    }
}
