use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::hash::Hash;

// ========== Core MapReduce traits ==========

pub trait Mapper {
    type Input: Send + 'static;
    type Key: Send + Serialize + DeserializeOwned + Hash + Eq + Clone + 'static;
    type Value: Send + Serialize + DeserializeOwned + Clone + 'static;

    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::Input>,
        F: FnMut(Self::Key, Self::Value);
}

/// Reducer produces a single typed output record per grouped key.
/// The grouping key is internal to the framework; the reducer emits final records
/// which will be written by a pluggable Sink implementation.
pub trait Reducer {
    type Key: Send + Serialize + DeserializeOwned + Hash + Eq + Clone + 'static;
    type ValueIn: Send + Serialize + DeserializeOwned + Clone + 'static;
    type Out: Send + Serialize + Clone + 'static;

    fn do_reduce<I, F>(&self, key: &Self::Key, values: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::ValueIn>,
        F: FnMut(Self::Out);
}

// ========== Executable pipeline interface (format- and sink-agnostic) ==========

pub trait ExecutablePipeline {
    fn add_input<T: Send + 'static>(&mut self, input_path: impl Into<String>);
    fn add_output(&mut self, output_path: impl Into<String>);

    fn map_reduce<M, R, Fmt, S>(&mut self, mapper: M, reducer: R, format: Fmt, sink: S) -> Result<()>
    where
        M: Mapper + Send + Sync + 'static,
        R: Reducer<Key = M::Key, ValueIn = M::Value> + Send + Sync + 'static,
        Fmt: crate::io::Format<M::Input> + Send + Sync + 'static,
        S: crate::io::Sink<R::Out> + Send + Sync + 'static;
}
