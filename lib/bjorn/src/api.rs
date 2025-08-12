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

/// Executable pipeline over a concrete mapper input type `MInput`.
/// Supports registering multiple heterogenous inputs by providing per-input decoders and
/// a mapping function into the mapper's input union type.
pub trait ExecutablePipeline<MInput> {
    /// Register an input directory with a specific `Format<T>` implementation and a mapping
    /// function that converts each decoded `T` into the mapper's input type `MInput`.
    fn add_input<T, F>(&mut self, input_path: impl Into<String>, format: F, into_union: fn(T) -> MInput)
    where
        T: Send + 'static,
        F: crate::io::Format<T> + Send + Sync + 'static;

    /// Convenience for single-input jobs where `MInput == T` (uses identity mapping).
    fn add_input_single<T, F>(&mut self, input_path: impl Into<String>, format: F)
    where
        T: Send + 'static,
        F: crate::io::Format<T> + Send + Sync + 'static,
        MInput: From<T>;

    /// Set output directory
    fn add_output(&mut self, output_path: impl Into<String>);

    /// Run MapReduce using the registered inputs and provided sink.
    fn map_reduce<M, R, S>(&mut self, mapper: M, reducer: R, sink: S) -> Result<()>
    where
        M: Mapper<Input = MInput> + Send + Sync + 'static,
        R: Reducer<Key = M::Key, ValueIn = M::Value> + Send + Sync + 'static,
        S: crate::io::Sink<R::Out> + Send + Sync + 'static;
}
