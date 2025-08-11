use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::hash::Hash;

pub trait Mapper {
    type Input: Send + 'static;
    type Key: Send + Serialize + DeserializeOwned + Hash + Eq + Clone + 'static;
    type Value: Send + Serialize + DeserializeOwned + Clone + 'static;

    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::Input>,
        F: FnMut(Self::Key, Self::Value);
}

pub trait Reducer {
    type Key: Send + Serialize + DeserializeOwned + Hash + Eq + Clone + 'static;
    type ValueIn: Send + Serialize + DeserializeOwned + Clone + 'static;

    fn do_reduce<I, F>(&self, key: &Self::Key, values: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::ValueIn>,
        F: FnMut(String);
}

pub struct Pipeline {}

impl Pipeline {
    pub fn new() -> Self { Self {} }
}

pub struct InputSpec<T> {
    pub path: String,
    pub _marker: std::marker::PhantomData<T>,
}

pub struct OutputSpec {
    pub path: String,
}

impl<T> InputSpec<T> {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into(), _marker: std::marker::PhantomData }
    }
}

impl OutputSpec {
    pub fn new(path: impl Into<String>) -> Self { Self { path: path.into() } }
}

pub trait ExecutablePipeline {
    fn add_input<T: Send + 'static>(&mut self, input_path: impl Into<String>);
    fn add_output(&mut self, output_path: impl Into<String>);

    fn map_reduce<M, R>(&mut self, mapper: M, reducer: R) -> Result<()>
    where
        M: Mapper<Input = String> + Send + Sync + 'static,
        R: Reducer<Key = M::Key, ValueIn = M::Value> + Send + Sync + 'static;
}
