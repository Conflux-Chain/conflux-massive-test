use pyo3::{
    prelude::*,
    types::{PyBytes, PyList, PySet},
};
use tree_graph_parse_rust::block::Block;

use crate::to_py_obj::ToPyObj;

#[pyclass]
#[derive(Clone)]
pub(super) struct RustBlock {
    pub block: Block,
}

impl From<&Block> for RustBlock {
    fn from(block: &Block) -> Self {
        Self {
            block: block.clone(),
        }
    }
}

#[pymethods]
impl RustBlock {
    #[getter]
    pub fn id(&self) -> usize { self.block.id }

    #[getter]
    pub fn height(&self) -> u64 { self.block.height }

    #[getter]
    pub fn hash(&self, py: Python) -> Py<PyBytes> { self.block.hash.to_py_obj(py) }

    #[getter]
    pub fn parent_hash(&self, py: Python) -> Py<PyAny> { self.block.parent_hash.to_py_obj(py) }

    #[getter]
    pub fn referee_hashes(&self, py: Python) -> Py<PySet> {
        self.block.referee_hashes.to_py_obj(py)
    }

    #[getter]
    pub fn timestamp(&self) -> u64 { self.block.timestamp }

    #[getter]
    pub fn log_timestamp(&self) -> u64 { self.block.log_timestamp }

    #[getter]
    pub fn tx_count(&self) -> u64 { self.block.tx_count }

    #[getter]
    pub fn block_size(&self) -> u64 { self.block.block_size }

    #[getter]
    pub fn children(&self, py: Python) -> Py<PyList> { self.block.children.to_py_obj(py) }

    #[getter]
    pub fn epoch_block(&self, py: Python) -> Py<PyAny> { self.block.epoch_block.to_py_obj(py) }

    #[getter]
    pub fn epoch_set(&self, py: Python) -> Py<PySet> { self.block.epoch_set.to_py_obj(py) }

    #[getter]
    pub fn past_set_size(&self) -> u64 { self.block.past_set_size }

    #[getter]
    pub fn subtree_size(&self) -> u64 { self.block.subtree_size }

    #[getter]
    pub fn epoch_size(&self) -> usize { self.block.epoch_size() }
}
