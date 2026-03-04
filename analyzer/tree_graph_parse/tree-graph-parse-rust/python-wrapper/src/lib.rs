mod block;
mod to_py_obj;
mod utils;

use block::RustBlock;
use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};
use tree_graph_parse_rust::graph::Graph;

macro_rules! no_gil {
    ($py:ident, $expr:expr) => {
        $py.allow_threads(|| $expr)
    };
}

#[pyclass]
struct RustGraph {
    graph: Graph, // 裸指针字段
}

#[pymethods]
impl RustGraph {
    #[staticmethod]
    fn load(path: &str, py: Python) -> PyResult<Self> {
        let graph = no_gil!(py, Graph::load(path))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
        Ok(Self { graph })
    }

    #[getter]
    fn genesis_block(&self) -> RustBlock { self.graph.genesis_block().into() }

    #[getter]
    fn pivot_chain(&self, py: Python) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for block in self.graph.pivot_chain() {
            list.append(PyCell::new(py, RustBlock::from(block))?)?;
        }
        Ok(list.into())
    }

    fn epoch_span(&self, block: &RustBlock) -> u64 { self.graph.epoch_span(&block.block) }

    fn avg_epoch_time(&self, block: &RustBlock) -> f64 { self.graph.avg_epoch_time(&block.block) }

    fn confirmation_risk(
        &self, block: &RustBlock, adv_percent: usize, risk_threshold: f64, py: Python,
    ) -> Py<PyAny> {
        match no_gil!(
            py,
            self.graph
                .confirmation_risk(&block.block, adv_percent, risk_threshold)
        ) {
            Some((a, b, c, d)) => PyTuple::new(
                py,
                &[a.into_py(py), b.into_py(py), c.into_py(py), d.into_py(py)],
            )
            .into(),
            None => py.None().into(),
        }
    }

    fn avg_confirm_time(&self, adv_percent: usize, risk_threshold: f64, py: Python) -> (f64, u64) {
        no_gil!(py, self.graph.avg_confirm_time(adv_percent, risk_threshold))
    }
}

#[pymodule]
fn tg_parse_rpy(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RustGraph>()?; // 注册 RustGraph 类
    m.add_class::<RustBlock>()?; // 注册 RustBlock 类
    Ok(())
}
