use ethereum_types::H256;
use pyo3::{
    prelude::*,
    types::{PyBytes, PyList, PySet},
};
use std::collections::BTreeSet;

pub(crate) trait ToPyObj {
    type Item;

    fn to_py_obj(&self, py: Python) -> Py<Self::Item>;
}

impl ToPyObj for H256 {
    type Item = PyBytes;

    fn to_py_obj(&self, py: Python) -> Py<Self::Item> { PyBytes::new(py, &self.0).into() }
}

impl ToPyObj for Option<H256> {
    type Item = PyAny;

    // 可以是 PyBytes 或 None

    fn to_py_obj(&self, py: Python) -> Py<Self::Item> {
        match self {
            Some(hash) => hash.to_py_obj(py).into(), // 复用 H256 的实现
            None => py.None().into(),                // 返回 Python 的 None
        }
    }
}

impl ToPyObj for Vec<H256> {
    type Item = PyList;

    // Python 的 list 类型

    fn to_py_obj(&self, py: Python) -> Py<Self::Item> {
        let list = PyList::empty(py);
        for hash in self {
            list.append(hash.to_py_obj(py)).unwrap(); // 将每个 H256 转为 PyBytes
        }
        list.into()
    }
}

impl ToPyObj for BTreeSet<H256> {
    type Item = PySet;

    // Python 的 set 类型

    fn to_py_obj(&self, py: Python) -> Py<Self::Item> {
        let set = PySet::empty(py).unwrap();
        for hash in self {
            set.add(hash.to_py_obj(py)).unwrap(); // 将每个 H256 转为 PyBytes
        }
        set.into()
    }
}

impl ToPyObj for Option<BTreeSet<H256>> {
    type Item = PySet;

    // Python 的 set 类型

    fn to_py_obj(&self, py: Python) -> Py<Self::Item> {
        let set = PySet::empty(py).unwrap();
        if let Some(hash_set) = self {
            for hash in hash_set {
                set.add(hash.to_py_obj(py)).unwrap(); // 将每个 H256 转为 PyBytes
            }
        }

        set.into()
    }
}
