#![allow(dead_code)]

#[derive(Clone)]
pub struct Bitmap {
    inner: Vec<u8>,
}

impl Bitmap {
    /// 创建一个新的空 Bitmap
    pub fn new() -> Self { Bitmap { inner: Vec::new() } }

    /// 创建一个指定容量的 Bitmap，所有位初始化为 0
    pub fn one_hot(bit_index: usize) -> Self {
        let mut bitmap = Self::with_capacity(bit_index + 1);
        bitmap.set(bit_index);
        bitmap
    }

    /// 创建一个指定容量的 Bitmap，所有位初始化为 0
    pub fn with_capacity(bits: usize) -> Self {
        let bytes = (bits + 7) / 8; // 向上取整到字节
        Bitmap {
            inner: vec![0; bytes],
        }
    }

    /// 获取指定位置的位值
    pub fn get(&self, bit_index: usize) -> bool {
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;

        if byte_index >= self.inner.len() {
            return false; // 超出范围返回默认值 false
        }

        (self.inner[byte_index] & (1 << bit_offset)) != 0
    }

    /// 设置指定位置的位值
    pub fn set(&mut self, bit_index: usize) {
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;

        // 如果需要，自动扩充容量
        if byte_index >= self.inner.len() {
            self.inner.resize(byte_index + 1, 0);
        }

        // 设置位为 1
        self.inner[byte_index] |= 1 << bit_offset;
    }

    /// 设置指定位置的位值
    pub fn reset(&mut self, bit_index: usize) {
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;

        // 如果需要，自动扩充容量
        if byte_index >= self.inner.len() {
            return;
        }

        // 设置位为 0
        self.inner[byte_index] &= !(1 << bit_offset);
    }

    /// 统计设置为 1 的位数量
    pub fn count(&self) -> usize {
        self.inner
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum()
    }

    /// 将另一个 Bitmap 与当前 Bitmap 进行按位或操作，合并两个 Bitmap
    pub fn combine(&mut self, other: &Bitmap) {
        // 确保当前 Bitmap 至少与 other 长度相同
        if other.inner.len() > self.inner.len() {
            self.inner.resize(other.inner.len(), 0);
        }

        // 按位或合并
        for (me, &input) in self.inner.iter_mut().zip(other.inner.iter()) {
            *me |= input;
        }
    }

    /// 获取 Bitmap 可存储的位数量
    pub fn capacity(&self) -> usize { self.inner.len() * 8 }

    /// 获取 Bitmap 的字节数
    pub fn len_bytes(&self) -> usize { self.inner.len() }

    /// 清空 Bitmap，将所有位设为 0
    pub fn clear(&mut self) {
        for byte in &mut self.inner {
            *byte = 0;
        }
    }
}

impl Default for Bitmap {
    fn default() -> Self { Self::new() }
}
