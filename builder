#![allow(unused_variables)] // TODO(你): 实现此模块后删除此lint
#![allow(dead_code)] // TODO(你): 实现此模块后删除此lint

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use xxhash_rust::xxh32::Xxh32;

use anyhow::Result;
use bytes::BufMut;

use super::bloom::Bloom;
use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::key::{KeySlice, KeyVec};
use crate::lsm_storage::BlockCache;

/// 从键值对构建SSTable的构建器。
/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    // 用于构建当前块的BlockBuilder实例。
    // BlockBuilder instance to build the current block.
    builder: BlockBuilder,
    // 当前块中的第一个键。
    // First key in the current block.
    first_key: KeyVec,
    // 当前块中的最后一个键。
    // Last key in the current block.
    last_key: KeyVec,
    // 存储所有编码块的数据缓冲区。
    // Data buffer storing all encoded blocks.
    data: Vec<u8>,
    // 每个块的元数据（例如，偏移量、键）。
    // Metadata for each block (e.g., offset, keys).
    pub(crate) meta: Vec<BlockMeta>,
    // 每个块的目标大小。
    // Target size of each block.
    block_size: usize,
    // 所有键的哈希值，用于构建Bloom过滤器。
    // Hashes of all keys, used for building Bloom filter.
    key_hashes: Vec<u32>,
    // 所有键的最大时间戳。
    // Maximum timestamp of all keys.
    max_ts: u64,
}

impl SsTableBuilder {
    /// 基于目标块大小创建构建器。
    /// Create a builder based on the target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            block_size,
            builder: BlockBuilder::new(block_size),
            key_hashes: Vec::new(),
            max_ts: 0,
        }
    }

    /// 向SSTable添加一个键值对。
    /// Adds a key-value pair to the SSTable.
    ///
    ///  key: 要添加的键。
    ///  value: 与键关联的值。
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // 如果第一个键为空，将其设置为当前键。
        // If the first key is empty, set it to the current key.
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }
        
        // 生成时间戳
        // Generate timestamp
        let timestamp = generate_timestamp();
        println!("Timestamp: {}", timestamp);
    
        // 如有必要，更新最大时间戳
        // Update max timestamp if necessary
        if timestamp > self.max_ts {
            self.max_ts = timestamp;
        }
    
        // 使用Xxh32生成并存储键的哈希值
        // Generate and store the hash of the key using Xxh32
        let mut hasher = Xxh32::new(0);
        hasher.update(key.into_inner());
        let hash = hasher.digest();
        println!("Key hash: {}", hash);
        self.key_hashes.push(hash);
    
        // 尝试将键值对添加到当前块中。
        // Try to add the key-value pair to the current block.
        if !self.builder.add(key, value) {
            // 如果当前块已满，完成当前块并开始一个新块。
            // If the current block is full, finish the block and start a new one.
            self.finish_block();
            assert!(self.builder.add(key, value));
            self.first_key.set_from_slice(key);
        }
    
        // 将最后一个键更新为当前键
        // Update last key to the current key
        self.last_key.set_from_slice(key);
    
        // 定义生成时间戳的函数
        // Define the function to generate a timestamp
        fn generate_timestamp() -> u64 {
            // 获取当前系统时间
            // Get current system time
            let now = SystemTime::now();
    
            // 计算自UNIX纪元以来的持续时间
            // Calculate duration since UNIX epoch
            let duration = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    
            // 返回以秒为单位的持续时间
            // Return the number of seconds as u64
            duration.as_secs()
        }
    }

    /// 获取SSTable的估计大小。
    /// Get the estimated size of the SSTable.
    ///
    /// 返回当前编码数据的大小。
    /// The size of the current encoded data.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// 完成当前块并将其编码到数据缓冲区中。
    /// Finishes the current block and encodes it into the data buffer.
    fn finish_block(&mut self) {
        // 使用相同块大小的新BlockBuilder实例替换当前构建器。
        // Replace the current builder with a new BlockBuilder instance of the same block size.
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        
        // 编码块
        // Encode the block
        let encoded_block = builder.build().encode();
        println!("Encoded block length: {}", encoded_block.len());

        // 将当前块的元数据推送到meta向量中。
        // Push metadata for the current block to the meta vector.
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });

        // 使用Xxh32计算校验和
        // Calculate checksum using Xxh32
        let mut hasher = Xxh32::new(0);
        let update = hasher.update(&encoded_block);
        println!("Checksum update: {:?}", update);
        let checksum = hasher.digest();
        println!("Checksum: {}", checksum);
        
        // 将编码块数据追加到数据向量中
        // Append the encoded block data to the data vector
        self.data.extend(encoded_block);

        // 将校验和（作为u32）追加到数据向量中。
        // Append the checksum (as a u32) to the data vector.
        self.data.put_u32(checksum);
    }

    /// 构建SSTable并将其写入指定路径。
    /// Builds the SSTable and writes it to the given path.
    ///
    /// 成功时包含SsTable的结果。
    /// A Result containing the SsTable if successful.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);

        let bloom = Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);

        // 使用Xxh32计算整个缓冲区的校验和
        // Calculate checksum using Xxh32 for the whole buffer
        let mut hasher = Xxh32::new(0);
        hasher.update(&buf);
        let checksum = hasher.digest();
        // 将校验和（作为u32）追加到缓冲区。
        // Append the checksum (as a u32) to the buffer.
        buf.put_u32(checksum);

        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
    }

    /// 从键哈希构建Bloom过滤器。
    /// Builds the Bloom filter from key hashes.
    ///
    /// Bloom过滤器实例。
    /// A Bloom filter instance.
    fn build_bloom_filter(&self) -> Bloom {
        Bloom::build_from_key_hashes(
            &self.key_hashes,
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        )
    }

    /// 用于测试目的构建SSTable。
    /// Builds the SSTable for testing purposes.
    ///
    /// 成功时包含SsTable的结果。
    /// A Result containing the SsTable if successful.
    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
