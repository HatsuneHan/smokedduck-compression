//
// Created by hxy on 8/8/24.
//
#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_compression.hpp"

#include "lz4.hpp"
#include "zstd.h"

namespace duckdb {
	void Compressed64List::WriteBitsToBuffer(idx_t curr_buffer_bit_size, idx_t value){
	    // Ensure enough space in buffer
	    
	    idx_t block_index = curr_buffer_bit_size / 8;
	    int8_t block_offset = curr_buffer_bit_size % 8;
	    const int8_t block_size = 8;

	    int8_t unwritten_bit_size = delta_bit_size;
        
	    while (unwritten_bit_size > 0) {
            
		    int8_t avail_size = block_size - block_offset;
		    int8_t write_bit_size_in_block;
		    int8_t size_cmp = avail_size - unwritten_bit_size;
		    if (size_cmp >= 0) {
			    write_bit_size_in_block = unwritten_bit_size;
		    } else {
			    write_bit_size_in_block = avail_size;
		    }

		    data_t delta_addr_block;
		    if (write_bit_size_in_block != unwritten_bit_size) {
			    delta_addr_block = static_cast<data_t>((value >> (~size_cmp+1)) & ((1ull << write_bit_size_in_block) - 1));
		    } else {
			    delta_addr_block = static_cast<data_t>((value & ((1ull << write_bit_size_in_block) - 1)) << (size_cmp));
			}

		    delta_buffer[block_index] |= delta_addr_block;

		    unwritten_bit_size -= write_bit_size_in_block;
		    curr_buffer_bit_size += write_bit_size_in_block;

		    // if (block_offset + write_bit_size_in_block >= 8)
		    //     block_index++;
		    // block_offset = (block_offset + write_bit_size_in_block) % 8;

		    // this is right because except the first write, each time we write from the first bit of the block
		    // and each time the block we write is the next block
		    // though it is wrong when the last write is not a full block, we do not need to continue to write at that time
		    // so we can ignore this situation

		    block_index++;
		    block_offset = 0;

		    value = value & ((1ull << unwritten_bit_size) - 1);
	    }
    }

    idx_t Compressed64List::ReadBitsFromBuffer(idx_t read_buffer_from_bit) {
	    // Ensure enough space in buffer
	    
	    idx_t block_index = read_buffer_from_bit / 8;
	    int8_t block_offset = read_buffer_from_bit % 8;
	    const int8_t block_size = 8;

	    int8_t unread_bit_size = delta_bit_size;

	    idx_t value = 0;

	    while (unread_bit_size > 0) {
		    data_t curr_block = delta_buffer[block_index];

		    int8_t avail_size = block_size - block_offset;
		    int8_t read_bit_size_in_block;
		    int8_t size_cmp = avail_size - unread_bit_size;
		    if (size_cmp >= 0) {
			    read_bit_size_in_block = unread_bit_size;
		    } else {
			    read_bit_size_in_block = avail_size;
		    }

		    if (read_bit_size_in_block != unread_bit_size) {
			    curr_block &= ((1ull << read_bit_size_in_block) - 1);
		    } else {
			    curr_block = (curr_block >> (size_cmp)) & ((1ull << read_bit_size_in_block) - 1);
		    }

		    value = (value << read_bit_size_in_block) | curr_block;

		    unread_bit_size -= read_bit_size_in_block;

		    // if (block_offset + read_bit_size_in_block >= 8)
		    //     block_index++;
		    // block_offset = (block_offset + read_bit_size_in_block) % 8;

		    block_index++;
		    block_offset = 0;
	    }

	    return value;
    }

    void Compressed64List::PushBack(idx_t sel, size_t artifact_size) {
	    // Deal with delta encoding for address
	    if (artifact_size == 0) {
		    base = sel & ~0x1ull;
		    delta_bit_size = static_cast<data_t>(1);
		    
		    delta_buffer_size = sizeof(idx_t);
		    delete[] delta_buffer;
		    delta_buffer = new unsigned char[delta_buffer_size];
		    std::memset(delta_buffer, 0, delta_buffer_size);

		    idx_t uint_delta_addr = (sel & 0x1ull);

		    if(uint_delta_addr == 1) {
			    delta_buffer[0] = 0x80;
		    }
            
		    return;
	    }

	    idx_t check_addr_with_base = (sel ^ base) >> delta_bit_size;
	    bool base_addr_is_usable = !check_addr_with_base;

	    if (base_addr_is_usable) {
		    idx_t curr_buffer_bit_size = artifact_size * delta_bit_size;

		    while (curr_buffer_bit_size + delta_bit_size > 8 * delta_buffer_size) {
			    idx_t new_delta_addr_buffer_size = std::ceil(1.25 * delta_buffer_size);
			    unsigned char* new_delta_addr_buffer = new unsigned char[new_delta_addr_buffer_size];
			    std::memset(new_delta_addr_buffer, 0, new_delta_addr_buffer_size);
			    std::memcpy(new_delta_addr_buffer, delta_buffer, delta_buffer_size);
			    delete[] delta_buffer;

			    delta_buffer = new_delta_addr_buffer;
			    delta_buffer_size = new_delta_addr_buffer_size;
		    }

		    idx_t delta_addr_bitmask = delta_bit_size == 64 ? ~0ull : ((1ull << delta_bit_size) - 1);
		    WriteBitsToBuffer(curr_buffer_bit_size, sel & delta_addr_bitmask);
	    } else {
		    data_t reduced_bit_size = 0;
		    while (check_addr_with_base) {
			    check_addr_with_base >>= 1;
			    reduced_bit_size++;
		    }
		    idx_t suppl_delta_addr = (base >> delta_bit_size) & ((1ull << reduced_bit_size) - 1);

		    idx_t new_buffer_size = ((artifact_size + 1) * (delta_bit_size + reduced_bit_size) + 7) / 8;
		    new_buffer_size = std::max(new_buffer_size, delta_buffer_size);

		    Compressed64List new_compressed_list;
            
		    new_compressed_list.delta_buffer = new unsigned char[new_buffer_size];
		    new_compressed_list.delta_buffer_size = new_buffer_size;
		    std::memset(new_compressed_list.delta_buffer, 0, new_buffer_size);

		    new_compressed_list.delta_buffer_size = new_buffer_size;
		    new_compressed_list.delta_bit_size = delta_bit_size + reduced_bit_size;
		    new_compressed_list.base = sel & ~((1ull << (new_compressed_list.delta_bit_size)) - 1);

		    idx_t curr_new_buffer_bit_size = 0;
		    size_t old_buffer_addr_cnt = 0;

		    while (old_buffer_addr_cnt < artifact_size) {

			    idx_t delta_addr = ReadBitsFromBuffer(old_buffer_addr_cnt * delta_bit_size);
                
			    delta_addr |= (suppl_delta_addr << delta_bit_size);
			    new_compressed_list.WriteBitsToBuffer(curr_new_buffer_bit_size, delta_addr);
			    curr_new_buffer_bit_size += new_compressed_list.delta_bit_size;

			    old_buffer_addr_cnt++;
		    }

		    // printBinary(new_compressed_list.delta_buffer, new_compressed_list.delta_buffer_size);
		    new_compressed_list.WriteBitsToBuffer(curr_new_buffer_bit_size, sel & (((1ull << new_compressed_list.delta_bit_size) - 1)));
            
		    // change the current object to the new object
		    base = new_compressed_list.base;
		    delta_bit_size = new_compressed_list.delta_bit_size;
		    delta_buffer_size = new_compressed_list.delta_buffer_size;

		    // memcpy the new delta buffer to the current delta buffer
		    delete[] delta_buffer;
		    delta_buffer = new unsigned char[delta_buffer_size];
		    std::memcpy(delta_buffer, new_compressed_list.delta_buffer, delta_buffer_size);
            
		    // the new_compressed_list will be automatically destroyed because it is a local variable and we set the destructor
	    }
    }

    idx_t Compressed64List::Get(idx_t index) {
	    return ReadBitsFromBuffer(index * delta_bit_size) | base;
    }

    idx_t Compressed64List::operator[](idx_t index) {
	    return ReadBitsFromBuffer(index * delta_bit_size) | base;
    }

    size_t Compressed64List::GetBytesSize() {
	    return sizeof(Compressed64List) + delta_buffer_size;
    }

    void Compressed64List::Resize(idx_t size_p) {
	    idx_t new_buffer_size = (size_p * delta_bit_size + 7) / 8;
	    unsigned char* new_delta_buffer = new unsigned char[new_buffer_size];

	    std::memset(new_delta_buffer, 0, new_buffer_size);
	    std::memcpy(new_delta_buffer, delta_buffer, new_buffer_size);

	    delete[] delta_buffer;

	    delta_buffer = new_delta_buffer;
	    delta_buffer_size = new_buffer_size;
    }

    vector<idx_t> CompressBitmap(idx_t curr_bitmap_size, unsigned char *bitmap, CompressionMethod method) {

	    idx_t bitmap_size = (STANDARD_VECTOR_SIZE + 7) / 8;
	    vector<idx_t> result_vector;

	    if (curr_bitmap_size <= STANDARD_VECTOR_SIZE / 4 || curr_bitmap_size >= 3 * STANDARD_VECTOR_SIZE / 4) {
		    if (method == CompressionMethod::LZ4) {
			    // LZ4 compression
			    int max_compressed_size = duckdb_lz4::LZ4_compressBound(bitmap_size);
			    char *compressed_bitmap = new char[max_compressed_size];
			    int compressed_size = duckdb_lz4::LZ4_compress_fast(reinterpret_cast<const char *>(bitmap),
			                                            compressed_bitmap, bitmap_size, max_compressed_size, 1);
			    if (compressed_size <= 0) {
				    delete[] compressed_bitmap;
				    throw std::runtime_error("LZ4 compression failed");
			    }

			    unsigned char *compressed_bitmap_copy = new unsigned char[compressed_size];
			    std::copy(compressed_bitmap, compressed_bitmap + compressed_size, compressed_bitmap_copy);

			    result_vector.push_back(reinterpret_cast<idx_t>(compressed_bitmap_copy));
			    result_vector.push_back(compressed_size);
			    result_vector.push_back(1); // indicates lz4 compression

			    delete[] compressed_bitmap;
			    delete[] bitmap;  // delete the original bitmap

		    } else if (method == CompressionMethod::ZSTD) {
			    // ZSTD compression
			    size_t max_compressed_size = duckdb_zstd::ZSTD_compressBound(bitmap_size);
			    char *compressed_bitmap = new char[max_compressed_size];
			    size_t compressed_size = duckdb_zstd::ZSTD_compress(compressed_bitmap, max_compressed_size, bitmap, bitmap_size, 1);
			    if (duckdb_zstd::ZSTD_isError(compressed_size)) {
				    delete[] compressed_bitmap;
				    throw std::runtime_error("ZSTD compression failed");
			    }

			    unsigned char *compressed_bitmap_copy = new unsigned char[compressed_size];
			    std::copy(compressed_bitmap, compressed_bitmap + compressed_size, compressed_bitmap_copy);

			    result_vector.push_back(reinterpret_cast<idx_t>(compressed_bitmap_copy));
			    result_vector.push_back(compressed_size);
			    result_vector.push_back(2); // indicates zstd compression

			    delete[] compressed_bitmap;
			    delete[] bitmap;  // delete the original bitmap
		    }

	    } else {
		    // No compression
		    result_vector.push_back(reinterpret_cast<idx_t>(bitmap));
		    result_vector.push_back(bitmap_size);
		    result_vector.push_back(0); // indicates no compression
	    }

	    return result_vector;
    }

    unsigned char* DecompressBitmap(idx_t compressed_bitmap_size, idx_t bitmap_is_compressed, unsigned char *compressed_bitmap) {

	    unsigned char* decompressed_bitmap;

	    if (bitmap_is_compressed == 1) {
		    // LZ4 decompression
		    size_t dst_size = (STANDARD_VECTOR_SIZE + 7) / 8;
		    char* tmp_decompressed_bitmap = new char[dst_size];
		    size_t decompressed_size = duckdb_lz4::LZ4_decompress_safe(reinterpret_cast<char *>(compressed_bitmap), tmp_decompressed_bitmap, compressed_bitmap_size, dst_size);
		    if (decompressed_size != dst_size) {
			    delete[] tmp_decompressed_bitmap;
			    throw std::runtime_error("LZ4 decompression failed");
		    }
		    decompressed_bitmap = reinterpret_cast<unsigned char*>(tmp_decompressed_bitmap);

	    } else if (bitmap_is_compressed == 2) {
		    // ZSTD decompression
		    size_t dst_size = (STANDARD_VECTOR_SIZE + 7) / 8;
		    char* tmp_decompressed_bitmap = new char[dst_size];
		    size_t decompressed_size = duckdb_zstd::ZSTD_decompress(tmp_decompressed_bitmap, dst_size, compressed_bitmap, compressed_bitmap_size);
		    if (duckdb_zstd::ZSTD_isError(decompressed_size) || decompressed_size != dst_size) {
			    delete[] tmp_decompressed_bitmap;
			    throw std::runtime_error("ZSTD decompression failed");
		    }
		    decompressed_bitmap = reinterpret_cast<unsigned char*>(tmp_decompressed_bitmap);

	    } else {
		    // No decompression needed
		    decompressed_bitmap = compressed_bitmap;
	    }

	    return decompressed_bitmap;
    }

    vector<idx_t> CompressDataTArray(idx_t array_size, data_ptr_t data, CompressionMethod method) {
	    vector<idx_t> result_vector;

	    if (array_size >= 32) {
		    if (method == CompressionMethod::LZ4) {
			    // LZ4 compression
			    idx_t compressed_size = duckdb_lz4::LZ4_compressBound(array_size * sizeof(data_t));
			    char *compressed_data = new char[compressed_size];
			    int compressed_size_actual = duckdb_lz4::LZ4_compress_fast(
			        reinterpret_cast<const char *>(data), compressed_data, array_size * sizeof(data_t), compressed_size, 1);
			    if (compressed_size_actual <= 0) {
				    delete[] compressed_data;
				    throw std::runtime_error("LZ4 compression failed");
			    }

			    unsigned char *compressed_data_copy = new unsigned char[compressed_size_actual];
			    std::copy(compressed_data, compressed_data + compressed_size_actual, compressed_data_copy);

			    result_vector.push_back(reinterpret_cast<idx_t>(compressed_data_copy)); // (compressed) data addr
			    result_vector.push_back(compressed_size_actual); // compressed size
			    result_vector.push_back(1); // is compressed or not

			    delete[] compressed_data;

		    } else if (method == CompressionMethod::ZSTD) {
			    // ZSTD compression
			    size_t compressed_size = duckdb_zstd::ZSTD_compressBound(array_size * sizeof(data_t));
			    char *compressed_data = new char[compressed_size];
			    size_t compressed_size_actual = duckdb_zstd::ZSTD_compress(
			        compressed_data, compressed_size, data, array_size * sizeof(data_t), 1);
			    if (duckdb_zstd::ZSTD_isError(compressed_size_actual)) {
				    delete[] compressed_data;
				    throw std::runtime_error("ZSTD compression failed");
			    }

			    unsigned char *compressed_data_copy = new unsigned char[compressed_size_actual];
			    std::copy(compressed_data, compressed_data + compressed_size_actual, compressed_data_copy);

			    result_vector.push_back(reinterpret_cast<idx_t>(compressed_data_copy)); // (compressed) data addr
			    result_vector.push_back(compressed_size_actual); // compressed size
			    result_vector.push_back(2); // indicates zstd compression

			    delete[] compressed_data;
		    }
	    } else {
		    // No compression
		    data_ptr_t data_copy = new data_t[array_size];
		    std::copy(data, data + array_size, data_copy);

		    result_vector.push_back(reinterpret_cast<idx_t>(data_copy));
		    result_vector.push_back(array_size * sizeof(data_t));
		    result_vector.push_back(0); // indicates no compression
	    }

	    return result_vector;
    }

    data_ptr_t DecompressDataTArray(idx_t compressed_data_size, idx_t compression_method,
                                    unsigned char* compressed_data, idx_t array_size) {

	    data_ptr_t decompressed_data;

	    if (compression_method == 1) {
		    // LZ4 decompression
		    size_t dst_size = array_size * sizeof(data_t);
		    char* tmp_decompressed_data = new char[dst_size];

		    size_t decompressed_size = duckdb_lz4::LZ4_decompress_safe(reinterpret_cast<char *>(compressed_data), tmp_decompressed_data, compressed_data_size, dst_size);
		    if (decompressed_size != dst_size) {
			    delete[] tmp_decompressed_data;
			    throw std::runtime_error("LZ4 decompression failed");
		    }

		    decompressed_data = reinterpret_cast<data_ptr_t>(tmp_decompressed_data);

	    } else if (compression_method == 2) {
		    // ZSTD decompression
		    size_t dst_size = array_size * sizeof(data_t);
		    char* tmp_decompressed_data = new char[dst_size];

		    size_t decompressed_size = duckdb_zstd::ZSTD_decompress(tmp_decompressed_data, dst_size, compressed_data, compressed_data_size);
		    if (duckdb_zstd::ZSTD_isError(decompressed_size) || decompressed_size != dst_size) {
			    delete[] tmp_decompressed_data;
			    throw std::runtime_error("ZSTD decompression failed");
		    }

		    decompressed_data = reinterpret_cast<data_ptr_t>(tmp_decompressed_data);

	    } else {
		    // No decompression needed
		    decompressed_data = reinterpret_cast<data_ptr_t>(compressed_data);
	    }

	    return decompressed_data;
    }

    vector<vector<idx_t>> ChangeSelToBitMap(sel_t* sel_data, idx_t result_count, CompressionMethod method){
	    vector<idx_t> bitmap_vector;
	    vector<idx_t> bitmap_sizes;
	    vector<idx_t> bitmap_is_compressed;
	    vector<idx_t> use_bitmap;

	    if(result_count >= 16) {
		    size_t bitmap_size = (STANDARD_VECTOR_SIZE + 7) / 8;
		    unsigned char* bitmap = new unsigned char[bitmap_size];
		    std::memset(bitmap, 0, bitmap_size);
		    bool is_first = true;
		    sel_t prev_index = 0;
		    size_t curr_bitmap_size = 0;

		    for (size_t i = 0; i < result_count; ++i) {
			    sel_t index = sel_data[i];
			    if (index >= STANDARD_VECTOR_SIZE) {
				    throw std::runtime_error("Index out of range");
			    }

			    if ((index > prev_index) || is_first) {
				    // ensure monotonicity
				    bitmap[index / 8] |= (1 << (7 - index % 8));
				    is_first = false;
				    curr_bitmap_size += 1;
			    } else {
				    // here we finish the current bitmap
				    // compress the bitmap

					vector<idx_t> result_vector = CompressBitmap(curr_bitmap_size, bitmap, method);
				    bitmap_vector.push_back(result_vector[0]);
				    bitmap_sizes.push_back(result_vector[1]);
				    bitmap_is_compressed.push_back(result_vector[2]);

				    // create a new bitmap
				    bitmap = new unsigned char[bitmap_size];
				    std::memset(bitmap, 0, bitmap_size);

				    bitmap[index / 8] |= (1 << (7 - index % 8));
				    curr_bitmap_size = 1;
			    }

			    prev_index = index;
		    }

		    // compress the last bitmap
		    vector<idx_t> result_vector = CompressBitmap(curr_bitmap_size, bitmap, method);

		    bitmap_vector.push_back(result_vector[0]);
		    bitmap_sizes.push_back(result_vector[1]);
		    bitmap_is_compressed.push_back(result_vector[2]);

		    use_bitmap.push_back(1);

	    } else {
		    sel_t* sel_copy = new sel_t[result_count];
		    std::copy(sel_data, sel_data + result_count, sel_copy);

		    bitmap_vector.push_back(reinterpret_cast<idx_t>(sel_copy));
		    bitmap_sizes.push_back(result_count * sizeof(sel_t));
		    bitmap_is_compressed.push_back(0);
		    use_bitmap.push_back(0);
	    }

	    vector<vector<idx_t>> func_result_vector = {bitmap_vector, bitmap_sizes, bitmap_is_compressed, use_bitmap};
	    return func_result_vector;
    }

    // support CompressedFilterArtifacts
    template<typename ARTIFACT_TYPE>
    sel_t* ChangeBitMapToSel(const ARTIFACT_TYPE& artifacts, idx_t offset, idx_t lsn) {
//	    idx_t offset = this->artifacts->in_start[lsn];

		idx_t count = artifacts->count[lsn];
	    idx_t start_bitmap_idx = artifacts->start_bitmap_idx[lsn];
	    idx_t use_bitmap = artifacts->use_bitmap[lsn];
	    idx_t bitmap_num = artifacts->start_bitmap_idx[lsn+1] - start_bitmap_idx;

	    if (bitmap_num) {
		    if (use_bitmap){
			    sel_t* sel_copy = new sel_t[count];
			    size_t index = 0;

			    for(size_t i = 0; i < bitmap_num; i++){

				    unsigned char* decompressed_bitmap = DecompressBitmap(artifacts->bitmap_size[start_bitmap_idx+i],
				                                                 artifacts->bitmap_is_compressed[start_bitmap_idx+i],
				                                                  reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx+i]));

				    for (size_t j = 0; j < STANDARD_VECTOR_SIZE; ++j) {
					    if (decompressed_bitmap[j / 8] & (1 << (7 - (j % 8)))) {
						    sel_copy[index++] = static_cast<sel_t>(j) + offset; // PostProcess() here
					    }
				    }
			    }

			    return sel_copy;
		    }
		    else {
			    sel_t* bitmap_sel = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx]);
			    for(size_t i = 0; i < count; i++){
				    bitmap_sel[i] += offset;
			    }
			    return bitmap_sel;
		    }
	    }

	    return nullptr;

    }

    // because rle number may be idx_t, we need to use idx_t*
	idx_t* ChangeSelDataToDeltaRLE(sel_t* sel_data, idx_t key_count){

		if(key_count <= 8){
		    sel_t* sel_data_copy = new sel_t[key_count];
		    std::copy(sel_data, sel_data + key_count, sel_data_copy);
		    return reinterpret_cast<idx_t*>(sel_data_copy);
	    }

		vector<idx_t> delta_rle_vector;

		// we should store the first element
	    delta_rle_vector.push_back(sel_data[0]);

	    if(sel_data[1] <= sel_data[0]){
		    throw std::runtime_error("sel_data should be in ascending order");
	    }
	    idx_t prev_delta = sel_data[1] - sel_data[0];

	    idx_t curr_rle = 1;

	    for(size_t i = 2; i < key_count; i++) {
		    // we should guarantee sel_data should in ascending order
		    // and it is true for sel_tuples
		    if(sel_data[i] <= sel_data[i-1]){
			    throw std::runtime_error("sel_data should be in ascending order");
		    }

		    idx_t delta = sel_data[i] - sel_data[i - 1];
		    if (delta == prev_delta) {
			    curr_rle++;
		    } else {
			    delta_rle_vector.push_back(prev_delta);
			    delta_rle_vector.push_back(curr_rle);

			    prev_delta = delta;
			    curr_rle = 1;
		    }
	    }

	    delta_rle_vector.push_back(prev_delta);
	    delta_rle_vector.push_back(curr_rle);

	    idx_t *delta_rle = new idx_t[delta_rle_vector.size()];
	    std::copy(delta_rle_vector.begin(), delta_rle_vector.end(), delta_rle);

		return delta_rle;
    }

    sel_t* ChangeDeltaRLEToSelData(idx_t* delta_rle, idx_t key_count){

	    if(key_count <= 8){
		    return reinterpret_cast<sel_t*>(delta_rle);
	    }

	    sel_t* sel_data = new sel_t[key_count];
	    sel_data[0] = delta_rle[0];

	    idx_t index = 1;
	    idx_t delta_rle_index = 1;

	    while(index < key_count){
		    idx_t delta = delta_rle[delta_rle_index];
		    idx_t rle = delta_rle[delta_rle_index+1];

		    for(size_t i = 0; i < rle; i++){
			    sel_data[index] = sel_data[index-1] + delta;
			    index++;
		    }

		    delta_rle_index += 2;
	    }

	    return sel_data;
	}

    size_t GetDeltaRLESize(idx_t* delta_rle, idx_t key_count){
	    if(key_count <= 8){
		    return sizeof(sel_t) * key_count;
	    } else {
		    sel_t* sel_data = new sel_t[key_count];
		    sel_data[0] = delta_rle[0];

		    idx_t index = 1;
		    idx_t delta_rle_index = 1;

		    while(index < key_count){
			    idx_t delta = delta_rle[delta_rle_index];
			    idx_t rle = delta_rle[delta_rle_index+1];

			    for(size_t i = 0; i < rle; i++){
				    sel_data[index] = sel_data[index-1] + delta;
				    index++;
			    }

			    delta_rle_index += 2;
		    }
			delete[] sel_data;

		    return sizeof(idx_t) * delta_rle_index;
	    }

	}

    sel_t* ChangeSelDataToDeltaBitpack(const sel_t* sel_data, idx_t key_count) {
	    if(key_count <= 16){
		    sel_t* sel_data_copy = new sel_t[key_count];
		    std::copy(sel_data, sel_data + key_count, sel_data_copy);

		    return sel_data_copy;
	    }

	    vector<sel_t> delta_bitpack_vector;
	    delta_bitpack_vector.push_back(sel_data[0]);

	    for(size_t i = 1; i < key_count; i++){
		    if(sel_data[i] <= sel_data[i-1]){
			    // actually no possibility of equal for sel_buildn

			    // use 0 to represent the next ascending piece
			    delta_bitpack_vector.push_back(0);
			    // directly store the data as the start of the ascending piece
			    delta_bitpack_vector.push_back(sel_data[i]);
		    } else {
			    // store delta
			    sel_t delta = sel_data[i] - sel_data[i-1];
			    delta_bitpack_vector.push_back(delta);
		    }
	    }

	    idx_t data_length = delta_bitpack_vector.size();

	    sel_t* delta_bitpack = new sel_t[data_length];
	    std::copy(delta_bitpack_vector.begin(), delta_bitpack_vector.end(), delta_bitpack);
	    Compressed64ListWithSize* compressed_list = new Compressed64ListWithSize(delta_bitpack, data_length);
	    delete[] delta_bitpack;

	    // the array looks like
	    // start_1 delta_11 delta_12 ... delta_1n 0 start_2 delta_21 delta_22 ... delta_2n 0 start_3 delta_31 ...
	    // compress list is used to bitpack the delta, because delta is much smaller than the original data
	    // of course, the start is usually small because the piece ascends

		return reinterpret_cast<sel_t*>(compressed_list);
    }

    sel_t* ChangeDeltaBitpackToSelData(sel_t* delta_bitpack, idx_t key_count) {

	    if(key_count <= 16){
		    return delta_bitpack;
	    }

	    Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(delta_bitpack);

	    sel_t* sel_data = new sel_t[key_count];
	    sel_data[0] = compressed_list->Get(0);

	    idx_t index = 1;
	    idx_t delta_index = 1;

	    while(index < key_count){
		    idx_t delta = compressed_list->Get(delta_index);
		    if(delta == 0){
			    // the next ascending piece
			    delta_index++;
			    sel_data[index] = compressed_list->Get(delta_index);
		    } else {
			    sel_data[index] = sel_data[index-1] + delta;
		    }

		    index++;
		    delta_index++;
	    }

	    return sel_data;
	}

    size_t GetDeltaBitpackSize(sel_t* delta_bitpack, idx_t key_count) {
	    if(key_count <= 16){
		    return sizeof(sel_t) * key_count;
	    } else {
		    Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(delta_bitpack);
		    return compressed_list->GetBytesSize();
	    }
    }

    sel_t* ChangeSelDataToBitpack(sel_t* sel_data, idx_t count){

	    if(count < 30){
		    sel_t* sel_data_copy = new sel_t[count];
		    std::copy(sel_data, sel_data + count, sel_data_copy);

		    return sel_data_copy;
	    }

	    Compressed64ListWithSize* compressed_list = new Compressed64ListWithSize(sel_data, count);
	    return reinterpret_cast<sel_t*>(compressed_list);
    }

    sel_t* ChangeBitpackToSelData(sel_t* compressed_list, idx_t count){
	    if(count < 30){
		    return compressed_list;
	    }

	    Compressed64ListWithSize* compressed_list_with_size = reinterpret_cast<Compressed64ListWithSize*>(compressed_list);
	    sel_t* sel_data = new sel_t[count];
	    for(size_t i = 0; i < count; i++){
		    sel_data[i] = compressed_list_with_size->Get(i);
	    }

	    return sel_data;
	}

    size_t GetSelBitpackSize(sel_t* compressed_list, idx_t count){
	    if(count < 30){
		    return sizeof(sel_t) * count;
	    }

	    Compressed64ListWithSize* compressed_list_with_size = reinterpret_cast<Compressed64ListWithSize*>(compressed_list);
	    return compressed_list_with_size->GetBytesSize();
    }

    data_ptr_t* ChangeAddressToBitpack(data_ptr_t* address_data, idx_t count, idx_t is_ascend){

	    // case 1 too few data to compress, no need to compress
	    if(count < 4){
		    data_ptr_t* address_data_copy = new data_ptr_t[count];
		    std::copy(address_data, address_data + count, address_data_copy);

		    return address_data_copy;
	    }

	    // case 2 ascending order, use delta bitpack
	    if(is_ascend <= 2){
		    // we need is_ascend + 1 Compressed64ListDelta*
		    Compressed64ListDelta** compressed_delta_list = new Compressed64ListDelta*[is_ascend+1];
		    idx_t curr_idx = 0;

		    vector<idx_t> delta_bitpack_vector;
		    delta_bitpack_vector.push_back(reinterpret_cast<idx_t>(address_data[0]));
		    for(size_t i = 1; i < count; i++){
			    if(reinterpret_cast<idx_t>(address_data[i]) >= reinterpret_cast<idx_t>(address_data[i-1])){
				    idx_t delta = reinterpret_cast<idx_t>(address_data[i]) - reinterpret_cast<idx_t>(address_data[i-1]);
				    delta_bitpack_vector.push_back(delta);
			    } else {
				    idx_t data_length = delta_bitpack_vector.size();
				    if(data_length == 1){
					    idx_t* delta_bitpack = nullptr;
					    compressed_delta_list[curr_idx] = new Compressed64ListDelta(delta_bitpack, 0, delta_bitpack_vector[0]);
				    } else {
					    idx_t* delta_bitpack = new idx_t[data_length-1];
					    std::copy(delta_bitpack_vector.begin() + 1, delta_bitpack_vector.end(), delta_bitpack);
					    compressed_delta_list[curr_idx] = new Compressed64ListDelta(delta_bitpack, data_length-1, delta_bitpack_vector[0]);
					    delete[] delta_bitpack;
				    }
				    curr_idx++;

				    delta_bitpack_vector.clear();
				    delta_bitpack_vector.push_back(reinterpret_cast<idx_t>(address_data[i]));
			    }
		    }

		    idx_t data_length = delta_bitpack_vector.size();
		    if(data_length == 1){
			    idx_t* delta_bitpack = nullptr;
			    compressed_delta_list[curr_idx] = new Compressed64ListDelta(delta_bitpack, 0, delta_bitpack_vector[0]);
		    } else {
			    idx_t* delta_bitpack = new idx_t[data_length-1];
			    std::copy(delta_bitpack_vector.begin() + 1, delta_bitpack_vector.end(), delta_bitpack);
			    compressed_delta_list[curr_idx] = new Compressed64ListDelta(delta_bitpack, data_length-1, delta_bitpack_vector[0]);
			    delete[] delta_bitpack;
		    }

		    curr_idx++;

		    // quick check
		    if(curr_idx != is_ascend + 1){
			    throw std::runtime_error("curr_idx != is_ascend + 1");
		    }

		    return reinterpret_cast<data_ptr_t*>(compressed_delta_list);
	    }

	    // case 3 no ascending order, use bitpack
	    Compressed64ListWithSize* compressed_list_with_size = new Compressed64ListWithSize(reinterpret_cast<idx_t*>(address_data), count);
	    return reinterpret_cast<data_ptr_t*>(compressed_list_with_size);

    }

    data_ptr_t* ChangeBitpackToAddress(data_ptr_t* compressed_list, idx_t count, idx_t is_ascend){

	    if(count < 4){
		    return compressed_list;
	    }

	    if(is_ascend <= 2){
		    Compressed64ListDelta** compressed_delta_list = reinterpret_cast<Compressed64ListDelta**>(compressed_list);
		    data_ptr_t * address_data = new data_ptr_t[count];

		    idx_t curr_idx = 0;
		    for(size_t i = 0; i < is_ascend+1; i++){
			    idx_t curr_delta = 0;

			    address_data[curr_idx] = reinterpret_cast<data_ptr_t>(compressed_delta_list[i]->delta_base);
			    curr_idx++;

			    if(compressed_delta_list[i]->size != 0){
				    while(curr_delta < compressed_delta_list[i]->size){
					    address_data[curr_idx] = reinterpret_cast<data_ptr_t>(reinterpret_cast<idx_t>(address_data[curr_idx-1]) + compressed_delta_list[i]->Get(curr_delta));
					    curr_delta++;
					    curr_idx++;
				    }
			    }
		    }

		    return address_data;
	    }

	    Compressed64ListWithSize* compressed_list_with_size = reinterpret_cast<Compressed64ListWithSize*>(compressed_list);
	    data_ptr_t* address_data = new data_ptr_t[count];
	    for(size_t i = 0; i < count; i++){
		    address_data[i] = reinterpret_cast<data_ptr_t>(compressed_list_with_size->Get(i));
	    }

	    return address_data;
    }

    size_t GetAddressBitpackSize(data_ptr_t* compressed_list, idx_t count, idx_t is_ascend){

	    if(count < 4){
		    return sizeof(data_ptr_t) * count;
	    } else if (is_ascend <= 2){
		    Compressed64ListDelta** compressed_delta_list = reinterpret_cast<Compressed64ListDelta**>(compressed_list);
		    size_t total_size = 0;
		    total_size += sizeof(Compressed64ListDelta*) * (is_ascend+1);
		    for(size_t i = 0; i < is_ascend+1; i++){
			    total_size += compressed_delta_list[i]->GetBytesSize();
		    }
		    return total_size;
	    } else {
		    Compressed64ListWithSize* compressed_list_with_size = reinterpret_cast<Compressed64ListWithSize*>(compressed_list);
		    return compressed_list_with_size->GetBytesSize();
	    }

	}


    data_ptr_t* ChangeAddressToRLEBitpack(data_ptr_t* address_data, idx_t count, idx_t use_rle){
	    // case 1 use rle (+ bitpack)
	    if(use_rle){
		    vector<idx_t> rle_bitpack;
		    rle_bitpack.push_back(reinterpret_cast<idx_t>(address_data[0]));
		    idx_t curr_rle = 1;

			for(size_t i = 1; i < count; i++){
			    if(address_data[i] == address_data[i-1]){
				    curr_rle++;
			    } else {
				    rle_bitpack.push_back(curr_rle);
				    rle_bitpack.push_back(reinterpret_cast<idx_t>(address_data[i]));
				    curr_rle = 1;
			    }
		    }
		    rle_bitpack.push_back(curr_rle);

		    idx_t* rle_bitpack_array = new idx_t[rle_bitpack.size()];
		    std::copy(rle_bitpack.begin(), rle_bitpack.end(), rle_bitpack_array);

		    if(use_rle == 1){
			    Compressed64ListWithSize* compressed_list_with_size = new Compressed64ListWithSize(rle_bitpack_array, rle_bitpack.size());
			    delete[] rle_bitpack_array;
			    return reinterpret_cast<data_ptr_t*>(compressed_list_with_size);
		    } else if (use_rle == 2){
			    return reinterpret_cast<data_ptr_t*>(rle_bitpack_array);
		    }
	    }

	    // case 2 use original data
	    if(count <= 8){
		    data_ptr_t* address_data_copy = new data_ptr_t[count];
		    std::copy(address_data, address_data + count, address_data_copy);

		    return address_data_copy;
	    }

	    // case 3 use bitpack
	    Compressed64ListWithSize* compressed_list_with_size = new Compressed64ListWithSize(reinterpret_cast<idx_t*>(address_data), count);
	    return reinterpret_cast<data_ptr_t*>(compressed_list_with_size);

    }

    data_ptr_t* ChangeRLEBitpackToAddress(data_ptr_t* compressed_list, idx_t count, idx_t use_rle){

	    if(use_rle){
		    data_ptr_t* rle_bitpack_array = new data_ptr_t[count];
		    idx_t curr_idx = 0;

		    if(use_rle == 1){
			    Compressed64ListWithSize* compressed_list_with_size = reinterpret_cast<Compressed64ListWithSize*>(compressed_list);
			    for(size_t i = 0; i < compressed_list_with_size->size; i+=2){
				    rle_bitpack_array[curr_idx] = reinterpret_cast<data_ptr_t>(compressed_list_with_size->Get(i));
				    curr_idx++;

				    idx_t rle = compressed_list_with_size->Get(i+1);
				    for(size_t j = 0; j < rle - 1; j++){
					    rle_bitpack_array[curr_idx] = reinterpret_cast<data_ptr_t>(compressed_list_with_size->Get(i));
					    curr_idx++;
				    }
			    }
		    } else if(use_rle == 2){
			    idx_t rle_idx = 0;
			    while(curr_idx < count){
				    rle_bitpack_array[curr_idx] = compressed_list[rle_idx];
				    curr_idx++;

				    idx_t rle = reinterpret_cast<idx_t>(compressed_list[rle_idx+1]);
				    for(size_t j = 0; j < rle - 1; j++){
					    rle_bitpack_array[curr_idx] = compressed_list[rle_idx];
					    curr_idx++;
				    }
				    rle_idx += 2;
			    }
		    }

		    if(curr_idx != count){
			    throw std::runtime_error("curr_idx != count");
		    }

		    return rle_bitpack_array;
	    }

	    if(count <= 8){
		    return compressed_list;
	    }

	    Compressed64ListWithSize* compressed_list_with_size = reinterpret_cast<Compressed64ListWithSize*>(compressed_list);
	    data_ptr_t* address_data = new data_ptr_t[count];
	    for(size_t i = 0; i < count; i++){
		    address_data[i] = reinterpret_cast<data_ptr_t>(compressed_list_with_size->Get(i));
	    }

	    return address_data;
    }

    size_t GetAddressRLEBitpackSize(data_ptr_t* compressed_list, idx_t count, idx_t use_rle){

	    if(use_rle == 2) {
		    data_ptr_t *rle_bitpack_array = new data_ptr_t[count];

		    idx_t curr_idx = 0;
		    idx_t rle_idx = 0;

		    while (curr_idx < count) {
			    rle_bitpack_array[curr_idx] = compressed_list[rle_idx];
			    curr_idx++;

			    idx_t rle = reinterpret_cast<idx_t>(compressed_list[rle_idx + 1]);
			    for (size_t j = 0; j < rle - 1; j++) {
				    rle_bitpack_array[curr_idx] = compressed_list[rle_idx];
				    curr_idx++;
			    }
			    rle_idx += 2;
		    }
		    delete[] rle_bitpack_array;

		    return sizeof(data_ptr_t) * rle_idx;

	    } else if (use_rle == 1){
		    Compressed64ListWithSize* compressed_list_with_size = reinterpret_cast<Compressed64ListWithSize*>(compressed_list);
		    return compressed_list_with_size->GetBytesSize();
	    }

	    if(count <= 8){
		    return sizeof(data_ptr_t) * count;
	    }

	    Compressed64ListWithSize* compressed_list_with_size = reinterpret_cast<Compressed64ListWithSize*>(compressed_list);
	    return compressed_list_with_size->GetBytesSize();
	}

    idx_t GetUseRle(data_ptr_t* rhs_ptrs, idx_t result_count){
	    size_t rle_number = 1;
	    idx_t use_rle = 0;
	    idx_t prev_val = reinterpret_cast<idx_t>(rhs_ptrs[0]);
	    for (idx_t i = 1; i < result_count; i++) {
		    idx_t curr_val = reinterpret_cast<idx_t>(rhs_ptrs[i]);
		    if(curr_val != prev_val){
			    rle_number++;
			    prev_val = curr_val;
		    }
	    }

	    if( 2 * rle_number < result_count){
		    if (rle_number >= 12){ // use bitpack
			    use_rle = 1;
		    } else { // directly rle
			    use_rle = 2;
		    }
	    }

	    return use_rle;
    }

    data_ptr_t* ChangeAddressToDeltaRLE(data_ptr_t* address_data, idx_t count){
	    if(count <= 8){
		    data_ptr_t* address_data_copy = new data_ptr_t[count];
		    std::copy(address_data, address_data + count, address_data_copy);

		    return address_data_copy;
	    }

	    vector<Compressed64ListDelta*> compressed_delta_list;

	    idx_t start_idx = 0;
	    idx_t curr_base = reinterpret_cast<idx_t>(address_data[start_idx]);
	    while(reinterpret_cast<idx_t>(address_data[start_idx+1]) < reinterpret_cast<idx_t>(address_data[start_idx])){
		    // print all address_data
		    Compressed64ListDelta* compressed_delta = new Compressed64ListDelta(reinterpret_cast<idx_t*>(address_data), 0, curr_base);
		    compressed_delta_list.push_back(compressed_delta);

		    start_idx++;
		    curr_base = reinterpret_cast<idx_t>(address_data[start_idx]);
	    }

	    vector<idx_t> delta_rle_vector;
	    idx_t prev_delta = reinterpret_cast<idx_t>(address_data[start_idx+1]) - reinterpret_cast<idx_t>(address_data[start_idx]);
	    idx_t curr_rle = 1;

	    for(size_t i = start_idx+2; i < count; i++){
		    // guaranteed in the code when generating address_data, it is in ascending order
		    bool change_order = false;
		    while(i < count && (reinterpret_cast<idx_t>(address_data[i]) < reinterpret_cast<idx_t>(address_data[i-1]))){
			    change_order = true;
			    if(curr_rle){
				    delta_rle_vector.push_back(prev_delta);
				    delta_rle_vector.push_back(curr_rle);

				    idx_t *delta_rle = new idx_t[delta_rle_vector.size()];
				    std::copy(delta_rle_vector.begin(), delta_rle_vector.end(), delta_rle);

				    Compressed64ListDelta* compressed_delta = new Compressed64ListDelta(delta_rle, delta_rle_vector.size(), curr_base);
				    compressed_delta_list.push_back(compressed_delta);

				    delete[] delta_rle;

				    delta_rle_vector.clear();
				    curr_base = reinterpret_cast<idx_t>(address_data[i]);
				    curr_rle = 0;

			    } else{
				    Compressed64ListDelta* compressed_delta = new Compressed64ListDelta(reinterpret_cast<idx_t*>(address_data), 0, curr_base);
				    compressed_delta_list.push_back(compressed_delta);

				    delta_rle_vector.clear();
				    curr_base = reinterpret_cast<idx_t>(address_data[i]);
				    curr_rle = 0;
			    }

			    i++;

			    if(i >= count){
				    break;
			    }
		    }

		    if(i >= count){
			    break;
		    }

		    if(change_order){
			    prev_delta = reinterpret_cast<idx_t>(address_data[i]) - reinterpret_cast<idx_t>(address_data[i-1]);
			    curr_rle = 1;

			    continue;
		    }

		    idx_t delta = reinterpret_cast<idx_t>(address_data[i]) - reinterpret_cast<idx_t>(address_data[i-1]);
		    if(delta == prev_delta){
			    curr_rle++;
		    } else {
			    delta_rle_vector.push_back(prev_delta);
			    delta_rle_vector.push_back(curr_rle);

			    prev_delta = delta;
			    curr_rle = 1;
		    }
	    }

	    if(curr_rle){
		    delta_rle_vector.push_back(prev_delta);
		    delta_rle_vector.push_back(curr_rle);
		    idx_t *delta_rle = new idx_t[delta_rle_vector.size()];
		    std::copy(delta_rle_vector.begin(), delta_rle_vector.end(), delta_rle);

		    Compressed64ListDelta* compressed_delta = new Compressed64ListDelta(delta_rle, delta_rle_vector.size(), curr_base);
		    compressed_delta_list.push_back(compressed_delta);

		    delete[] delta_rle;

	    } else {
		    Compressed64ListDelta* compressed_delta = new Compressed64ListDelta(reinterpret_cast<idx_t*>(0), 0, curr_base);
		    compressed_delta_list.push_back(compressed_delta);
	    }

	    Compressed64ListDelta** compressed_delta_list_array = new Compressed64ListDelta*[compressed_delta_list.size()];
	    std::copy(compressed_delta_list.begin(), compressed_delta_list.end(), compressed_delta_list_array);
		return reinterpret_cast<data_ptr_t*>(compressed_delta_list_array);
	}

    data_ptr_t* ChangeDeltaRLEToAddress(data_ptr_t* compressed_list, idx_t count){

	    if(count <= 8){
		    return compressed_list;
	    }

	    Compressed64ListDelta** compressed_delta_list = reinterpret_cast<Compressed64ListDelta**>(compressed_list);
	    data_ptr_t* address_data = new data_ptr_t[count];

	    idx_t index = 0;
	    idx_t ptr_index = 0;

	    while(index < count){
		    address_data[index] = reinterpret_cast<data_ptr_t>(compressed_delta_list[ptr_index]->delta_base);
		    index++;

		    for(size_t i = 0; i < compressed_delta_list[ptr_index]->size; i += 2){
			    idx_t delta = compressed_delta_list[ptr_index]->Get(i);
			    idx_t rle = compressed_delta_list[ptr_index]->Get(i+1);

			    for(size_t j = 0; j < rle; j++){
				    address_data[index] = reinterpret_cast<data_ptr_t>(reinterpret_cast<idx_t>(address_data[index-1]) + delta);
				    index++;
			    }
		    }

		    ptr_index++;
	    }
	    return address_data;
	}

	size_t GetAddressDeltaRLESize(data_ptr_t* compressed_list, idx_t count){

	    if(count <= 8){
		    return sizeof(data_ptr_t) * count;
	    }

	    Compressed64ListDelta** compressed_delta_list = reinterpret_cast<Compressed64ListDelta**>(compressed_list);

	    idx_t index = 0;
	    idx_t ptr_index = 0;

	    while(index < count){
		    Compressed64ListDelta* compressed_delta = compressed_delta_list[ptr_index];
		    index++;

		    for(size_t i = 0; i < compressed_delta->size; i += 2){
			    idx_t rle = compressed_delta->Get(i+1);
			    index += rle;
		    }

		    ptr_index++;
	    }

	    size_t total_size = 0;

	    for(size_t i = 0; i < ptr_index; i++){
		    total_size += compressed_delta_list[i]->GetBytesSize();
	    }

	    return total_size;
	}


    idx_t* ChangeSelDataToRLE(sel_t* sel_data, idx_t key_count){

	    if(key_count <= 4){
		    sel_t* sel_data_copy = new sel_t[key_count];
		    std::copy(sel_data, sel_data + key_count, sel_data_copy);
		    return reinterpret_cast<idx_t*>(sel_data_copy);
	    }

	    vector<idx_t> rle_vector;

	    // we should store the first element
	    idx_t prev_val = sel_data[0];
	    idx_t curr_rle = 1;

	    for(size_t i = 1; i < key_count; i++) {
		    idx_t curr_val = sel_data[i];

		    if (curr_val == prev_val) {
			    curr_rle++;
		    } else {
			    rle_vector.push_back(prev_val);
			    rle_vector.push_back(curr_rle);

			    prev_val = curr_val;
			    curr_rle = 1;
		    }
	    }

	    rle_vector.push_back(prev_val);
	    rle_vector.push_back(curr_rle);

	    idx_t *rle = new idx_t[rle_vector.size()];
	    std::copy(rle_vector.begin(), rle_vector.end(), rle);

	    return rle;
    }

    sel_t* ChangeRLEToSelData(idx_t* rle, idx_t key_count, idx_t offset) {

	    if (key_count <= 4) {
		    sel_t* rle_sel = reinterpret_cast<sel_t *>(rle);
		    for(size_t i = 0; i < key_count; i++){
			    rle_sel[i] += offset;
		    }
		    return rle_sel;
	    }

	    sel_t *sel_data = new sel_t[key_count];

	    idx_t index = 0;
	    idx_t rle_index = 0;

	    while (index < key_count) {
		    idx_t rle_num = rle[rle_index + 1];

		    for (size_t i = 0; i < rle_num; i++) {
			    sel_data[index] = rle[rle_index] + offset;
			    index++;
		    }

		    rle_index += 2;
	    }

	    return sel_data;
    }

    size_t GetRLESize(idx_t* rle, idx_t key_count) {

	    if (key_count <= 4) {
		    return sizeof(sel_t) * key_count;
	    }

	    idx_t index = 0;
	    idx_t rle_index = 0;

	    while (index < key_count) {
		    idx_t rle_num = rle[rle_index + 1];
		    index += rle_num;
		    rle_index += 2;
	    }

	    return sizeof(idx_t) * rle_index;
	}

}

#endif