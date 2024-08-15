//
// Created by hxy on 8/8/24.
//
#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_compression.hpp"

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

    idx_t Compressed64List::GetBytesSize() {
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

    vector<idx_t> CompressedFilterArtifactList::CompressBitmap(duckdb::idx_t curr_bitmap_size, unsigned char *bitmap) {

	    idx_t bitmap_size = (STANDARD_VECTOR_SIZE + 7) / 8;

	    vector<idx_t> result_vector;

	    if(curr_bitmap_size <= STANDARD_VECTOR_SIZE/4 || curr_bitmap_size >= 3*STANDARD_VECTOR_SIZE/4){
		    int max_compressed_size = duckdb_lz4::LZ4_compressBound(bitmap_size);
		    char *compressed_bitmap = new char[max_compressed_size];
		    int compressed_size =
		        duckdb_lz4::LZ4_compress_fast(reinterpret_cast<const char *>(bitmap),
		                                      compressed_bitmap, bitmap_size, max_compressed_size, 1);
		    if (compressed_size <= 0) {
			    throw std::runtime_error("Compression failed");
		    }

		    // resize bitmap
		    unsigned char *compressed_bitmap_copy = new unsigned char[compressed_size];
		    std::copy(compressed_bitmap, compressed_bitmap + compressed_size, compressed_bitmap_copy);

		    result_vector.push_back(reinterpret_cast<idx_t>(compressed_bitmap_copy));
		    result_vector.push_back(compressed_size);
		    result_vector.push_back(1);

		    // destruction
		    delete[] compressed_bitmap; // delete the original compressed bitmap
		    delete[] bitmap;            // delete the original bitmap

	    } else {
		    // no need to lz4 compress
		    result_vector.push_back(reinterpret_cast<idx_t>(bitmap));
		    result_vector.push_back(bitmap_size);
		    result_vector.push_back(0);
	    }

	    return result_vector;
    }

    unsigned char* CompressedFilterArtifactList::DecompressBitmap(idx_t compressed_bitmap_size,
                                                                  idx_t bitmap_is_compressed,
                                                                  unsigned char *compressed_bitmap) {
	    unsigned char* decompressed_bitmap;

	    if(bitmap_is_compressed){
		    // lz4 uncompress
		    size_t dst_size = (STANDARD_VECTOR_SIZE + 7) / 8;
		    char* tmp_decompressed_bitmap = new char[dst_size];
		    size_t decompressed_size = duckdb_lz4::LZ4_decompress_safe(reinterpret_cast<char *>(compressed_bitmap), tmp_decompressed_bitmap, compressed_bitmap_size, dst_size);
		    if (decompressed_size != dst_size) {
			    throw std::runtime_error("Decompression failed");
		    }
		    decompressed_bitmap = reinterpret_cast<unsigned char*>(tmp_decompressed_bitmap);
	    } else {
		    decompressed_bitmap = compressed_bitmap;
	    }

	    return decompressed_bitmap;
    }

    vector<vector<idx_t>> CompressedFilterArtifactList::ChangeSelToBitMap(sel_t* sel_data, idx_t result_count){
	    vector<idx_t> bitmap_vector;
	    vector<idx_t> bitmap_sizes;
	    vector<idx_t> bitmap_is_compressed;
	    vector<idx_t> use_bitmap;

	    if(result_count >= 32) {
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

					vector<idx_t> result_vector = CompressBitmap(curr_bitmap_size, bitmap);
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
		    vector<idx_t> result_vector = CompressBitmap(curr_bitmap_size, bitmap);

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

    sel_t* CompressedFilterArtifactList::ChangeBitMapToSel(idx_t lsn) {
	    idx_t count = this->artifacts->count[lsn];
	    idx_t offset = this->artifacts->in_start[lsn];
	    idx_t start_bitmap_idx = this->artifacts->start_bitmap_idx[lsn];
	    idx_t use_bitmap = this->artifacts->use_bitmap[lsn];

	    idx_t bitmap_num = this->artifacts->start_bitmap_idx[lsn+1] - start_bitmap_idx;

	    if (bitmap_num) {
		    if (use_bitmap){
			    sel_t* sel_copy = new sel_t[count];
			    size_t index = 0;

			    for(size_t i = 0; i < bitmap_num; i++){

				    unsigned char* decompressed_bitmap = DecompressBitmap(this->artifacts->bitmap_size[start_bitmap_idx+i],
				                                                 this->artifacts->bitmap_is_compressed[start_bitmap_idx+i],
				                                                  reinterpret_cast<unsigned char*>(this->artifacts->bitmap[start_bitmap_idx+i]));

				    for (size_t j = 0; j < STANDARD_VECTOR_SIZE; ++j) {
					    if (decompressed_bitmap[j / 8] & (1 << (7 - (j % 8)))) {
						    sel_copy[index++] = static_cast<sel_t>(j) + offset; // PostProcess() here
					    }
				    }
			    }
			    return sel_copy;
		    }
		    else {
			    return reinterpret_cast<sel_t*>(this->artifacts->bitmap[start_bitmap_idx]);
		    }
	    }

	    return nullptr;

    }


}

#endif