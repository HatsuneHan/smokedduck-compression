//
// Created by hxy on 8/8/24.
//

#ifdef LINEAGE
#pragma once
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"


namespace duckdb{

class Compressed64List;
class CompressedScanArtifactList;

// use to replace vector<idx_t>

class Compressed64List{
public:
	explicit Compressed64List()
	    : delta_buffer(nullptr), base(0), delta_bit_size(0), delta_buffer_size(0) {};

	~Compressed64List() {
		delete[] delta_buffer; // Ensure to free the allocated memory if any
	};

	void WriteBitsToBuffer(idx_t curr_buffer_bit_size, idx_t value);
	idx_t ReadBitsFromBuffer(idx_t read_buffer_from_bit);
	void PushBack(idx_t sel, idx_t artifact_size);
	idx_t Get(idx_t index);
	idx_t operator[](idx_t index);
	idx_t GetBytesSize();

public:
	unsigned char* delta_buffer;         // Store the bitpacked delta values
	idx_t base;              // Store the base value
	data_t delta_bit_size;     // Store the bit size of each packed delta value, no more than 64 bits
	idx_t delta_buffer_size; // Store the byte size of the delta buffer
};

class CompressedScanArtifactList{
public:
	// Constructor
	explicit CompressedScanArtifactList()
	    : size(0) {}

	// Destructor
	~CompressedScanArtifactList() {
		// Member variables `sel_addr` and `count` will be automatically destroyed
	}
	
	void PushBack(idx_t sel_addr_p, idx_t count_p, idx_t start_p, idx_t vector_index_p){
		this->sel_addr.PushBack(sel_addr_p, size);
		this->count.PushBack(count_p, size);
		this->start.PushBack(start_p, size);
		this->vector_index.PushBack(vector_index_p, size);
		
		size++;
	}
	
	idx_t GetBytesSize() {
		return this->sel_addr.GetBytesSize() + this->count.GetBytesSize() + this->start.GetBytesSize() + this->vector_index.GetBytesSize() + sizeof(size);
	}

public:
	// Member variables
	Compressed64List sel_addr;
	Compressed64List count;
	Compressed64List start;
	Compressed64List vector_index;

	idx_t size;

};

}


#endif
