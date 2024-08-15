//
// Created by hxy on 8/8/24.
//

#ifdef LINEAGE
#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"

// #include "duckdb/execution/lineage/lineage_manager.hpp"

#include <iostream>

namespace duckdb{

class Compressed64List;
class CompressedScanArtifactList;
class CompressedFilterArtifactList;
class CompressedAddressArtifactList;
class CompressedCombineArtifactList;
class CompressedAddressSelArtifactList;
class CompressedJoinGatherArtifactList;
class CompressedPerfectJoinArtifactList;
class CompressedPerfectFullScanHTArtifactList;
class CompressedLimitArtifactList;
class CompressedReorderLogArtifactList;
class CompressedCrossArtifactList;
class CompressedNLJArtifactList;
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
	void Resize(idx_t size_p);

public:
	unsigned char* delta_buffer;         // Store the bitpacked delta values
	idx_t base;              // Store the base value
	data_t delta_bit_size;     // Store the bit size of each packed delta value, no more than 64 bits
	idx_t delta_buffer_size; // Store the byte size of the delta buffer
};

struct CompressedScanArtifacts {
	Compressed64List sel;
	Compressed64List count;
	Compressed64List start;
	Compressed64List vector_index;
};

class CompressedScanArtifactList{
public:
	// Constructor
	explicit CompressedScanArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedScanArtifactList() {
		for(size_t i = 0; i < size; i++){
			// when without compression, we use move(), so we have the ownership of the memory
			// if we delete the memory, we also need to delete the memory of the pointer we record
			sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->sel[i]);
			delete[] sel_addr;
		}
		delete artifacts;
	}

	void Clear(){
		for(size_t i = 0; i < size; i++){
			// when without compression, we use move(), so we have the ownership of the memory
			// if we delete the memory, we also need to delete the memory of the pointer we record
			sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->sel[i]);
			delete[] sel_addr;
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}
	
	void PushBack(idx_t sel_p, idx_t count_p, idx_t start_p, idx_t vector_index_p){
		if (size == 0) {
			artifacts = new CompressedScanArtifacts();
		}

		this->artifacts->sel.PushBack(sel_p, size);
		this->artifacts->count.PushBack(count_p, size);
		this->artifacts->start.PushBack(start_p, size);
		this->artifacts->vector_index.PushBack(vector_index_p, size);
		
		size++;
	}
	
	size_t GetBytesSize() {
		// size of the all the elements, not calculating the size of the extra buffers held by the elements

		if(size == 0){
			return sizeof(CompressedScanArtifactList);
		} else {
			return this->artifacts->sel.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->start.GetBytesSize()
			       + this->artifacts->vector_index.GetBytesSize()
			       + sizeof(CompressedScanArtifactList);
		}
	}

public:
	// Member variables
	CompressedScanArtifacts* artifacts;

	size_t size;

};

struct CompressedFilterArtifacts {
	Compressed64List bitmap;
	Compressed64List bitmap_size;
	Compressed64List bitmap_is_compressed;

	Compressed64List start_bitmap_idx;
	Compressed64List count;
	Compressed64List in_start;
	Compressed64List use_bitmap;

};

class CompressedFilterArtifactList{
public:
	// Constructor
	explicit CompressedFilterArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedFilterArtifactList() {
		if(artifacts != nullptr){
			idx_t total_bitmap_num = artifacts->start_bitmap_idx[size];
			for(size_t i = 0; i < total_bitmap_num; i++){
				char* sel_addr = reinterpret_cast<char*>(artifacts->bitmap[i]);
				delete[] sel_addr;
			}
		}

		delete artifacts;
	}

	void Clear(){

		if(artifacts != nullptr){
			idx_t total_bitmap_num = artifacts->start_bitmap_idx[size];
			for(size_t i = 0; i < total_bitmap_num; i++){
				char* sel_addr = reinterpret_cast<char*>(artifacts->bitmap[i]);
				delete[] sel_addr;
			}
		}

		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(const vector<idx_t>& bitmap_p, const vector<idx_t>& bitmap_size_p, const vector<idx_t>& bitmap_is_compressed_p, const idx_t bitmap_num_p, idx_t count_p, idx_t in_start_p, idx_t use_bitmap_p){
		if (size == 0) {
			artifacts = new CompressedFilterArtifacts();
			artifacts->start_bitmap_idx.PushBack(0, size);
		}

		idx_t curr_total_bitmap_num = artifacts->start_bitmap_idx[size];

		for(size_t i = 0; i < bitmap_num_p; i++){
			artifacts->bitmap.PushBack(bitmap_p[i], curr_total_bitmap_num);
			artifacts->bitmap_size.PushBack(bitmap_size_p[i], curr_total_bitmap_num);
			artifacts->bitmap_is_compressed.PushBack(bitmap_is_compressed_p[i], curr_total_bitmap_num);
			curr_total_bitmap_num += 1;
		}

		artifacts->start_bitmap_idx.PushBack(curr_total_bitmap_num, size + 1);

		artifacts->count.PushBack(count_p, size);
		artifacts->in_start.PushBack(in_start_p, size);
		artifacts->use_bitmap.PushBack(use_bitmap_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedFilterArtifactList);
		} else {
			return this->artifacts->bitmap.GetBytesSize()
			       + this->artifacts->bitmap_size.GetBytesSize()
			       + this->artifacts->bitmap_is_compressed.GetBytesSize()
			       + this->artifacts->start_bitmap_idx.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + this->artifacts->use_bitmap.GetBytesSize()
			       + sizeof(CompressedFilterArtifactList);
		}
	}

	vector<idx_t> CompressBitmap(idx_t curr_bitmap_size, unsigned char* bitmap);
	unsigned char* DecompressBitmap(idx_t compressed_bitmap_size, idx_t bitmap_is_compressed, unsigned char* compressed_bitmap);

	vector<vector<idx_t>> ChangeSelToBitMap(sel_t* sel_data, idx_t result_count);
	sel_t* ChangeBitMapToSel(idx_t lsn);

public:
	// Member variables
	CompressedFilterArtifacts* artifacts;

	size_t size;
};

struct CompressedAddressArtifacts{
	Compressed64List addresses;
	Compressed64List count;
};

class CompressedAddressArtifactList{
public:
	// Constructor
	explicit CompressedAddressArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedAddressArtifactList() {
		for (size_t i = 0; i < size; i++) {
			data_ptr_t* addresses_addr = reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
			delete[] addresses_addr;
		}
		delete artifacts;
	}

	void Clear(){
		for (size_t i = 0; i < size; i++) {
			data_ptr_t* addresses_addr = reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
			delete[] addresses_addr;
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t addresses_p, idx_t count_p){
		if (size == 0) {
			artifacts = new CompressedAddressArtifacts();
		}

		this->artifacts->addresses.PushBack(addresses_p, size);
		this->artifacts->count.PushBack(count_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedAddressArtifactList);
		} else {
			return this->artifacts->addresses.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + sizeof(CompressedAddressArtifactList);
		}
	}

public:
	// Member variables
	CompressedAddressArtifacts* artifacts;

	size_t size;

};

struct CompressedCombineArtifacts{
	Compressed64List src;
	Compressed64List target;
	Compressed64List count;
};

class CompressedCombineArtifactList{
public:
	// Constructor
	explicit CompressedCombineArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedCombineArtifactList() {
		for (size_t i = 0; i < size; i++) {
			data_ptr_t* src_addr = reinterpret_cast<data_ptr_t*>(artifacts->src[i]);
			data_ptr_t* target_addr = reinterpret_cast<data_ptr_t*>(artifacts->target[i]);
			delete[] src_addr;
			delete[] target_addr;
		}
		delete artifacts;
	}

	void Clear(){
		for (size_t i = 0; i < size; i++) {
			data_ptr_t* src_addr = reinterpret_cast<data_ptr_t*>(artifacts->src[i]);
			data_ptr_t* target_addr = reinterpret_cast<data_ptr_t*>(artifacts->target[i]);
			delete[] src_addr;
			delete[] target_addr;
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t src_p, idx_t target_p, idx_t count_p){
		if (size == 0) {
			artifacts = new CompressedCombineArtifacts();
		}

		this->artifacts->src.PushBack(src_p, size);
		this->artifacts->target.PushBack(target_p, size);
		this->artifacts->count.PushBack(count_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedCombineArtifactList);
		} else {
			return this->artifacts->src.GetBytesSize()
			       + this->artifacts->target.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + sizeof(CompressedCombineArtifactList);
		}
	}

public:
	// Member variables
	CompressedCombineArtifacts* artifacts;

	size_t size;

};

struct CompressedAddressSelArtifacts{
	Compressed64List addresses;
	Compressed64List sel;
	Compressed64List count;
	Compressed64List in_start;
};

class CompressedAddressSelArtifactList{
public:
	// Constructor
	explicit CompressedAddressSelArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedAddressSelArtifactList() {
		for (size_t i = 0; i < size; i++) {
			data_ptr_t* addresses_addr = reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
			sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->sel[i]);
			delete[] addresses_addr;
			delete[] sel_addr;
		}
		delete artifacts;
	}

	void Clear(){
		for (size_t i = 0; i < size; i++) {
			data_ptr_t* addresses_addr = reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
			sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->sel[i]);
			delete[] addresses_addr;
			delete[] sel_addr;
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t addresses_p, idx_t sel_p, idx_t count_p, idx_t in_start_p){
		if (size == 0) {
			artifacts = new CompressedAddressSelArtifacts();
		}

		this->artifacts->addresses.PushBack(addresses_p, size);
		this->artifacts->sel.PushBack(sel_p, size);
		this->artifacts->count.PushBack(count_p, size);
		this->artifacts->in_start.PushBack(in_start_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedAddressSelArtifactList);
		} else {
			return this->artifacts->addresses.GetBytesSize()
			       + this->artifacts->sel.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + sizeof(CompressedAddressSelArtifactList);
		}
	}

public:
	// Member variables
	CompressedAddressSelArtifacts* artifacts;

	size_t size;

};

struct CompressedJoinGatherArtifacts{
	Compressed64List rhs;
	Compressed64List lhs;
	Compressed64List count;
	Compressed64List in_start;
};

class CompressedJoinGatherArtifactList{
public:
	// Constructor
	explicit CompressedJoinGatherArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedJoinGatherArtifactList() {
		for (size_t i = 0; i < size; i++) {
			data_ptr_t* rhs_addr = reinterpret_cast<data_ptr_t*>(artifacts->rhs[i]);
			sel_t* lhs_addr = reinterpret_cast<sel_t*>(artifacts->lhs[i]);
			delete[] rhs_addr;
			delete[] lhs_addr;
		}
		delete artifacts;
	}

	void Clear(){
		for (size_t i = 0; i < size; i++) {
			data_ptr_t* rhs_addr = reinterpret_cast<data_ptr_t*>(artifacts->rhs[i]);
			sel_t* lhs_addr = reinterpret_cast<sel_t*>(artifacts->lhs[i]);
			delete[] rhs_addr;
			delete[] lhs_addr;
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t rhs_p, idx_t lhs_p, idx_t count_p, idx_t in_start_p){
		if (size == 0) {
			artifacts = new CompressedJoinGatherArtifacts();
		}

		this->artifacts->rhs.PushBack(rhs_p, size);
		this->artifacts->lhs.PushBack(lhs_p, size);
		this->artifacts->count.PushBack(count_p, size);
		this->artifacts->in_start.PushBack(in_start_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedJoinGatherArtifactList);
		} else {
			return this->artifacts->rhs.GetBytesSize()
			       + this->artifacts->lhs.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + sizeof(CompressedJoinGatherArtifactList);
		}
	}

public:
	// Member variables
	CompressedJoinGatherArtifacts* artifacts;

	size_t size;

};

struct CompressedPerfectJoinArtifacts{
	Compressed64List left;
	Compressed64List right;
	Compressed64List count;
	Compressed64List in_start;
};

class CompressedPerfectJoinArtifactList{
public:
	// Constructor
	explicit CompressedPerfectJoinArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedPerfectJoinArtifactList() {
		for (size_t i = 0; i < size; i++) {
			sel_t* left_addr = reinterpret_cast<sel_t*>(artifacts->left[i]);
			sel_t* right_addr = reinterpret_cast<sel_t*>(artifacts->right[i]);
			delete[] left_addr;
			delete[] right_addr;
		}
		delete artifacts;
	}

	void Clear(){
		for (size_t i = 0; i < size; i++) {
			sel_t* left_addr = reinterpret_cast<sel_t*>(artifacts->left[i]);
			sel_t* right_addr = reinterpret_cast<sel_t*>(artifacts->right[i]);
			delete[] left_addr;
			delete[] right_addr;
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t left_p, idx_t right_p, idx_t count_p, idx_t in_start_p){
		if (size == 0) {
			artifacts = new CompressedPerfectJoinArtifacts();
		}

		this->artifacts->left.PushBack(left_p, size);
		this->artifacts->right.PushBack(right_p, size);
		this->artifacts->count.PushBack(count_p, size);
		this->artifacts->in_start.PushBack(in_start_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedPerfectJoinArtifactList);
		} else {
			return this->artifacts->left.GetBytesSize()
			       + this->artifacts->right.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + sizeof(CompressedPerfectJoinArtifactList);
		}
	}

public:
	// Member variables
	CompressedPerfectJoinArtifacts* artifacts;

	size_t size;

};

struct CompressedPerfectFullScanHTArtifacts{
	Compressed64List sel_build;
	Compressed64List sel_tuples;
	Compressed64List row_locations;
	Compressed64List key_count;
	Compressed64List ht_count;

	Compressed64List vector_buffer_size;
};

class CompressedPerfectFullScanHTArtifactList{
public:
	// Constructor
	explicit CompressedPerfectFullScanHTArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedPerfectFullScanHTArtifactList() {
		for (size_t i = 0; i < size; i++) {
			sel_t* sel_build_addr = reinterpret_cast<sel_t*>(artifacts->sel_build[i]);
			sel_t* sel_tuples_addr = reinterpret_cast<sel_t*>(artifacts->sel_tuples[i]);
			data_ptr_t row_locations_addr = reinterpret_cast<data_ptr_t>(artifacts->row_locations[i]);
			delete[] sel_build_addr;
			delete[] sel_tuples_addr;
			delete[] row_locations_addr;
		}
		delete artifacts;
	}

	void Clear(){
		for (size_t i = 0; i < size; i++) {
			sel_t* sel_build_addr = reinterpret_cast<sel_t*>(artifacts->sel_build[i]);
			sel_t* sel_tuples_addr = reinterpret_cast<sel_t*>(artifacts->sel_tuples[i]);
			data_ptr_t row_locations_addr = reinterpret_cast<data_ptr_t>(artifacts->row_locations[i]);
			delete[] sel_build_addr;
			delete[] sel_tuples_addr;
			delete[] row_locations_addr;
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t sel_build_p, idx_t sel_tuples_p, idx_t row_locations_p, idx_t key_count_p, idx_t ht_count_p, idx_t vector_buffer_size_p){
		if (size == 0) {
			artifacts = new CompressedPerfectFullScanHTArtifacts();
		}

		this->artifacts->sel_build.PushBack(sel_build_p, size);
		this->artifacts->sel_tuples.PushBack(sel_tuples_p, size);
		this->artifacts->row_locations.PushBack(row_locations_p, size);
		this->artifacts->key_count.PushBack(key_count_p, size);
		this->artifacts->ht_count.PushBack(ht_count_p, size);
		this->artifacts->vector_buffer_size.PushBack(vector_buffer_size_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		// do not consider vector buffer size, it is only used for statistics
		if(size == 0){
			return sizeof(CompressedPerfectFullScanHTArtifactList);
		} else {
			return this->artifacts->sel_build.GetBytesSize()
			       + this->artifacts->sel_tuples.GetBytesSize()
			       + this->artifacts->row_locations.GetBytesSize()
			       + this->artifacts->key_count.GetBytesSize()
			       + this->artifacts->ht_count.GetBytesSize()
			       + sizeof(CompressedPerfectFullScanHTArtifactList);
		}
	}

public:
	// Member variables
	CompressedPerfectFullScanHTArtifacts* artifacts;

	size_t size;
};

struct CompressedLimitArtifacts{
	Compressed64List start;
	Compressed64List end;
	Compressed64List in_start;
};

class CompressedLimitArtifactList{
public:
	// Constructor
	explicit CompressedLimitArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedLimitArtifactList() {
		delete artifacts;
	}

	void Clear(){
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t start_p, idx_t end_p, idx_t in_start_p){
		if (size == 0) {
			artifacts = new CompressedLimitArtifacts();
		}

		this->artifacts->start.PushBack(start_p, size);
		this->artifacts->end.PushBack(end_p, size);
		this->artifacts->in_start.PushBack(in_start_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedLimitArtifactList);
		} else {
			return this->artifacts->start.GetBytesSize()
			       + this->artifacts->end.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + sizeof(CompressedLimitArtifactList);
		}
	}

public:
	// Member variables
	CompressedLimitArtifacts* artifacts;

	size_t size;

};

struct CompressedReorderLogArtifacts{
	Compressed64List index;
};

class CompressedReorderLogArtifactList{
public:
	// Constructor
	explicit CompressedReorderLogArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedReorderLogArtifactList() {
		delete artifacts;
	}

	void Clear(){
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void Resize(idx_t size_p){
		if (size == 0) {
			artifacts = new CompressedReorderLogArtifacts();
		}

		this->artifacts->index.Resize(size_p);
		size = size_p;
	}

	void PushBack(idx_t index_p){
		if (size == 0) {
			artifacts = new CompressedReorderLogArtifacts();
		}

		this->artifacts->index.PushBack(index_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedReorderLogArtifactList);
		} else {
			return this->artifacts->index.GetBytesSize()
			       + sizeof(CompressedReorderLogArtifactList);
		}
	}

public:
	// Member variables
	CompressedReorderLogArtifacts* artifacts;

	size_t size;

};

struct CompressedCrossArtifacts{
	Compressed64List branch_scan_lhs;
	Compressed64List position_in_chunk;
	Compressed64List scan_position;
	Compressed64List count;
	Compressed64List in_start;
};

class CompressedCrossArtifactList{
public:
	// Constructor
	explicit CompressedCrossArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedCrossArtifactList() {
		delete artifacts;
	}

	void Clear(){
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t branch_scan_lhs_p, idx_t position_in_chunk_p, idx_t scan_position_p, idx_t count_p, idx_t in_start_p){
		if (size == 0) {
			artifacts = new CompressedCrossArtifacts();
		}

		this->artifacts->branch_scan_lhs.PushBack(branch_scan_lhs_p, size);
		this->artifacts->position_in_chunk.PushBack(position_in_chunk_p, size);
		this->artifacts->scan_position.PushBack(scan_position_p, size);
		this->artifacts->count.PushBack(count_p, size);
		this->artifacts->in_start.PushBack(in_start_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedCrossArtifactList);
		} else {
			return this->artifacts->branch_scan_lhs.GetBytesSize()
			       + this->artifacts->position_in_chunk.GetBytesSize()
			       + this->artifacts->scan_position.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + sizeof(CompressedCrossArtifactList);
		}
	}

public:
	// Member variables
	CompressedCrossArtifacts* artifacts;

	size_t size;

};

struct CompressedNljArtifacts{
	Compressed64List left;
	Compressed64List right;
	Compressed64List count;
	Compressed64List current_row_index;
	Compressed64List out_start;
};

class CompressedNLJArtifactList{
public:
	// Constructor
	explicit CompressedNLJArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedNLJArtifactList() {
		for (size_t i = 0; i < size; i++) {
			sel_t* left_addr = reinterpret_cast<sel_t*>(artifacts->left[i]);
			sel_t* right_addr = reinterpret_cast<sel_t*>(artifacts->right[i]);
			delete[] left_addr;
			delete[] right_addr;
		}
		delete artifacts;
	}

	void Clear(){
		for (size_t i = 0; i < size; i++) {
			sel_t* left_addr = reinterpret_cast<sel_t*>(artifacts->left[i]);
			sel_t* right_addr = reinterpret_cast<sel_t*>(artifacts->right[i]);
			delete[] left_addr;
			delete[] right_addr;
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t left_p, idx_t right_p, idx_t count_p, idx_t current_row_index_p, idx_t out_start_p){
		if (size == 0) {
			artifacts = new CompressedNljArtifacts();
		}

		this->artifacts->left.PushBack(left_p, size);
		this->artifacts->right.PushBack(right_p, size);
		this->artifacts->count.PushBack(count_p, size);
		this->artifacts->current_row_index.PushBack(current_row_index_p, size);
		this->artifacts->out_start.PushBack(out_start_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return sizeof(CompressedNLJArtifactList);
		} else {
			return this->artifacts->left.GetBytesSize()
			       + this->artifacts->right.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->current_row_index.GetBytesSize()
			       + this->artifacts->out_start.GetBytesSize()
			       + sizeof(CompressedNLJArtifactList);
		}
	}

public:
	// Member variables
	CompressedNljArtifacts* artifacts;

	size_t size;

};


}


#endif
