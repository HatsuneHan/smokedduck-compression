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

#include <zstd.h>

namespace duckdb{

class Compressed64List;
class Compressed64ListWithSize;
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
class CompressedFinalizeAddressArtifactList;

enum class CompressionMethod {
	LZ4,
	ZSTD
};

vector<idx_t> CompressBitmap(idx_t, unsigned char*, CompressionMethod);
unsigned char* DecompressBitmap(idx_t, idx_t, unsigned char *);

vector<idx_t> CompressDataTArray(idx_t, data_ptr_t, CompressionMethod);
data_ptr_t DecompressDataTArray(idx_t, idx_t, data_ptr_t, idx_t);


vector<vector<idx_t>> ChangeSelToBitMap(sel_t*, idx_t, CompressionMethod);

template<typename ARTIFACT_TYPE>
sel_t* ChangeBitMapToSel(const ARTIFACT_TYPE&, idx_t, idx_t);

sel_t* ChangeSelDataToDeltaRLE(const sel_t*, idx_t);
sel_t* ChangeDeltaRLEToSelData(sel_t*, idx_t);
size_t GetSelDataDeltaRLESize(sel_t*, idx_t);

sel_t* ChangeSelDataToDeltaBitpack(const sel_t*, idx_t);
sel_t* ChangeDeltaBitpackToSelData(sel_t*, idx_t);
size_t GetDeltaBitpackSize(sel_t*, idx_t);

sel_t* ChangeSelDataToBitpack(sel_t*, idx_t);
sel_t* ChangeBitpackToSelData(sel_t*, idx_t);
size_t GetSelBitpackSize(sel_t*, idx_t);

data_ptr_t* ChangeAddressToBitpack(data_ptr_t*, idx_t, idx_t);
data_ptr_t* ChangeBitpackToAddress(data_ptr_t*, idx_t, idx_t);
size_t GetAddressBitpackSize(data_ptr_t*, idx_t, idx_t);

data_ptr_t* ChangeAddressToRLEBitpack(data_ptr_t*, idx_t, idx_t);
data_ptr_t* ChangeRLEBitpackToAddress(data_ptr_t*, idx_t, idx_t);
size_t GetAddressRLEBitpackSize(data_ptr_t*, idx_t, idx_t);
idx_t GetUseRle(data_ptr_t*, idx_t);

data_ptr_t* ChangeAddressToDeltaRLE(data_ptr_t*, idx_t);
data_ptr_t* ChangeDeltaRLEToAddress(data_ptr_t*, idx_t);
size_t GetAddressDeltaRLESize(data_ptr_t*, idx_t);

sel_t* ChangeSelDataToRLE(sel_t*, idx_t);
sel_t* ChangeRLEToSelData(sel_t*, idx_t, idx_t);
size_t GetRLESize(sel_t*, idx_t);

// use to replace vector<idx_t>

class Compressed64List{
public:
	explicit Compressed64List()
	    : delta_buffer(nullptr), base(0), delta_bit_size(0), delta_buffer_size(0) {};

	virtual ~Compressed64List() {
		if (delta_buffer != nullptr) {
			delete[] delta_buffer; // Ensure to free the allocated memory if any
		}
	};

	void WriteBitsToBuffer(idx_t curr_buffer_bit_size, idx_t value);
	idx_t ReadBitsFromBuffer(idx_t read_buffer_from_bit);
	void PushBack(idx_t sel, idx_t artifact_size);
	idx_t Get(idx_t index);
	idx_t operator[](idx_t index);
	virtual size_t GetBytesSize();
	void Resize(idx_t size_p);

public:
	unsigned char* delta_buffer;         // Store the bitpacked delta values
	idx_t base;              // Store the base value
	data_t delta_bit_size;     // Store the bit size of each packed delta value, no more than 64 bits
	idx_t delta_buffer_size; // Store the byte size of the delta buffer
};

class Compressed64ListWithSize : public Compressed64List {
public:

	explicit Compressed64ListWithSize() : size(0) {};
	~Compressed64ListWithSize() override = default;

	template<typename DATA_TYPE>
	Compressed64ListWithSize(DATA_TYPE* sel_data, idx_t count) : Compressed64List(), size(0) {
		if (count == 0) {
			return;
		}

		idx_t sel_start = static_cast<idx_t>(sel_data[0]);
		base = sel_start & ~0x1ull;
		delta_bit_size = static_cast<data_t>(1);

		delta_buffer_size = 0;
		delta_buffer = nullptr;

		for (idx_t i = 1; i < count; i++) {
			idx_t sel = static_cast<idx_t>(sel_data[i]);

			idx_t check_addr_with_base = (sel ^ base) >> delta_bit_size;
			bool base_addr_is_usable = !check_addr_with_base;

			if (base_addr_is_usable){
				continue;
			} else {
				data_t reduced_bit_size = 0;
				while (check_addr_with_base) {
					check_addr_with_base >>= 1;
					reduced_bit_size++;
				}

				delta_bit_size = delta_bit_size + reduced_bit_size;
				base = sel & ~((1ull << (delta_bit_size)) - 1);
			}
		}

		delta_buffer_size = (delta_bit_size * count + 7) / 8;
		delta_buffer = new unsigned char[delta_buffer_size];
		std::memset(delta_buffer, 0, delta_buffer_size);

		idx_t delta_addr_bitmask = delta_bit_size == 64 ? ~0ull : ((1ull << delta_bit_size) - 1);

		for (idx_t i = 0; i < count; i++) {
			idx_t sel = static_cast<idx_t>(sel_data[i]);
			WriteBitsToBuffer(i * delta_bit_size, sel & delta_addr_bitmask);
		}

		size = count;
	}

	void PushBack(idx_t sel) {
		Compressed64List::PushBack(sel, size);
		size++;
	}

	idx_t GetSize() const {
		return size;
	}

	size_t GetBytesSize() override {
		return sizeof(Compressed64ListWithSize) + delta_buffer_size;
	}

public:
	idx_t size;
};


class Compressed64ListDelta : public Compressed64ListWithSize {
public:
	explicit Compressed64ListDelta() : delta_base(0) {}
	~Compressed64ListDelta() override = default;

	template<typename DATA_TYPE>
	Compressed64ListDelta(DATA_TYPE* sel_data, idx_t count, idx_t delta_base_p) : Compressed64ListWithSize(sel_data, count), delta_base(delta_base_p) {}

	size_t GetBytesSize() override {
		return sizeof(Compressed64ListDelta) + delta_buffer_size;
	}

public:
	idx_t delta_base;
};

struct CompressedScanArtifacts {
	Compressed64List bitmap;
	Compressed64List bitmap_size;
	Compressed64List bitmap_is_compressed;

	Compressed64List start_bitmap_idx;
	Compressed64List count;
	Compressed64List start;
	Compressed64List vector_index;
	Compressed64List use_bitmap;
};

class CompressedScanArtifactList{
public:
	// Constructor
	explicit CompressedScanArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedScanArtifactList() {
		if(artifacts != nullptr){
			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}
		}

		delete artifacts;
	}

	void Clear(){
		if(artifacts != nullptr){
			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}
		}

		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(const vector<idx_t>& bitmap_p, const vector<idx_t>& bitmap_size_p, const vector<idx_t>& bitmap_is_compressed_p, const idx_t bitmap_num_p, idx_t count_p, idx_t start_p, idx_t vector_index_p, idx_t use_bitmap_p){
		if (size == 0) {
			artifacts = new CompressedScanArtifacts();
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
		artifacts->start.PushBack(start_p, size);
		artifacts->vector_index.PushBack(vector_index_p, size);
		artifacts->use_bitmap.PushBack(use_bitmap_p, size);

		size++;
	}

	size_t GetBytesSize() {
		// size of the all the elements, not calculating the size of the extra buffers held by the elements

		if(size == 0){
//			return sizeof(CompressedScanArtifactList);
			return 0;
		} else {
			return this->artifacts->bitmap.GetBytesSize()
			       + this->artifacts->bitmap_size.GetBytesSize()
			       + this->artifacts->bitmap_is_compressed.GetBytesSize()
			       + this->artifacts->start_bitmap_idx.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->start.GetBytesSize()
			       + this->artifacts->vector_index.GetBytesSize()
			       + this->artifacts->use_bitmap.GetBytesSize();
//			       + sizeof(CompressedScanArtifactList);
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
			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}
		}

		delete artifacts;
	}

	void Clear(){
		if(artifacts != nullptr){
			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
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
//			return sizeof(CompressedFilterArtifactList);
			return 0;
		} else {
			return this->artifacts->bitmap.GetBytesSize()
			       + this->artifacts->bitmap_size.GetBytesSize()
			       + this->artifacts->bitmap_is_compressed.GetBytesSize()
			       + this->artifacts->start_bitmap_idx.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + this->artifacts->use_bitmap.GetBytesSize();
//			       + sizeof(CompressedFilterArtifactList);
		}
	}

public:
	// Member variables
	CompressedFilterArtifacts* artifacts;

	size_t size;
};

struct CompressedAddressArtifacts{
	Compressed64List addresses;
	Compressed64List is_ascend;

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
			if((artifacts->count[i] / (artifacts->is_ascend[i]+1)) >= 16){
				if(artifacts->count[i] <= 8){
					data_ptr_t* addresses_addr = reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
					delete[] addresses_addr;
				} else {
					Compressed64ListDelta** compressed_delta_list = reinterpret_cast<Compressed64ListDelta**>(artifacts->addresses[i]);
					delete[] compressed_delta_list;
				}
			} else {
				if(artifacts->count[i] < 4){
					data_ptr_t* addresses_addr = reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
					delete[] addresses_addr;
				} else {
					Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(artifacts->addresses[i]);
					delete compressed_list;
				}
			}
		}
		delete artifacts;

	}

	void Clear(){
		for (size_t i = 0; i < size; i++) {
			if((artifacts->count[i] / (artifacts->is_ascend[i]+1)) >= 16){
				if(artifacts->count[i] <= 8){
					data_ptr_t* addresses_addr = reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
					delete[] addresses_addr;
				} else {
					Compressed64ListDelta** compressed_delta_list = reinterpret_cast<Compressed64ListDelta**>(artifacts->addresses[i]);
					delete[] compressed_delta_list;
				}
			} else {
				if(artifacts->count[i] < 4){
					data_ptr_t* addresses_addr = reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
					delete[] addresses_addr;
				} else {
					Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(artifacts->addresses[i]);
					delete compressed_list;
				}
			}
		}
		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t addresses_p, idx_t is_ascend, idx_t count_p){
		if (size == 0) {
			artifacts = new CompressedAddressArtifacts();
		}

		this->artifacts->addresses.PushBack(addresses_p, size);
		this->artifacts->is_ascend.PushBack(is_ascend, size);
		this->artifacts->count.PushBack(count_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
//			return sizeof(CompressedAddressArtifactList);
			return 0;
		} else {
			return this->artifacts->addresses.GetBytesSize()
			       + this->artifacts->is_ascend.GetBytesSize()
			       + this->artifacts->count.GetBytesSize();
//			       + sizeof(CompressedAddressArtifactList);
		}
	}

public:
	// Member variables
	CompressedAddressArtifacts* artifacts;

	size_t size;

};

struct CompressedFinalizeAddressArtifacts{
	Compressed64List addresses;
	Compressed64List count;

};

class CompressedFinalizeAddressArtifactList{
public:
	// Constructor
	explicit CompressedFinalizeAddressArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedFinalizeAddressArtifactList() {

		if(artifacts != nullptr) {
			for (size_t i = 0; i < size; i++) {
				idx_t res_count = artifacts->count[i];
				if (res_count <= 8) {
					delete[] reinterpret_cast<data_ptr_t *>(artifacts->addresses[i]);
				} else {
					delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->addresses[i]);
				}
			}
		}
		delete artifacts;

	}

	void Clear(){
		if(artifacts != nullptr) {
			for (size_t i = 0; i < size; i++) {
				idx_t res_count = artifacts->count[i];
				if (res_count <= 8) {
					delete[] reinterpret_cast<data_ptr_t *>(artifacts->addresses[i]);
				} else {
					delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->addresses[i]);
				}
			}
		}
		delete artifacts;

		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t addresses_p, idx_t count_p){
		if (size == 0) {
			artifacts = new CompressedFinalizeAddressArtifacts();
		}

		this->artifacts->addresses.PushBack(addresses_p, size);
		this->artifacts->count.PushBack(count_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
			return 0;
//			return sizeof(CompressedFinalizeAddressArtifactList);
		} else {
			return this->artifacts->addresses.GetBytesSize()
			       + this->artifacts->count.GetBytesSize();
//			       + sizeof(CompressedFinalizeAddressArtifactList);
		}
	}

public:
	// Member variables
	CompressedFinalizeAddressArtifacts* artifacts;

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

		if(artifacts != nullptr){
			for (size_t i = 0; i < size; i++) {
				idx_t res_count = artifacts->count[i];
				if(res_count <= 8){
					delete[] reinterpret_cast<data_ptr_t*>(artifacts->src[i]);
				} else {
					delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->src[i]);
				}
			}

			for (size_t i = 0; i < size; i++) {
				idx_t res_count = artifacts->count[i];
				if(res_count <= 8){
					delete[] reinterpret_cast<data_ptr_t*>(artifacts->target[i]);
				} else {
					delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->target[i]);
				}
			}
		}
		delete artifacts;

	}

	void Clear(){
		if(artifacts != nullptr){
			for (size_t i = 0; i < size; i++) {
				idx_t res_count = artifacts->count[i];
				if(res_count <= 8){
					delete[] reinterpret_cast<data_ptr_t*>(artifacts->src[i]);
				} else {
					delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->src[i]);
				}
			}

			for (size_t i = 0; i < size; i++) {
				idx_t res_count = artifacts->count[i];
				if(res_count <= 8){
					delete[] reinterpret_cast<data_ptr_t*>(artifacts->target[i]);
				} else {
					delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->target[i]);
				}
			}
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
//			return sizeof(CompressedCombineArtifactList);
			return 0;
		} else {
			return this->artifacts->src.GetBytesSize()
			       + this->artifacts->target.GetBytesSize()
			       + this->artifacts->count.GetBytesSize();
//			       + sizeof(CompressedCombineArtifactList);
		}
	}

public:
	// Member variables
	CompressedCombineArtifacts* artifacts;

	size_t size;

};

struct CompressedAddressSelArtifacts{
	Compressed64List addresses;
	Compressed64List is_ascend;

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
			idx_t res_count = artifacts->count[i];
			idx_t is_ascend = artifacts->is_ascend[i];

			if(res_count / (is_ascend+1) >= 16){
				if(res_count <= 8){
					delete[] reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
				} else {
					delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->addresses[i]);
				}
			} else {
				if(res_count < 4){
					delete[] reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
				} else {
					Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(artifacts->addresses[i]);
					delete compressed_list;
				}
			}
		}

		for (size_t i = 0; i < size; i++) {
			idx_t count = artifacts->count[i];
			if(artifacts->sel[i] != 0){
				if(count <= 16){
					sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->sel[i]);
					delete[] sel_addr;
				} else {
					Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(artifacts->sel[i]);
					delete compressed_list;
				}
			}
		}

		delete artifacts;

	}

	void Clear(){

		for (size_t i = 0; i < size; i++) {
			idx_t res_count = artifacts->count[i];
			idx_t is_ascend = artifacts->is_ascend[i];

			if(res_count / (is_ascend+1) >= 16){
				if(res_count <= 8){
					delete[] reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
				} else {
					delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->addresses[i]);
				}
			} else {
				if(res_count < 4){
					delete[] reinterpret_cast<data_ptr_t*>(artifacts->addresses[i]);
				} else {
					Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(artifacts->addresses[i]);
					delete compressed_list;
				}
			}
		}

		for (size_t i = 0; i < size; i++) {
			idx_t count = artifacts->count[i];
			if(artifacts->sel[i] != 0){
				if(count <= 16){
					sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->sel[i]);
					delete[] sel_addr;
				} else {
					Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(artifacts->sel[i]);
					delete compressed_list;
				}
			}

		}

		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t addresses_p, idx_t is_ascend_p, idx_t sel_p, idx_t count_p, idx_t in_start_p){
		if (size == 0) {
			artifacts = new CompressedAddressSelArtifacts();
		}

		this->artifacts->addresses.PushBack(addresses_p, size);
		this->artifacts->is_ascend.PushBack(is_ascend_p, size);

		this->artifacts->sel.PushBack(sel_p, size);
		this->artifacts->count.PushBack(count_p, size);
		this->artifacts->in_start.PushBack(in_start_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
//			return sizeof(CompressedAddressSelArtifactList);
			return 0;
		} else {
			return this->artifacts->addresses.GetBytesSize()
			       + this->artifacts->is_ascend.GetBytesSize()
			       + this->artifacts->sel.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize();
//			       + sizeof(CompressedAddressSelArtifactList);
		}
	}

public:
	// Member variables
	CompressedAddressSelArtifacts* artifacts;

	size_t size;

};

struct CompressedJoinGatherArtifacts{
	Compressed64List rhs;
	Compressed64List use_rle;

	Compressed64List bitmap;
	Compressed64List bitmap_size;
	Compressed64List bitmap_is_compressed;

	Compressed64List start_bitmap_idx;
	Compressed64List count;
	Compressed64List in_start;
	Compressed64List use_bitmap;
};

class CompressedJoinGatherArtifactList{
public:
	// Constructor
	explicit CompressedJoinGatherArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedJoinGatherArtifactList() {
		if(artifacts != nullptr){
			for (size_t i = 0; i < size; i++) {
				data_ptr_t* compressed_rhs_addr = reinterpret_cast<data_ptr_t*>(artifacts->rhs[i]);
				idx_t use_rle = artifacts->use_rle[i];
				idx_t count = artifacts->count[i];

				if(use_rle){
					if(use_rle == 1){
						Compressed64ListWithSize** compressed_list = reinterpret_cast<Compressed64ListWithSize**>(compressed_rhs_addr);
						delete compressed_list[0];
						delete compressed_list[1];
						delete[] compressed_list;

					} else if (use_rle == 2){
						idx_t** rhs_addr = reinterpret_cast<idx_t**>(compressed_rhs_addr);
						delete[] rhs_addr[0];
						delete[] reinterpret_cast<uint16_t*>(rhs_addr[1]);
						delete[] rhs_addr;
					}

					continue;
				}

				if(count <= 8){
					delete[] compressed_rhs_addr;
					continue;
				}

				Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(compressed_rhs_addr);
				delete compressed_list;
			}

			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}

		}

		delete artifacts;

	}

	void Clear(){
		if(artifacts != nullptr){
			for (size_t i = 0; i < size; i++) {
				data_ptr_t* compressed_rhs_addr = reinterpret_cast<data_ptr_t*>(artifacts->rhs[i]);
				idx_t use_rle = artifacts->use_rle[i];
				idx_t count = artifacts->count[i];

				if(use_rle){
					if(use_rle == 1){
						Compressed64ListWithSize** compressed_list = reinterpret_cast<Compressed64ListWithSize**>(compressed_rhs_addr);
						delete compressed_list[0];
						delete compressed_list[1];
						delete[] compressed_list;

					} else if (use_rle == 2){
						idx_t** rhs_addr = reinterpret_cast<idx_t**>(compressed_rhs_addr);
						delete[] rhs_addr[0];
						delete[] reinterpret_cast<uint16_t*>(rhs_addr[1]);
						delete[] rhs_addr;
					}

					continue;
				}

				if(count <= 8){
					delete[] compressed_rhs_addr;
					continue;
				}

				Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(compressed_rhs_addr);
				delete compressed_list;
			}

			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}
		}

		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t rhs_p, idx_t use_rle_p, const vector<idx_t>& bitmap_p, const vector<idx_t>& bitmap_size_p, const vector<idx_t>& bitmap_is_compressed_p,
	              const idx_t bitmap_num_p, idx_t count_p, idx_t in_start_p, idx_t use_bitmap_p){

		if (size == 0) {
			artifacts = new CompressedJoinGatherArtifacts();
			artifacts->start_bitmap_idx.PushBack(0, size);
		}

		artifacts->rhs.PushBack(rhs_p, size);
		artifacts->use_rle.PushBack(use_rle_p, size);

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
//			return sizeof(CompressedJoinGatherArtifactList);
			return 0;
		} else {
			return this->artifacts->rhs.GetBytesSize()
			       + this->artifacts->use_rle.GetBytesSize()
			       + this->artifacts->bitmap.GetBytesSize()
			       + this->artifacts->bitmap_size.GetBytesSize()
			       + this->artifacts->bitmap_is_compressed.GetBytesSize()
			       + this->artifacts->start_bitmap_idx.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + this->artifacts->use_bitmap.GetBytesSize();
//			       + sizeof(CompressedJoinGatherArtifactList);
		}
	}

public:
	// Member variables
	CompressedJoinGatherArtifacts* artifacts;

	size_t size;

};

struct CompressedPerfectJoinArtifacts{
	// bitmap for left
	Compressed64List bitmap;
	Compressed64List bitmap_size;
	Compressed64List bitmap_is_compressed;
	Compressed64List start_bitmap_idx;

	// bitpack for right
	Compressed64List right;

	Compressed64List count;
	Compressed64List in_start;
	Compressed64List use_bitmap;
};

class CompressedPerfectJoinArtifactList{
public:
	// Constructor
	explicit CompressedPerfectJoinArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedPerfectJoinArtifactList() {

		if(artifacts != nullptr){
			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}

			for (size_t i = 0; i < size; i++) {
				if(artifacts->count[i] < 30){
					sel_t* right_addr = reinterpret_cast<sel_t*>(artifacts->right[i]);
					delete[] right_addr;
				} else {
					Compressed64ListWithSize* right_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->right[i]);
					delete right_addr;
				}
			}
		}

		delete artifacts;

	}

	void Clear(){
		if(artifacts != nullptr){
			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}

			for (size_t i = 0; i < size; i++) {
				if(artifacts->count[i] < 30){
					sel_t* right_addr = reinterpret_cast<sel_t*>(artifacts->right[i]);
					delete[] right_addr;
				} else {
					Compressed64ListWithSize* right_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->right[i]);
					delete right_addr;
				}
			}
		}

		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(const vector<idx_t>& bitmap_p, const vector<idx_t>& bitmap_size_p,
	              const vector<idx_t>& bitmap_is_compressed_p, const idx_t bitmap_num_p,
	              idx_t right_p, idx_t count_p, idx_t in_start_p, idx_t use_bitmap_p){

		if (size == 0) {
			artifacts = new CompressedPerfectJoinArtifacts();
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

		artifacts->right.PushBack(right_p, size);
		artifacts->count.PushBack(count_p, size);
		artifacts->in_start.PushBack(in_start_p, size);

		artifacts->use_bitmap.PushBack(use_bitmap_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
//			return sizeof(CompressedPerfectJoinArtifactList);
			return 0;
		} else {
			return this->artifacts->bitmap.GetBytesSize()
			       + this->artifacts->bitmap_size.GetBytesSize()
			       + this->artifacts->bitmap_is_compressed.GetBytesSize()
			       + this->artifacts->start_bitmap_idx.GetBytesSize()
			       + this->artifacts->right.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize()
			       + this->artifacts->use_bitmap.GetBytesSize();
//			       + sizeof(CompressedPerfectJoinArtifactList);
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

	Compressed64List compressed_row_locations;
	Compressed64List compressed_row_locations_size;
	Compressed64List is_ascend;

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

		if(artifacts != nullptr){
			for (size_t i = 0; i < size; i++) {
				if(artifacts->key_count[i] <= 16){
					sel_t* sel_build_addr = reinterpret_cast<sel_t*>(artifacts->sel_build[i]);
					delete[] sel_build_addr;
				} else {
					Compressed64ListWithSize* sel_build_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->sel_build[i]);
					delete sel_build_addr;
				}

				if(artifacts->key_count[i] <= 16){
					sel_t* sel_tuples_addr = reinterpret_cast<sel_t*>(artifacts->sel_tuples[i]);
					delete[] sel_tuples_addr;
				} else {
					Compressed64ListDelta** sel_tuples_addr = reinterpret_cast<Compressed64ListDelta**>(artifacts->sel_tuples[i]);
					delete[] sel_tuples_addr;
				}

				idx_t compressed_row_location_size = artifacts->compressed_row_locations_size[i];
				idx_t is_ascend_count = artifacts->is_ascend[i];

				if(artifacts->compressed_row_locations[i] != 0){
					if(compressed_row_location_size / (is_ascend_count+1) >= 16){
						if(compressed_row_location_size <= 8){
							delete[] reinterpret_cast<data_ptr_t*>(artifacts->compressed_row_locations[i]);
						} else {
							delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->compressed_row_locations[i]);
						}
					} else {
						if(compressed_row_location_size < 4){
							delete[] reinterpret_cast<data_ptr_t*>(artifacts->compressed_row_locations[i]);
						} else {
							Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(artifacts->compressed_row_locations[i]);
							delete compressed_list;
						}

					}
				}
			}
		}

		delete artifacts;

	}

	void Clear(){
		if(artifacts != nullptr){
			for (size_t i = 0; i < size; i++) {
				if(artifacts->key_count[i] <= 16){
					sel_t* sel_build_addr = reinterpret_cast<sel_t*>(artifacts->sel_build[i]);
					delete[] sel_build_addr;
				} else {
					Compressed64ListWithSize* sel_build_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->sel_build[i]);
					delete sel_build_addr;
				}

				if(artifacts->key_count[i] <= 16){
					sel_t* sel_tuples_addr = reinterpret_cast<sel_t*>(artifacts->sel_tuples[i]);
					delete[] sel_tuples_addr;
				} else {
					Compressed64ListDelta** sel_tuples_addr = reinterpret_cast<Compressed64ListDelta**>(artifacts->sel_tuples[i]);
					delete[] sel_tuples_addr;
				}

				idx_t compressed_row_location_size = artifacts->compressed_row_locations_size[i];
				idx_t is_ascend_count = artifacts->is_ascend[i];

				if(artifacts->compressed_row_locations[i] != 0){
					if(compressed_row_location_size / (is_ascend_count+1) >= 16){
						if(compressed_row_location_size <= 8){
							delete[] reinterpret_cast<data_ptr_t*>(artifacts->compressed_row_locations[i]);
						} else {
							delete[] reinterpret_cast<Compressed64ListDelta**>(artifacts->compressed_row_locations[i]);
						}
					} else {
						if(compressed_row_location_size < 4){
							delete[] reinterpret_cast<data_ptr_t*>(artifacts->compressed_row_locations[i]);
						} else {
							Compressed64ListWithSize* compressed_list = reinterpret_cast<Compressed64ListWithSize*>(artifacts->compressed_row_locations[i]);
							delete compressed_list;
						}
					}
				}
			}
		}

		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(idx_t sel_build_p, idx_t sel_tuples_p, idx_t compressed_row_locations_p,
	              idx_t compressed_row_locations_size_p, idx_t is_ascend_p,
	              idx_t key_count_p, idx_t ht_count_p, idx_t vector_buffer_size_p){
		if (size == 0) {
			artifacts = new CompressedPerfectFullScanHTArtifacts();
		}

		this->artifacts->sel_build.PushBack(sel_build_p, size);
		this->artifacts->sel_tuples.PushBack(sel_tuples_p, size);

		this->artifacts->compressed_row_locations.PushBack(compressed_row_locations_p, size);
		this->artifacts->compressed_row_locations_size.PushBack(compressed_row_locations_size_p, size);
		this->artifacts->is_ascend.PushBack(is_ascend_p, size);

		this->artifacts->key_count.PushBack(key_count_p, size);
		this->artifacts->ht_count.PushBack(ht_count_p, size);
		this->artifacts->vector_buffer_size.PushBack(vector_buffer_size_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		// do not consider vector buffer size, it is only used for statistics
		if(size == 0){
//			return sizeof(CompressedPerfectFullScanHTArtifactList);
			return 0;
		} else {
			return this->artifacts->sel_build.GetBytesSize()
			       + this->artifacts->sel_tuples.GetBytesSize()

			       + this->artifacts->compressed_row_locations.GetBytesSize()
			       + this->artifacts->compressed_row_locations_size.GetBytesSize()
			       + this->artifacts->is_ascend.GetBytesSize()

			       + this->artifacts->key_count.GetBytesSize()
			       + this->artifacts->ht_count.GetBytesSize()
			       + this->artifacts->vector_buffer_size.GetBytesSize();

//			       + sizeof(CompressedPerfectFullScanHTArtifactList);
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
//			return sizeof(CompressedLimitArtifactList);
			return 0;
		} else {
			return this->artifacts->start.GetBytesSize()
			       + this->artifacts->end.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize();
//			       + sizeof(CompressedLimitArtifactList);
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
//			return sizeof(CompressedReorderLogArtifactList);
			return 0;
		} else {
			return this->artifacts->index.GetBytesSize();
//			       + sizeof(CompressedReorderLogArtifactList);
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
			return 0;
//			return sizeof(CompressedCrossArtifactList);
		} else {
			return this->artifacts->branch_scan_lhs.GetBytesSize()
			       + this->artifacts->position_in_chunk.GetBytesSize()
			       + this->artifacts->scan_position.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->in_start.GetBytesSize();
//			       + sizeof(CompressedCrossArtifactList);
		}
	}

public:
	// Member variables
	CompressedCrossArtifacts* artifacts;

	size_t size;

};

struct CompressedNljArtifacts{
	// bitmap for left
	Compressed64List bitmap;
	Compressed64List bitmap_size;
	Compressed64List bitmap_is_compressed;
	Compressed64List start_bitmap_idx;

	Compressed64List right;

	Compressed64List count;
	Compressed64List current_row_index;
	Compressed64List out_start;

	Compressed64List use_bitmap;
};

class CompressedNLJArtifactList{
public:
	// Constructor
	explicit CompressedNLJArtifactList()
	    : artifacts(nullptr), size(0) {};

	// Destructor
	~CompressedNLJArtifactList() {

		if(artifacts != nullptr){
			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}

			for (size_t i = 0; i < size; i++) {
				sel_t* right_addr = reinterpret_cast<sel_t*>(artifacts->right[i]);
				delete[] right_addr;
			}
		}

		delete artifacts;
	}

	void Clear(){
		if(artifacts != nullptr){
			for(size_t i = 0; i < size; i++){
				idx_t start_bitmap_idx = artifacts->start_bitmap_idx[i];
				idx_t bitmap_num = artifacts->start_bitmap_idx[i + 1] - start_bitmap_idx;

				for(size_t j = 0; j < bitmap_num; j++){
					if(artifacts->count[i] < 16){
						sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
						delete[] sel_addr;
					} else {
						idx_t use_bitmap = artifacts->use_bitmap[i];
						idx_t bitmap_is_compressed = artifacts->bitmap_is_compressed[start_bitmap_idx + j];

						if(use_bitmap == 0 && bitmap_is_compressed == 1){
							if(artifacts->count[i] < 30){
								sel_t* sel_addr = reinterpret_cast<sel_t*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete[] sel_addr;
							} else {
								Compressed64ListWithSize* sel_addr = reinterpret_cast<Compressed64ListWithSize*>(artifacts->bitmap[start_bitmap_idx + j]);
								delete sel_addr;
							}
						} else {
							unsigned char* sel_addr = reinterpret_cast<unsigned char*>(artifacts->bitmap[start_bitmap_idx + j]);
							delete[] sel_addr;
						}
					}
				}
			}

			for (size_t i = 0; i < size; i++) {
				sel_t* right_addr = reinterpret_cast<sel_t*>(artifacts->right[i]);
				delete[] right_addr;
			}
		}

		delete artifacts;
		artifacts = nullptr;
		size = 0;
	}

	void PushBack(const vector<idx_t>& bitmap_p, const vector<idx_t>& bitmap_size_p,
	              const vector<idx_t>& bitmap_is_compressed_p, const idx_t bitmap_num_p,
	              idx_t right_p, idx_t count_p, idx_t current_row_index_p,
	              idx_t out_start_p, idx_t use_bitmap_p){

		if (size == 0) {
			artifacts = new CompressedNljArtifacts();
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

		artifacts->right.PushBack(right_p, size);
		artifacts->count.PushBack(count_p, size);
		artifacts->current_row_index.PushBack(current_row_index_p, size);
		artifacts->out_start.PushBack(out_start_p, size);

		artifacts->use_bitmap.PushBack(use_bitmap_p, size);

		size++;
	}

	idx_t GetBytesSize() {
		if(size == 0){
//			return sizeof(CompressedNLJArtifactList);
			return 0;
		} else {
			return this->artifacts->bitmap.GetBytesSize()
			       + this->artifacts->bitmap_size.GetBytesSize()
			       + this->artifacts->bitmap_is_compressed.GetBytesSize()
			       + this->artifacts->start_bitmap_idx.GetBytesSize()
			       + this->artifacts->right.GetBytesSize()
			       + this->artifacts->count.GetBytesSize()
			       + this->artifacts->current_row_index.GetBytesSize()
			       + this->artifacts->out_start.GetBytesSize()
			       + this->artifacts->use_bitmap.GetBytesSize();
//			       + sizeof(CompressedNLJArtifactList);
		}
	}

public:
	// Member variables
	CompressedNljArtifacts* artifacts;

	size_t size;

};


}


#endif
