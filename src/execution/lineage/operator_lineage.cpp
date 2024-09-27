#ifdef LINEAGE

#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

void OperatorLineage::PostProcess() {
	if (processed){
		return;
	}
	thread_vec.reserve(log.size());
	for (const auto& pair : log) {
		thread_vec.push_back(pair.first);
	}
	switch (type) {
	case PhysicalOperatorType::FILTER: {
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (lineage_manager->compress){
		  if (log.count(tkey) == 0 || log[tkey]->compressed_filter_log.size == 0){
			  continue;
		  }
		  // postprocess during decompression in GetLineageAsChunkLocal
	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->filter_log.empty()){
			  continue;
		  }
		  for (size_t k=0; k < log[tkey]->filter_log.size(); ++k) {
			  idx_t res_count = log[tkey]->filter_log[k].count;
			  idx_t offset = log[tkey]->filter_log[k].in_start;
			  if (log[tkey]->filter_log[k].sel) {
				  auto payload = log[tkey]->filter_log[k].sel.get();
				  for (idx_t j=0; j < res_count; ++j) {
					  payload[j] += offset;
				  }
			  }
		  }
	  }
    }
    break;
                                     }
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT: {
    break;
                                    }
	case PhysicalOperatorType::TABLE_SCAN: {
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (lineage_manager->compress){
		  if (log.count(tkey) == 0 || log[tkey]->compressed_row_group_log.size == 0) {
			  continue;
		  }
		  // postprocess during decompression in GetLineageAsChunkLocal
	  }else{
		  if (log.count(tkey) == 0 || log[tkey]->row_group_log.empty()){
			  continue;
		  }
		  for (size_t k=0; k < log[tkey]->row_group_log.size(); ++k) {
			  idx_t res_count = log[tkey]->row_group_log[k].count;
			  idx_t offset = log[tkey]->row_group_log[k].start + log[tkey]->row_group_log[k].vector_index;
			  if (log[tkey]->row_group_log[k].sel) {
				  auto payload = log[tkey]->row_group_log[k].sel.get();
				  for (idx_t j=0; j < res_count; ++j) {
					  payload[j] += offset;
				  }
			  }
		  }
	  }
    }
    break;
                                         }
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    // gather
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (lineage_manager->compress){
		  if (log.count(tkey) == 0 || log[tkey]->compressed_finalize_states_log.size == 0){
			  continue;
		  }
		  idx_t count_so_far = 0;
		  for (size_t k=0; k < log[tkey]->compressed_finalize_states_log.size; ++k) {
			  idx_t res_count = log[tkey]->compressed_finalize_states_log.artifacts->count[k];
			  // std::cout << count_so_far+res_count << " finalize states: " << tkey << " " << log[tkey]->finalize_states_log.size() << std::endl;

			  data_ptr_t* compressed_payload = reinterpret_cast<data_ptr_t*>(log[tkey]->compressed_finalize_states_log.artifacts->addresses[k]);
//			  idx_t is_ascend_count = log[tkey]->compressed_finalize_states_log.artifacts->is_ascend[k];
			  data_ptr_t* payload = ChangeDeltaRLEToAddress(compressed_payload, res_count);

			  for (idx_t j=0; j < res_count; ++j) {
				  if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
					  log_index->codes[payload[j]] = j + count_so_far;
					  // TODO: add tkey associasted with this code
					  //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
				  }
			  }
			  count_so_far += res_count;

			  if(res_count > 8){
				  delete[] payload;
			  }

		  }
//		  log[tkey]->compressed_finalize_states_log.Clear();

	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->finalize_states_log.empty()){
			  continue;
		  }
		  idx_t count_so_far = 0;
		  for (size_t k=0; k < log[tkey]->finalize_states_log.size(); ++k) {
			  idx_t res_count = log[tkey]->finalize_states_log[k].count;
			  // std::cout << count_so_far+res_count << " finalize states: " << tkey << " " << log[tkey]->finalize_states_log.size() << std::endl;
			  auto payload = log[tkey]->finalize_states_log[k].addresses.get();
			  for (idx_t j=0; j < res_count; ++j) {
				  if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
					  log_index->codes[payload[j]] = j + count_so_far;
					  // TODO: add tkey associasted with this code
					  //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
				  }
			  }
			  count_so_far += res_count;
		  }
//		  log[tkey]->finalize_states_log.clear();
	  }
      // free gather

    }      
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (lineage_manager->compress){
		  if (log.count(tkey) == 0 || log[tkey]->compressed_combine_log.size == 0){
			  continue;
		  }
		  for (size_t k=0; k < log[tkey]->compressed_combine_log.size; ++k) {
			  idx_t res_count = log[tkey]->compressed_combine_log.artifacts->count[k];

			  data_ptr_t* compressed_src = reinterpret_cast<data_ptr_t*>(log[tkey]->compressed_combine_log.artifacts->src[k]);
			  data_ptr_t* src = ChangeDeltaRLEToAddress(compressed_src, res_count);

			  data_ptr_t* compressed_target = reinterpret_cast<data_ptr_t*>(log[tkey]->compressed_combine_log.artifacts->target[k]);
			  data_ptr_t* target = ChangeDeltaRLEToAddress(compressed_target, res_count);

			  for (idx_t j=0; j < res_count; ++j) {
				  log_index->codes[src[j]] = log_index->codes[target[j]];
			  }

			  if(res_count > 8){
				  delete[] src;
				  delete[] target;
			  }

		  }

//		  log[tkey]->compressed_combine_log.Clear();

	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->combine_log.empty()){
			  continue;
		  }
		  //std::cout << "combine states: " << log[tkey]->combine_log.size() << std::endl;
		  for (size_t k=0; k < log[tkey]->combine_log.size(); ++k) {
			  idx_t res_count = log[tkey]->combine_log[k].count;
			  auto src = log[tkey]->combine_log[k].src.get();
			  auto target = log[tkey]->combine_log[k].target.get();
			  for (idx_t j=0; j < res_count; ++j) {
				  log_index->codes[src[j]] = log_index->codes[target[j]];
			  }
		  }
		  // free combine
//		  log[tkey]->combine_log.clear();
	  }
    }      

    // std::cout << " done " << std::endl;
    break;
  }
	case PhysicalOperatorType::ORDER_BY: {
    idx_t count_so_far = 0;
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if(lineage_manager->compress){
		  if (log.count(tkey) == 0 || log[tkey]->compressed_reorder_log.empty()){
			  continue;
		  }
		  for (size_t k = 0; k < log[tkey]->compressed_reorder_log.size(); k++) {
			  idx_t res_count = log[tkey]->compressed_reorder_log[k].size;
			  for (idx_t j=0; j < res_count; ++j) {
				  log_index->vals.push_back( log[tkey]->compressed_reorder_log[k].artifacts->index[j] + count_so_far );
			  }
			  count_so_far += res_count;
		  }
//		  log[tkey]->compressed_reorder_log.clear();

	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->reorder_log.empty()){
			  continue;
		  }
		  for (size_t k = 0; k < log[tkey]->reorder_log.size(); k++) {
			  // std::cout << k << "Order : " << log[tkey]->reorder_log.size() << " " << count_so_far << " " << log[tkey]->reorder_log[k].size() << std::endl;
			  idx_t res_count = log[tkey]->reorder_log[k].size();
			  for (idx_t j=0; j < res_count; ++j) {
				  log_index->vals.push_back( log[tkey]->reorder_log[k][j] + count_so_far );
			  }
			  count_so_far += res_count;
		  }
//		  log[tkey]->reorder_log.clear();
	  }
    }
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (lineage_manager->compress) {
		  if (log.count(tkey) == 0 || log[tkey]->compressed_scatter_sel_log.size == 0){
			  continue;
		  }
		  for (size_t k = 0; k < log[tkey]->compressed_scatter_sel_log.size; k++) {
			  idx_t res_count = log[tkey]->compressed_scatter_sel_log.artifacts->count[k];
			  idx_t in_start = log[tkey]->compressed_scatter_sel_log.artifacts->in_start[k];

			  data_ptr_t* compressed_payload = reinterpret_cast<data_ptr_t*>(log[tkey]->compressed_scatter_sel_log.artifacts->addresses[k]);
			  idx_t is_ascend_count = log[tkey]->compressed_scatter_sel_log.artifacts->is_ascend[k];

			  data_ptr_t* payload;
			  if(is_ascend_count <= 2){
				  payload = ChangeDeltaRLEToAddress(compressed_payload, res_count);
			  } else {
				  payload = ChangeBitpackToAddress(compressed_payload, res_count, is_ascend_count);
			  }

			  idx_t compressed_sel_num = log[tkey]->compressed_scatter_sel_log.artifacts->sel[k];

			  if (compressed_sel_num) {
				  sel_t* sel = ChangeDeltaBitpackToSelData(reinterpret_cast<sel_t*>(compressed_sel_num), res_count);
//				  auto sel = reinterpret_cast<sel_t*>(compressed_sel_num);

				  for (idx_t j=0; j < res_count; ++j) {
					  if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
						  log_index->codes[payload[j]] = sel[j] + in_start;
						  //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
					  }
				  }

				  if(res_count > 16){
					  delete[] sel;
				  }

			  } else {
				  for (idx_t j=0; j < res_count; ++j) {
					  if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
						  log_index->codes[payload[j]] = j + in_start;
						  //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
					  }
				  }
			  }

			  if((is_ascend_count <= 2 && res_count > 8) || (is_ascend_count > 2 && res_count >= 4)){
				  delete[] payload;
			  }

		  }

//		  log[tkey]->compressed_scatter_sel_log.Clear();

	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->scatter_sel_log.empty()){
			  continue;
		  }
		  for (size_t k = 0; k < log[tkey]->scatter_sel_log.size(); k++) {
			  idx_t res_count = log[tkey]->scatter_sel_log[k].count;
			  idx_t in_start = log[tkey]->scatter_sel_log[k].in_start;
			  auto payload = log[tkey]->scatter_sel_log[k].addresses.get();
			  //std::cout << k << " " << res_count << std::endl;
			  if (log[tkey]->scatter_sel_log[k].sel) {
				  auto sel = log[tkey]->scatter_sel_log[k].sel.get();
				  for (idx_t j=0; j < res_count; ++j) {
					  if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
						  log_index->codes[payload[j]] = sel[j] + in_start;
						  //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
					  }
				  }
			  } else {
				  for (idx_t j=0; j < res_count; ++j) {
					  if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
						  log_index->codes[payload[j]] = j + in_start;
						  //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
					  }
				  }
			  }
		  }
	  }
    }
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (lineage_manager->compress){
		  if (log.count(tkey) == 0 || log[tkey]->compressed_perfect_full_scan_ht_log.size == 0){
			  continue;
		  }
		  for (size_t k = 0; k < log[tkey]->compressed_perfect_full_scan_ht_log.size; k++) {
			  idx_t key_count = log[tkey]->compressed_perfect_full_scan_ht_log.artifacts->key_count[k];
			  idx_t ht_count = log[tkey]->compressed_perfect_full_scan_ht_log.artifacts->ht_count[k];
			  data_ptr_t* ptr = reinterpret_cast<data_ptr_t*>(
				  DecompressDataTArray(log[tkey]->compressed_perfect_full_scan_ht_log.artifacts->compressed_row_locations_size[k],
					                   log[tkey]->compressed_perfect_full_scan_ht_log.artifacts->row_locations_is_compressed[k],
					                   reinterpret_cast<data_ptr_t>(log[tkey]->compressed_perfect_full_scan_ht_log.artifacts->compressed_row_locations[k]),
					                   log[tkey]->compressed_perfect_full_scan_ht_log.artifacts->vector_buffer_size[k]));

			  sel_t* sel_build_decode = ChangeDeltaBitpackToSelData(reinterpret_cast<sel_t*>(log[tkey]->compressed_perfect_full_scan_ht_log.artifacts->sel_build[k]), key_count);
			  sel_t* sel_tuples_decode = ChangeDeltaRLEToSelData(reinterpret_cast<idx_t*>(log[tkey]->compressed_perfect_full_scan_ht_log.artifacts->sel_tuples[k]), key_count);

			  for (idx_t e=0; e < key_count; e++) {
				  idx_t build_idx = sel_build_decode[e];
				  idx_t tuples_idx = sel_tuples_decode[e];
				  // TODO: check if this is correct. follow old implementation
				  log_index->perfect_codes[build_idx] = log_index->codes[ptr[tuples_idx]];
			  }

			  if (key_count > 16) {
				  delete[] sel_build_decode;
			  }
			  if (key_count > 8) {
				  delete[] sel_tuples_decode;
			  }

		  }

//		  log[tkey]->compressed_perfect_full_scan_ht_log.Clear();

	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->perfect_full_scan_ht_log.empty()){
			  continue;
		  }
		  // std::cout << "Perfect Join: " << log[tkey]->perfect_full_scan_ht_log.size() << std::endl;
		  for (size_t k = 0; k < log[tkey]->perfect_full_scan_ht_log.size(); k++) {
			  idx_t key_count = log[tkey]->perfect_full_scan_ht_log[k].key_count;
			  idx_t ht_count = log[tkey]->perfect_full_scan_ht_log[k].ht_count;
			  //  std::cout << k << " " << key_count << " " << ht_count << std::endl;
			  for (idx_t e=0; e < key_count; e++) {
				  idx_t build_idx = log[tkey]->perfect_full_scan_ht_log[k].sel_build->owned_data.get()[e];
				  idx_t tuples_idx = log[tkey]->perfect_full_scan_ht_log[k].sel_tuples->owned_data.get()[e];
				  data_ptr_t* ptr = (data_ptr_t*)log[tkey]->perfect_full_scan_ht_log[k].row_locations->GetData();
				  // std::cout << "-> " << build_idx << " " << tuples_idx << " " << key_count << " " << ht_count  << std::endl;
				  // TODO: check if this is correct. follow old implementation
				  log_index->perfect_codes[build_idx] = log_index->codes[ptr[tuples_idx]];
				  // std::cout << "-> " << (void*)ptr[ tuples_idx ]  << std::endl;
			  }
		  }
	  }
    }
    for (size_t i=0; i < thread_vec.size(); i++) {
      int count_so_far = 0;
      void* tkey = thread_vec[i];
      if (lineage_manager->compress){
		  if (log.count(tkey) == 0 || log[tkey]->compressed_perfect_probe_ht_log.size == 0){
			  continue;
		  }
		  for (size_t k = 0; k < log[tkey]->execute_internal.size(); k++) {
			  int lsn = log[tkey]->execute_internal[k].first-1;

			  idx_t count = log[tkey]->compressed_perfect_probe_ht_log.artifacts->count[lsn];
			  idx_t in_start = log[tkey]->compressed_perfect_probe_ht_log.artifacts->in_start[lsn];
			  idx_t use_bitmap = log[tkey]->compressed_perfect_probe_ht_log.artifacts->use_bitmap[lsn];

			  sel_t* left = ChangeBitMapToSel(log[tkey]->compressed_perfect_probe_ht_log.artifacts, 0, lsn);

			  sel_t* compressed_right = reinterpret_cast<sel_t*>(log[tkey]->compressed_perfect_probe_ht_log.artifacts->right[lsn]);
			  sel_t* right = ChangeBitpackToSelData(compressed_right, count);

			  if (left == nullptr) {
				  for (idx_t j=0; j < count; ++j) {
					  log_index->vals.push_back( log_index->perfect_codes[ right[j] ]  );
					  log_index->vals_2.push_back( j + in_start );
				  }
			  } else {
				  for (idx_t j=0; j < count; ++j) {
					  log_index->vals.push_back( log_index->perfect_codes[ right[j] ]  );
					  log_index->vals_2.push_back( left[j] + in_start );
				  }
			  }
			  count_so_far += count;

			  if(use_bitmap){
				  delete[] left;
			  }
			  if(count >= 30){
				  delete[] right;
			  }

		  }
//		  log[tkey]->compressed_perfect_probe_ht_log.Clear();

	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->perfect_probe_ht_log.empty()){
			  continue;
		  }
		  //std::cout << "Perfect Probe Join: " << log[tkey]->perfect_probe_ht_log.size() << " " << log[tkey]->execute_internal.size() << std::endl;
		  for (size_t k = 0; k < log[tkey]->execute_internal.size(); k++) {
			  int lsn = log[tkey]->execute_internal[k].first-1;
			  //std::cout << "lsn: " << lsn << " " << k << std::endl;
			  idx_t count = log[tkey]->perfect_probe_ht_log[lsn].count;
			  idx_t in_start = log[tkey]->perfect_probe_ht_log[lsn].in_start;
			  // std::cout << operator_id << " " << k << " " << count << " " << in_start << " " << count_so_far <<  std::endl;
			  auto left = log[tkey]->perfect_probe_ht_log[lsn].left.get();
			  auto right = log[tkey]->perfect_probe_ht_log[lsn].right.get();
			  if (left == nullptr) {
				  for (idx_t j=0; j < count; ++j) {
					  log_index->vals.push_back( log_index->perfect_codes[ right[j] ]  );
					  log_index->vals_2.push_back( j + in_start );
				  }
			  } else {
				  for (idx_t j=0; j < count; ++j) {
					  log_index->vals.push_back( log_index->perfect_codes[ right[j] ]  );
					  log_index->vals_2.push_back( left[j] + in_start );
				  }
			  }
			  count_so_far += count;
		  }
//		  log[tkey]->perfect_probe_ht_log.clear();
	  }
      log[tkey]->execute_internal.clear();
    }

    int count_so_far = 0;
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (lineage_manager->compress) {
		  if (log.count(tkey) == 0 || log[tkey]->compressed_join_gather_log.size == 0){
			  continue;
		  }
		  for (size_t k = 0; k < log[tkey]->execute_internal.size(); k++) {
			  int lsn = log[tkey]->execute_internal[k].first-1;

			  idx_t res_count = log[tkey]->compressed_join_gather_log.artifacts->count[lsn];
			  idx_t in_start = log[tkey]->compressed_join_gather_log.artifacts->in_start[lsn];
			  idx_t use_rle = log[tkey]->compressed_join_gather_log.artifacts->use_rle[lsn];

			  data_ptr_t* payload = nullptr;
			  data_ptr_t* compressed_payload = reinterpret_cast<data_ptr_t*>(log[tkey]->compressed_join_gather_log.artifacts->rhs[lsn]);
			  if(compressed_payload != nullptr){
				  payload = ChangeRLEBitpackToAddress(compressed_payload, res_count, use_rle);
			  }

			  sel_t* lhs = ChangeBitMapToSel(log[tkey]->compressed_join_gather_log.artifacts, 0, lsn);

			  for (idx_t j=0; j < res_count; ++j) {
				  if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
					  // std::cout << "probe: " << j<< " " << lhs[j] << " " << res_count <<  " " << (void*)payload[j] << std::endl;
				  }
				  log_index->vals.push_back( log_index->codes[ payload[j] ] );
				  log_index->vals_2.push_back( lhs[j] + in_start );
			  }
			  count_so_far += res_count;

			  if(compressed_payload != nullptr && (use_rle || (use_rle == 0 && res_count > 8))){
				  delete[] payload;
			  }
		  }
//		  std::cout << "start Clear\n";
//		  log[tkey]->compressed_join_gather_log.Clear();
//		  std::cout << "end Clear\n";

	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->join_gather_log.empty()){
			  continue;
		  }
		  // std::cout << "Join: " << log[tkey]->join_gather_log.size() << std::endl;
		  for (size_t k = 0; k < log[tkey]->execute_internal.size(); k++) {
			  int lsn = log[tkey]->execute_internal[k].first-1;
			  idx_t res_count = log[tkey]->join_gather_log[lsn].count;
			  idx_t in_start = log[tkey]->join_gather_log[lsn].in_start;
			  auto payload = log[tkey]->join_gather_log[lsn].rhs.get();
			  auto lhs = log[tkey]->join_gather_log[lsn].lhs.get();
			  // std::cout << operator_id << " " <<  k << " " << res_count << " " << count_so_far << std::endl;
			  for (idx_t j=0; j < res_count; ++j) {
				  if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
					  // std::cout << "probe: " << j<< " " << lhs[j] << " " << res_count <<  " " << (void*)payload[j] << std::endl;
				  }
				  log_index->vals.push_back( log_index->codes[ payload[j] ] );
				  log_index->vals_2.push_back( lhs[j] + in_start );
			  }
			  count_so_far += res_count;
		  }
//		  log[tkey]->join_gather_log.clear();
	  }
    }
    // std::cout << operator_id << " log_index: " << log_index->vals.size() << std::endl;
    break;
  }
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
    for (size_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];

      if(lineage_manager->compress){
		  if (log.count(tkey) == 0 || log[tkey]->compressed_nlj_log.size == 0){
			  continue;
		  }
		  // postprocess during decompression in GetLineageAsChunkLocal
	  } else {
		  if (log.count(tkey) == 0 || log[tkey]->nlj_log.empty()){
			  continue;
		  }
		  for (size_t k=0; k < log[tkey]->nlj_log.size(); ++k) {
			  idx_t count = log[tkey]->nlj_log[k].count;
			  idx_t offset = log[tkey]->nlj_log[k].out_start;

			  if (log[tkey]->nlj_log[k].left != nullptr) {
				  auto vec_ptr = log[tkey]->nlj_log[k].left->owned_data.get();
				  for (idx_t j=0; j < count; j++) {
					  *(vec_ptr + j) += offset;
				  }
			  }

			  idx_t current_row_index = log[tkey]->nlj_log[k].current_row_index;
			  if (log[tkey]->nlj_log[k].right != nullptr && current_row_index != 0) {
				  auto vec_ptr = log[tkey]->nlj_log[k].right->owned_data.get();
				  for (idx_t j=0; j < count; j++) {
					  *(vec_ptr + j) += current_row_index;
				  }
			  }
		  }
	  }
    }
    break;
  }
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	processed = true;
}

//! Get the column types for this operator
//! Returns 1 vector of ColumnDefinitions for each table that must be created
vector<ColumnDefinition> OperatorLineage::GetTableColumnTypes() {
	vector<ColumnDefinition> source;
	switch (type) {
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN: {
    source.emplace_back("in_index", LogicalType::INTEGER);
    source.emplace_back("out_index", LogicalType::BIGINT);
    source.emplace_back("partition_index", LogicalType::INTEGER);
    break;
  }
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY: {
    source.emplace_back("in_index", LogicalType::BIGINT);
    source.emplace_back("out_index", LogicalType::BIGINT);
    source.emplace_back("partition_index", LogicalType::INTEGER);
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    source.emplace_back("in_index", LogicalType::BIGINT);
    source.emplace_back("out_index", LogicalType::INTEGER);
    source.emplace_back("sink_index", LogicalType::INTEGER);
    source.emplace_back("getdata_index", LogicalType::INTEGER);
		break;
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		source.emplace_back("lhs_index", LogicalType::INTEGER);
		source.emplace_back("rhs_index", LogicalType::INTEGER);
		source.emplace_back("out_index", LogicalType::BIGINT);
    source.emplace_back("sink_index", LogicalType::INTEGER);
    source.emplace_back("getdata_index", LogicalType::INTEGER);
		break;
	}
	case PhysicalOperatorType::HASH_JOIN:{
		source.emplace_back("lhs_index", LogicalType::BIGINT);
		source.emplace_back("rhs_index", LogicalType::BIGINT);
		source.emplace_back("out_index", LogicalType::BIGINT);
    source.emplace_back("sink_index", LogicalType::INTEGER);
    source.emplace_back("getdata_index", LogicalType::INTEGER);
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return source;
}

idx_t OperatorLineage::GetLineageAsChunk(DataChunk &insert_chunk,
                        idx_t& global_count, idx_t& local_count,
                        idx_t &thread_id, idx_t &data_idx,  bool &cache) {
	auto table_types = GetTableColumnTypes();
	vector<LogicalType> types;

	for (const auto& col_def : table_types) {
		types.push_back(col_def.GetType());
	}

	insert_chunk.InitializeEmpty(types);
	if (thread_vec.size() <= thread_id) {
		return 0;
	}

	void* thread_val  = thread_vec[thread_id];
	GetLineageAsChunkLocal(data_idx, global_count, local_count, insert_chunk, thread_id, log[thread_val]);

	global_count += insert_chunk.size();
	local_count += insert_chunk.size();
	data_idx++;

	if (insert_chunk.size() == 0) {
		thread_id++;
		cache = true;
    local_count = 0;
		data_idx = 0;
	}

	return insert_chunk.size();
}

idx_t OperatorLineage::GetLineageAsChunkLocal(idx_t data_idx, idx_t global_count, idx_t local_count,
    DataChunk& chunk, int thread_id, shared_ptr<Log> log) {
	if (log == nullptr) return 0;
  // std::cout << "get lineage as chunk : " << data_idx << " " << global_count << std::endl;

  Vector thread_id_vec(Value::INTEGER(thread_id));

	switch (type) {
	// schema: [INTEGER in_index, INTEGER out_index, INTEGER partition_index]
	case PhysicalOperatorType::FILTER: {
		idx_t count;
		idx_t offset;
		data_ptr_t ptr = nullptr;

		if(lineage_manager->compress){
			if (data_idx >= log->compressed_filter_log.size){
				return 0;
			}
			int lsn = log->execute_internal[data_idx].first-1;
			count = log->compressed_filter_log.artifacts->count[lsn];
			offset = log->compressed_filter_log.artifacts->in_start[lsn];

			sel_t* sel_copy = ChangeBitMapToSel(log->compressed_filter_log.artifacts, offset, lsn);
			ptr = reinterpret_cast<data_ptr_t>(sel_copy);

		} else {
			if (data_idx >= log->filter_log.size()){
				return 0;
			}
			int lsn = log->execute_internal[data_idx].first-1;
			count = log->filter_log[lsn].count;
			offset = log->filter_log[lsn].in_start;
			if (log->filter_log[lsn].sel) {
				ptr = (data_ptr_t)log->filter_log[lsn].sel.get();
			}
		}

		chunk.SetCardinality(count);
		if (ptr != nullptr) {
		  Vector in_index(LogicalType::INTEGER, ptr);
		  chunk.data[0].Reference(in_index);
		} else {
		  chunk.data[0].Sequence(offset, 1, count); // in_index
		}
		chunk.data[1].Sequence(global_count, 1, count); // out_index
		chunk.data[2].Reference(thread_id_vec);

		break;
	  }
	case PhysicalOperatorType::TABLE_SCAN: {
		idx_t count;
		idx_t offset;
		data_ptr_t ptr = nullptr;

		if (lineage_manager->compress){
			if (data_idx >= log->compressed_row_group_log.size){
				return 0;
			}
			count = log->compressed_row_group_log.artifacts->count[data_idx];
			offset = log->compressed_row_group_log.artifacts->start[data_idx]
					 + log->compressed_row_group_log.artifacts->vector_index[data_idx];

			sel_t* sel_copy = ChangeBitMapToSel(log->compressed_row_group_log.artifacts, offset, data_idx);
			ptr = reinterpret_cast<data_ptr_t>(sel_copy);

		} else {
			if (data_idx >= log->row_group_log.size()){
				return 0;
			}
			count = log->row_group_log[data_idx].count;
			offset = log->row_group_log[data_idx].start + log->row_group_log[data_idx].vector_index;
			ptr = nullptr;
			if (log->row_group_log[data_idx].sel) {
				// ptr = (data_ptr_t)log->row_group_log[data_idx].sel->owned_data.get();
				ptr = (data_ptr_t)log->row_group_log[data_idx].sel.get(); //owned_data.get();
			}
		}

    chunk.SetCardinality(count);
    if (ptr != nullptr) {
      Vector in_index(LogicalType::INTEGER, ptr);
      chunk.data[0].Reference(in_index);
    } else {
      chunk.data[0].Sequence(offset, 1, count); // in_index
    }
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    break;
  }
  case PhysicalOperatorType::STREAMING_LIMIT:
  case PhysicalOperatorType::LIMIT: {
	  idx_t start;
	  idx_t count;
	  idx_t offset;
    if (lineage_manager->compress){
		if (data_idx >= log->compressed_limit_offset.size){
			return 0;
		}

		start = log->compressed_limit_offset.artifacts->start[data_idx];
		count = log->compressed_limit_offset.artifacts->end[data_idx];
		offset = log->compressed_limit_offset.artifacts->in_start[data_idx];

	} else {

		if (data_idx >= log->limit_offset.size()){
			return 0;
		}

		start = log->limit_offset[data_idx].start;
		count = log->limit_offset[data_idx].end;
		offset = log->limit_offset[data_idx].in_start;
	}
    chunk.SetCardinality(count);
    chunk.data[0].Sequence(start+offset, 1, count); // in_index
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    break;
  }
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
	idx_t count;
	int lsn;

    if(lineage_manager->compress){
		if (data_idx >= log->compressed_nlj_log.size){
			return 0;
		}
		lsn = log->execute_internal[data_idx].first-1;
		count = log->compressed_nlj_log.artifacts->count[lsn];
	} else {
		if (data_idx >= log->nlj_log.size()){
			return 0;
		}
		lsn = log->execute_internal[data_idx].first-1;
		count = log->nlj_log[lsn].count;
	}

    chunk.SetCardinality(count);

    Vector lhs_payload(LogicalType::INTEGER, count);
    Vector rhs_payload(LogicalType::INTEGER, count);

	if(lineage_manager->compress){
		idx_t offset = log->compressed_nlj_log.artifacts->out_start[lsn];
		sel_t* left_addr_num = ChangeBitMapToSel(log->compressed_nlj_log.artifacts, offset, lsn);

		if (left_addr_num != nullptr) {
			data_ptr_t left_ptr = reinterpret_cast<data_ptr_t>(left_addr_num);
			Vector temp(LogicalType::INTEGER, left_ptr);
			lhs_payload.Reference(temp);
		} else {
			lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(lhs_payload, true);
		}

		idx_t compressed_right_addr_num = log->compressed_nlj_log.artifacts->right[lsn];

		if (compressed_right_addr_num) {
			idx_t match_count = log->compressed_nlj_log.artifacts->count[lsn];
			idx_t right_offset = log->compressed_nlj_log.artifacts->current_row_index[lsn];

			sel_t* right_addr_num = ChangeRLEToSelData(reinterpret_cast<idx_t*>(compressed_right_addr_num), match_count, right_offset);
			data_ptr_t right_ptr = reinterpret_cast<data_ptr_t>(right_addr_num);

			Vector temp(LogicalType::INTEGER, right_ptr);
			rhs_payload.Reference(temp);
		} else {
			rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(rhs_payload, true);
		}

	} else {
		if (log->nlj_log[lsn].left) {
			data_ptr_t left_ptr = (data_ptr_t)log->nlj_log[lsn].left->owned_data.get();
			Vector temp(LogicalType::INTEGER, left_ptr);
			lhs_payload.Reference(temp);
		} else {
			lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(lhs_payload, true);
		}

		if (log->nlj_log[lsn].right) {
			data_ptr_t right_ptr = (data_ptr_t)log->nlj_log[lsn].right->owned_data.get();
			Vector temp(LogicalType::INTEGER, right_ptr);
			rhs_payload.Reference(temp);
		} else {
			rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(rhs_payload, true);
		}
	}




    chunk.data[0].Reference(lhs_payload);
    chunk.data[1].Reference(rhs_payload);
    chunk.data[2].Sequence(global_count, 1, count); // out_index
    chunk.data[3].Reference(thread_id_vec); // sink
    chunk.data[4].Reference(thread_id_vec); // get data
    break;
  }
  case PhysicalOperatorType::CROSS_PRODUCT: {
	  idx_t branch_scan_lhs;
	  idx_t count;
	  idx_t offset;
	  idx_t position_in_chunk;
	  idx_t scan_position;

	if(lineage_manager->compress){
		if (data_idx >= log->compressed_cross_log.size){
			return 0;
		}
		int lsn = log->execute_internal[data_idx].first-1;

		branch_scan_lhs = log->compressed_cross_log.artifacts->branch_scan_lhs[lsn];
		count = log->compressed_cross_log.artifacts->count[lsn];
		offset = log->compressed_cross_log.artifacts->in_start[lsn];
		position_in_chunk = log->compressed_cross_log.artifacts->position_in_chunk[lsn];
		scan_position = log->compressed_cross_log.artifacts->scan_position[lsn];

	} else {
		if (data_idx >= log->cross_log.size()){
			return 0;
		}
		int lsn = log->execute_internal[data_idx].first-1;

		branch_scan_lhs = log->cross_log[lsn].branch_scan_lhs;
		count = log->cross_log[lsn].count;
		offset = log->cross_log[lsn].in_start;
		position_in_chunk = log->cross_log[lsn].position_in_chunk;
		scan_position = log->cross_log[lsn].scan_position;
	}

    chunk.SetCardinality(count);
    
    if (branch_scan_lhs == false) {
      Vector rhs_payload(Value::Value::INTEGER(scan_position + position_in_chunk));
      Vector lhs_payload(LogicalType::INTEGER, count);
      lhs_payload.Sequence(offset, 1, count);
      chunk.data[0].Reference(lhs_payload);
      chunk.data[1].Reference(rhs_payload);
    } else {
      Vector rhs_payload(LogicalType::INTEGER, count);
      Vector lhs_payload(Value::Value::INTEGER(position_in_chunk + offset));
      rhs_payload.Sequence(scan_position, 1, count);
      chunk.data[0].Reference(lhs_payload);
      chunk.data[1].Reference(rhs_payload);
    }
    chunk.data[2].Sequence(global_count, 1, count); // out_index
    chunk.data[3].Reference(thread_id_vec); // sink
    chunk.data[4].Reference(thread_id_vec); // get data
    break;
  }
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		idx_t count;
		data_ptr_t* payload;

		if(lineage_manager->compress){
			if (data_idx >= log->compressed_scatter_log.size){
				return 0;
			}
			count = log->compressed_scatter_log.artifacts->count[data_idx];
			data_ptr_t* compressed_payload = reinterpret_cast<data_ptr_t*>(log->compressed_scatter_log.artifacts->addresses[data_idx]);
			idx_t is_ascend_count = log->compressed_scatter_log.artifacts->is_ascend[data_idx];
			payload = ChangeBitpackToAddress(compressed_payload, count, is_ascend_count);

		} else {
			if (data_idx >= log->scatter_log.size()){
				return 0;
			}
			count = log->scatter_log[data_idx].count;
			payload = log->scatter_log[data_idx].addresses.get();
		}

		chunk.SetCardinality(count);
		chunk.data[1].Initialize(false, count);
		int* out_index_ptr = (int*)chunk.data[1].GetData();
		for (idx_t j=0; j < count; ++j) {
			out_index_ptr[j] = (int)log_index->codes[ payload[j] ];
		}
		chunk.data[0].Sequence(global_count, 1, count); // in_index
		chunk.data[2].Reference(thread_id_vec);
		chunk.data[3].Reference(thread_id_vec);

		if(lineage_manager->compress){
			if(count >= 4){
				delete[] payload;
			}
		}

		break;
		}
	case PhysicalOperatorType::ORDER_BY: {
    if (log_index->offset >= log_index->vals.size()) return 0;
    idx_t count = log_index->vals.size() - log_index->offset;
    if (count > STANDARD_VECTOR_SIZE) {
      count = STANDARD_VECTOR_SIZE;
    }
    data_ptr_t ptr = (data_ptr_t)(log_index->vals.data() + log_index->offset);
    chunk.SetCardinality(count);
    Vector in_index(LogicalType::BIGINT, ptr);
    chunk.data[0].Reference(in_index); // in_index
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    log_index->offset += count;
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
    if (log_index->offset >= log_index->vals.size()) return 0;
    idx_t count = log_index->vals.size() - log_index->offset;
    if (count > STANDARD_VECTOR_SIZE) {
      count = STANDARD_VECTOR_SIZE;
    }
    chunk.SetCardinality(count);
    data_ptr_t lhs_ptr = (data_ptr_t)(log_index->vals_2.data() + log_index->offset);
    Vector lhs_index(LogicalType::BIGINT, lhs_ptr);

    data_ptr_t rhs_ptr = (data_ptr_t)(log_index->vals.data() + log_index->offset);
    Vector rhs_index(LogicalType::BIGINT, rhs_ptr);
    chunk.data[0].Reference(lhs_index); // lhs_index
    chunk.data[1].Reference(rhs_index); // rhs_index
    chunk.data[2].Sequence(global_count, 1, count); // out_index
    chunk.data[3].Reference(thread_id_vec);
    chunk.data[4].Reference(thread_id_vec);
    log_index->offset += count;
		break;
	}
	default: {
		// Not Implemented
	}
	}

	return chunk.size();
}

} // namespace duckdb
#endif
