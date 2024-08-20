#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"

namespace duckdb {

unique_ptr<LineageManager> lineage_manager;
thread_local Log* active_log;
thread_local shared_ptr<OperatorLineage> active_lop;

shared_ptr<OperatorLineage> LineageManager::CreateOperatorLineage(ClientContext &context, PhysicalOperator *op) {
	global_logger[(void*)op] = make_shared_ptr<OperatorLineage>(operators_ids[(void*)op], op->type, op->GetName());
	op->lop = global_logger[(void*)op];
	InitLog(op->lop, (void*)&context);

	if (op->type == PhysicalOperatorType::RESULT_COLLECTOR) {
		PhysicalOperator* plan = &dynamic_cast<PhysicalResultCollector*>(op)->plan;
		shared_ptr<OperatorLineage> lop = CreateOperatorLineage(context, plan);
		global_logger[(void*)op]->children.push_back(lop);
	}

  if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || op->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
		auto distinct = (PhysicalOperator*)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get();
		shared_ptr<OperatorLineage> lop = CreateOperatorLineage(context, distinct);
		global_logger[(void*)op]->children.push_back(lop);
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i) {
			// dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i]->lineage_op = distinct->lineage_op;
		}
		lop = CreateOperatorLineage(context, dynamic_cast<PhysicalDelimJoin *>(op)->join.get());
		global_logger[(void*)op]->children.push_back(lop);
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		shared_ptr<OperatorLineage> lop = CreateOperatorLineage(context, op->children[i].get());
		global_logger[(void*)op]->children.push_back(lop);
	}

	return global_logger[(void*)op];
}

// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
int LineageManager::PlanAnnotator(PhysicalOperator *op, int counter) {
	

  if (op->type == PhysicalOperatorType::RESULT_COLLECTOR) {
		PhysicalOperator* plan = &dynamic_cast<PhysicalResultCollector*>(op)->plan;
    if (persist) std::cout << plan->ToString() << std::endl;
		counter = PlanAnnotator(plan, counter);
	}

  if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || op->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
		counter = PlanAnnotator( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), counter);
		counter = PlanAnnotator((PhysicalOperator*) dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), counter);
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		counter = PlanAnnotator(op->children[i].get(), counter);
	}

	operators_ids[(void*)op] = counter;
	return counter + 1;
}

void LineageManager::InitOperatorPlan(ClientContext &context, PhysicalOperator *op) {
	if (!capture) return;
	PlanAnnotator(op, 0);
	CreateOperatorLineage(context, op);
}

void LineageManager::CreateLineageTables(ClientContext &context, PhysicalOperator *op, idx_t query_id) {
  if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || op->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
		CreateLineageTables( context, dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), query_id);
		CreateLineageTables(context, (PhysicalOperator*) dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), query_id);
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(context, op->children[i].get(), query_id);
	}

	OperatorLineage *lop = lineage_manager->global_logger[(void *)op].get();
	if (lop == nullptr) return;
  lop->extra = op->ParamsToString();

  if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		string table_str = dynamic_cast<PhysicalTableScan *>(op)->ParamsToString();
		lop->table_name = table_str.substr(0, table_str.find('\n'));
	}

	vector<ColumnDefinition> table_column_types = lop->GetTableColumnTypes();
	if (table_column_types.empty()) return;

	// Example: LINEAGE_1_HASH_JOIN_3
	string prefix = "LINEAGE_" + to_string(query_id) + "_" + op->GetName() + "_" + to_string(lop->operator_id);
	prefix.erase( remove( prefix.begin(), prefix.end(), ' ' ), prefix.end() );
	// add column_stats, cardinality
	string catalog_name = TEMP_CATALOG;
	auto binder = Binder::CreateBinder(context);
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
  // Example: LINEAGE_1_HASH_JOIN_3
  string table_name = prefix;
  // Create Table
  auto create_info = make_uniq<CreateTableInfo>(catalog_name, DEFAULT_SCHEMA, table_name);
  create_info->temporary = true;
  create_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
  for (idx_t col_i = 0; col_i < table_column_types.size(); col_i++) {
    create_info->columns.AddColumn(move(table_column_types[col_i]));
  }
  table_lineage_op[table_name] = lineage_manager->global_logger[(void *)op];
  catalog.CreateTable(context, move(create_info));
}

void LineageManager::StoreQueryLineage(ClientContext &context, PhysicalOperator *op, string query) {
	if (!capture)
		return;

	idx_t query_id = query_to_id.size();
	query_to_id.push_back(query);
	queryid_to_plan[query_id] = lineage_manager->global_logger[(void *)op];
  if (persist) CreateLineageTables(context, op, query_id);
}

size_t LineageManager::GetUncompressedArtifactSize() {
	size_t total_size = 0;
	size_t total_buffer_size = 0;

	size_t filter_log_size = 0;
	size_t filter_log_buffer_size = 0;

	size_t limit_offset_size = 0;
	size_t limit_offset_buffer_size = 0;

	size_t perfect_full_scan_ht_log_size = 0;
	size_t perfect_full_scan_ht_log_buffer_size = 0;

	size_t perfect_probe_ht_log_size = 0;
	size_t perfect_probe_ht_log_buffer_size = 0;

	size_t row_group_log_size = 0;
	size_t row_group_log_buffer_size = 0;

	size_t scatter_log_size = 0;
	size_t scatter_log_buffer_size = 0;

	size_t scatter_sel_log_size = 0;
	size_t scatter_sel_log_buffer_size = 0;

	size_t gather_log_size = 0;
	size_t gather_log_buffer_size = 0;

	size_t combine_log_size = 0;
	size_t combine_log_buffer_size = 0;

	size_t finalize_states_log_size = 0;
	size_t finalize_states_log_buffer_size = 0;

	size_t join_gather_log_size = 0;
	size_t join_gather_log_buffer_size = 0;

	size_t reorder_log_size = 0;
	size_t reorder_log_buffer_size = 0;

	size_t cross_log_size = 0;
	size_t cross_log_buffer_size = 0;

	size_t nlj_log_size = 0;
	size_t nlj_log_buffer_size = 0;

	for (const auto& pair : lineage_manager->global_logger){
		OperatorLineage* lop = pair.second.get();
		for (const auto& pair_log : lop->log){
			Log* curr_log = pair_log.second.get();

			// filter log
			{
				size_t tmp_filter_log_size = 0;
				size_t tmp_filter_log_buffer_size = 0;
				size_t tmp_filter_log_element_size = 0;

				tmp_filter_log_element_size += sizeof(std::vector<filter_artifact>); // vector size
				tmp_filter_log_element_size += sizeof(filter_artifact) * curr_log->filter_log.capacity(); // size of elements in vector

				for(size_t i = 0; i < curr_log->filter_log.size(); i++){
					if(curr_log->filter_log[i].sel != nullptr){
						tmp_filter_log_buffer_size += sizeof(sel_t) * curr_log->filter_log[i].count; // size of the buffer hold by each element
					}
				}

				tmp_filter_log_size = tmp_filter_log_element_size + tmp_filter_log_buffer_size;

				filter_log_size += tmp_filter_log_size;
				filter_log_buffer_size += tmp_filter_log_buffer_size;

				total_size += tmp_filter_log_size;
				total_buffer_size += tmp_filter_log_buffer_size;
			}

			// limit offset log
			{
				size_t tmp_limit_offset_size = 0;
				size_t tmp_limit_offset_buffer_size = 0;
				size_t tmp_limit_offset_element_size = 0;

				tmp_limit_offset_element_size += sizeof(std::vector<limit_artifact>);
				tmp_limit_offset_element_size += sizeof(limit_artifact) * curr_log->limit_offset.capacity();

				tmp_limit_offset_buffer_size = 0;

				tmp_limit_offset_size = tmp_limit_offset_element_size + tmp_limit_offset_buffer_size;

				limit_offset_size += tmp_limit_offset_size;
				limit_offset_buffer_size += tmp_limit_offset_buffer_size;

				total_size += tmp_limit_offset_size;
			}

			// perfect full scan ht log
			{
				size_t tmp_perfect_full_scan_ht_log_size = 0;
				size_t tmp_perfect_full_scan_ht_log_buffer_size = 0;
				size_t tmp_perfect_full_scan_ht_log_element_size = 0;

				tmp_perfect_full_scan_ht_log_element_size += sizeof(std::vector<perfect_full_scan_ht_artifact>);
				tmp_perfect_full_scan_ht_log_element_size += sizeof(perfect_full_scan_ht_artifact) * curr_log->perfect_full_scan_ht_log.capacity();

				for(size_t i = 0; i < curr_log->perfect_full_scan_ht_log.size(); i++){
					// for sel_build and sel_tuples, even though we use buffer_ptr/shared_ptr and do not use std::move
					// we need to additionally calculate the size of the buffer (though the buffer is shared)
					// because it should have been freed before PostProcess() / GetLineageAsChunk() is called
					if(curr_log->perfect_full_scan_ht_log[i].sel_build != nullptr){
						tmp_perfect_full_scan_ht_log_buffer_size += sizeof(SelectionData);
						if(curr_log->perfect_full_scan_ht_log[i].sel_build->owned_data != nullptr){
							tmp_perfect_full_scan_ht_log_buffer_size += sizeof(sel_t) * curr_log->perfect_full_scan_ht_log[i].key_count; // sel_build.owned_data
						}
					}

					if(curr_log->perfect_full_scan_ht_log[i].sel_tuples != nullptr){
						tmp_perfect_full_scan_ht_log_buffer_size += sizeof(SelectionData);
						if(curr_log->perfect_full_scan_ht_log[i].sel_tuples->owned_data != nullptr){
							tmp_perfect_full_scan_ht_log_buffer_size += sizeof(sel_t) * curr_log->perfect_full_scan_ht_log[i].key_count; // sel_tuples.owned_data
						}
					}

					if(curr_log->perfect_full_scan_ht_log[i].row_locations != nullptr){
						tmp_perfect_full_scan_ht_log_buffer_size += sizeof(VectorBuffer);
						if(curr_log->perfect_full_scan_ht_log[i].row_locations->GetData() != nullptr){
							tmp_perfect_full_scan_ht_log_buffer_size += sizeof(data_t) * curr_log->perfect_full_scan_ht_log[i].row_locations->GetDataSize(); // row_locations.data
						}
						// it's pretty tricky to calculate the size of row_locations.aux_data, so we just ignore it
					}

				}
				tmp_perfect_full_scan_ht_log_size = tmp_perfect_full_scan_ht_log_element_size + tmp_perfect_full_scan_ht_log_buffer_size;

				perfect_full_scan_ht_log_size += tmp_perfect_full_scan_ht_log_size;
				perfect_full_scan_ht_log_buffer_size += tmp_perfect_full_scan_ht_log_buffer_size;

				total_size += tmp_perfect_full_scan_ht_log_size;
				total_buffer_size += tmp_perfect_full_scan_ht_log_buffer_size;
			}

			// perfect probe ht log
			{
				size_t tmp_perfect_probe_ht_log_size = 0;
				size_t tmp_perfect_probe_ht_log_buffer_size = 0;
				size_t tmp_perfect_probe_ht_log_element_size = 0;

				tmp_perfect_probe_ht_log_element_size += sizeof(std::vector<perfect_join_artifact>);
				tmp_perfect_probe_ht_log_element_size += sizeof(perfect_join_artifact) * curr_log->perfect_probe_ht_log.capacity();

				size_t tmp_left = 0;
				size_t right_tmp = 0;
				for(size_t i = 0; i < curr_log->perfect_probe_ht_log.size(); i++){
					if(curr_log->perfect_probe_ht_log[i].left != nullptr){
						tmp_perfect_probe_ht_log_buffer_size += sizeof(sel_t) * curr_log->perfect_probe_ht_log[i].count; // left
						tmp_left += sizeof(sel_t) * curr_log->perfect_probe_ht_log[i].count;
					}
					if(curr_log->perfect_probe_ht_log[i].right != nullptr){
						tmp_perfect_probe_ht_log_buffer_size += sizeof(sel_t) * curr_log->perfect_probe_ht_log[i].count; // right
						right_tmp += sizeof(sel_t) * curr_log->perfect_probe_ht_log[i].count;
					}
				}

				tmp_perfect_probe_ht_log_size = tmp_perfect_probe_ht_log_element_size + tmp_perfect_probe_ht_log_buffer_size;

				perfect_probe_ht_log_size += tmp_perfect_probe_ht_log_size;
				perfect_probe_ht_log_buffer_size += tmp_perfect_probe_ht_log_buffer_size;

				total_size += tmp_perfect_probe_ht_log_size;
				total_buffer_size += tmp_perfect_probe_ht_log_buffer_size;
			}

			// row group log
			{
				size_t tmp_row_group_log_size = 0;
				size_t tmp_row_group_log_buffer_size = 0;
				size_t tmp_row_group_log_element_size = 0;

				tmp_row_group_log_element_size += sizeof(std::vector<scan_artifact>);
				tmp_row_group_log_element_size += sizeof(scan_artifact) * curr_log->row_group_log.capacity();

				for(size_t i = 0; i < curr_log->row_group_log.size(); i++){
					if( curr_log->row_group_log[i].sel != nullptr) {
						tmp_row_group_log_buffer_size += sizeof(sel_t) * curr_log->row_group_log[i].count; // sel
					}
				}

				tmp_row_group_log_size = tmp_row_group_log_element_size + tmp_row_group_log_buffer_size;

				row_group_log_size += tmp_row_group_log_size;
				row_group_log_buffer_size += tmp_row_group_log_buffer_size;

				total_size += tmp_row_group_log_size;
				total_buffer_size += tmp_row_group_log_buffer_size;
			}

			// scatter log
			{
				size_t tmp_scatter_log_size = 0;
				size_t tmp_scatter_log_buffer_size = 0;
				size_t tmp_scatter_log_element_size = 0;

				tmp_scatter_log_element_size += sizeof(std::vector<address_artifact>);
				tmp_scatter_log_element_size += sizeof(address_artifact) * curr_log->scatter_log.capacity();

				for(size_t i = 0; i < curr_log->scatter_log.size(); i++){
					if(curr_log->scatter_log[i].addresses != nullptr){
						tmp_scatter_log_buffer_size += sizeof(data_ptr_t) * curr_log->scatter_log[i].count; // addresses
					}
				}

				tmp_scatter_log_size = tmp_scatter_log_element_size + tmp_scatter_log_buffer_size;

				scatter_log_size += tmp_scatter_log_size;
				scatter_log_buffer_size += tmp_scatter_log_buffer_size;

				total_size += tmp_scatter_log_size;
				total_buffer_size += tmp_scatter_log_buffer_size;
			}

			// scatter sel log
			{
				size_t tmp_scatter_sel_log_size = 0;
				size_t tmp_scatter_sel_log_buffer_size = 0;
				size_t tmp_scatter_sel_log_element_size = 0;

				tmp_scatter_sel_log_element_size += sizeof(std::vector<address_sel_artifact>);
				tmp_scatter_sel_log_element_size += sizeof(address_sel_artifact) * curr_log->scatter_sel_log.capacity();

				for(size_t i = 0; i < curr_log->scatter_sel_log.size(); i++){
					if(curr_log->scatter_sel_log[i].addresses != nullptr){
						tmp_scatter_sel_log_buffer_size += sizeof(data_ptr_t) * curr_log->scatter_sel_log[i].count; // addresses
					}
					if(curr_log->scatter_sel_log[i].sel != nullptr){
						tmp_scatter_sel_log_buffer_size += sizeof(sel_t) * curr_log->scatter_sel_log[i].count; // sel
					}
				}

				tmp_scatter_sel_log_size = tmp_scatter_sel_log_element_size + tmp_scatter_sel_log_buffer_size;

				scatter_sel_log_size += tmp_scatter_sel_log_size;
				scatter_sel_log_buffer_size += tmp_scatter_sel_log_buffer_size;

				total_size += tmp_scatter_sel_log_size;
				total_buffer_size += tmp_scatter_sel_log_buffer_size;
			}

			// gather log
			{
				size_t tmp_gather_log_size = 0;
				size_t tmp_gather_log_buffer_size = 0;
				size_t tmp_gather_log_element_size = 0;

				tmp_gather_log_element_size += sizeof(std::vector<address_artifact>);
				tmp_gather_log_element_size += sizeof(address_artifact) * curr_log->gather_log.capacity();

				for(size_t i = 0; i < curr_log->gather_log.size(); i++){
					if(curr_log->gather_log[i].addresses != nullptr){
						tmp_gather_log_buffer_size += sizeof(data_ptr_t) * curr_log->gather_log[i].count; // addresses
					}
				}

				tmp_gather_log_size = tmp_gather_log_element_size + tmp_gather_log_buffer_size;

				gather_log_size += tmp_gather_log_size;
				gather_log_buffer_size += tmp_gather_log_buffer_size;

				total_size += tmp_gather_log_size;
				total_buffer_size += tmp_gather_log_buffer_size;
			}

			// combine log
			{
				size_t tmp_combine_log_size = 0;
				size_t tmp_combine_log_buffer_size = 0;
				size_t tmp_combine_log_element_size = 0;

				tmp_combine_log_element_size += sizeof(std::vector<combine_artifact>);
				tmp_combine_log_element_size += sizeof(combine_artifact) * curr_log->combine_log.capacity();

				for(size_t i = 0; i < curr_log->combine_log.size(); i++){
					if(curr_log->combine_log[i].src != nullptr){
						tmp_combine_log_buffer_size += sizeof(data_ptr_t) * curr_log->combine_log[i].count; // src
					}
					if(curr_log->combine_log[i].target != nullptr){
						tmp_combine_log_buffer_size += sizeof(data_ptr_t) * curr_log->combine_log[i].count; // dst
					}
				}

				tmp_combine_log_size = tmp_combine_log_element_size + tmp_combine_log_buffer_size;

				combine_log_size += tmp_combine_log_size;
				combine_log_buffer_size += tmp_combine_log_buffer_size;

				total_size += tmp_combine_log_size;
				total_buffer_size += tmp_combine_log_buffer_size;
			}

			// finalize states log
			{
				size_t tmp_finalize_states_log_size = 0;
				size_t tmp_finalize_states_log_buffer_size = 0;
				size_t tmp_finalize_states_log_element_size = 0;

				tmp_finalize_states_log_element_size += sizeof(std::vector<address_artifact>);
				tmp_finalize_states_log_element_size += sizeof(address_artifact) * curr_log->finalize_states_log.capacity();

				for(size_t i = 0; i < curr_log->finalize_states_log.size(); i++){
					if(curr_log->finalize_states_log[i].addresses != nullptr){
						tmp_finalize_states_log_buffer_size += sizeof(data_ptr_t) * curr_log->finalize_states_log[i].count; // addresses
					}
				}

				tmp_finalize_states_log_size = tmp_finalize_states_log_element_size + tmp_finalize_states_log_buffer_size;

				finalize_states_log_size += tmp_finalize_states_log_size;
				finalize_states_log_buffer_size += tmp_finalize_states_log_buffer_size;

				total_size += tmp_finalize_states_log_size;
				total_buffer_size += tmp_finalize_states_log_buffer_size;
			}

			// join gather log
			{
				size_t tmp_join_gather_log_size = 0;
				size_t tmp_join_gather_log_buffer_size = 0;
				size_t tmp_join_gather_log_element_size = 0;

				tmp_join_gather_log_element_size += sizeof(std::vector<join_gather_artifact>);
				tmp_join_gather_log_element_size += sizeof(join_gather_artifact) * curr_log->join_gather_log.capacity();

				for(size_t i = 0; i < curr_log->join_gather_log.size(); i++){
					if(curr_log->join_gather_log[i].rhs != nullptr){
						tmp_join_gather_log_buffer_size += sizeof(data_ptr_t) * curr_log->join_gather_log[i].count; // rhs
					}
					if(curr_log->join_gather_log[i].lhs != nullptr){
						tmp_join_gather_log_buffer_size += sizeof(sel_t) * curr_log->join_gather_log[i].count; // lhs
					}
				}

				tmp_join_gather_log_size = tmp_join_gather_log_element_size + tmp_join_gather_log_buffer_size;

				join_gather_log_size += tmp_join_gather_log_size;
				join_gather_log_buffer_size += tmp_join_gather_log_buffer_size;

				total_size += tmp_join_gather_log_size;
				total_buffer_size += tmp_join_gather_log_buffer_size;
			}

			// reorder log
			{
				size_t tmp_reorder_log_size = 0;
				size_t tmp_reorder_log_buffer_size = 0;
				size_t tmp_reorder_log_element_size = 0;

				tmp_reorder_log_element_size += sizeof(std::vector<std::vector<idx_t>>);
				tmp_reorder_log_element_size += sizeof(std::vector<idx_t>) * curr_log->reorder_log.capacity();

				for(size_t i = 0; i < curr_log->reorder_log.size(); i++){
					tmp_reorder_log_element_size += sizeof(idx_t) * curr_log->reorder_log[i].capacity();
				}

				tmp_reorder_log_buffer_size = 0;

				tmp_reorder_log_size = tmp_reorder_log_element_size + tmp_reorder_log_buffer_size;

				reorder_log_size += tmp_reorder_log_size;
				reorder_log_buffer_size += tmp_reorder_log_buffer_size;

				total_size += tmp_reorder_log_size;
				total_buffer_size += tmp_reorder_log_buffer_size;
			}

			// cross log
			{
				size_t tmp_cross_log_size = 0;
				size_t tmp_cross_log_buffer_size = 0;
				size_t tmp_cross_log_element_size = 0;

				tmp_cross_log_element_size += sizeof(std::vector<cross_artifact>);
				tmp_cross_log_element_size += sizeof(cross_artifact) * curr_log->cross_log.capacity();

				tmp_cross_log_buffer_size = 0;

				tmp_cross_log_size = tmp_cross_log_element_size + tmp_cross_log_buffer_size;

				cross_log_size += tmp_cross_log_size;
				cross_log_buffer_size += tmp_cross_log_buffer_size;

				total_size += tmp_cross_log_size;
				total_buffer_size += tmp_cross_log_buffer_size;
			}

			// nlj log
			{
				size_t tmp_nlj_log_size = 0;
				size_t tmp_nlj_log_buffer_size = 0;
				size_t tmp_nlj_log_element_size = 0;

				tmp_nlj_log_element_size += sizeof(std::vector<nlj_artifact>);
				tmp_nlj_log_element_size += sizeof(nlj_artifact) * curr_log->nlj_log.capacity();

				for(size_t i = 0; i < curr_log->nlj_log.size(); i++){
					if(curr_log->nlj_log[i].left != nullptr){
						tmp_nlj_log_buffer_size += sizeof(SelectionData);
						if(curr_log->nlj_log[i].left->owned_data != nullptr){
							tmp_nlj_log_buffer_size += sizeof(sel_t) * curr_log->nlj_log[i].count; // left.owned_data
						}
					}
					if(curr_log->nlj_log[i].right != nullptr){
						tmp_nlj_log_buffer_size += sizeof(SelectionData);
						if(curr_log->nlj_log[i].right->owned_data != nullptr){
							tmp_nlj_log_buffer_size += sizeof(sel_t) * curr_log->nlj_log[i].count; // right.owned_data
						}
					}
				}

				tmp_nlj_log_size = tmp_nlj_log_element_size + tmp_nlj_log_buffer_size;

				nlj_log_size += tmp_nlj_log_size;
				nlj_log_buffer_size += tmp_nlj_log_buffer_size;

				total_size += tmp_nlj_log_size;
				total_buffer_size += tmp_nlj_log_buffer_size;
			}


		}
	}

	std::cout << "\nuncompressed filter_log_size: " << filter_log_size << std::endl;
	std::cout << "uncompressed filter_log_buffer_size: " << filter_log_buffer_size << std::endl;

	std::cout << "uncompressed limit_offset_size: " << limit_offset_size << std::endl;
	std::cout << "uncompressed limit_offset_buffer_size: " << limit_offset_buffer_size << std::endl;

	std::cout << "uncompressed perfect_full_scan_ht_log_size: " << perfect_full_scan_ht_log_size << std::endl;
	std::cout << "uncompressed perfect_full_scan_ht_log_buffer_size: " << perfect_full_scan_ht_log_buffer_size << std::endl;

	std::cout << "uncompressed perfect_probe_ht_log_size: " << perfect_probe_ht_log_size << std::endl;
	std::cout << "uncompressed perfect_probe_ht_log_buffer_size: " << perfect_probe_ht_log_buffer_size << std::endl;

	std::cout << "uncompressed row_group_log_size: " << row_group_log_size << std::endl;
	std::cout << "uncompressed row_group_log_buffer_size: " << row_group_log_buffer_size << std::endl;

	std::cout << "uncompressed scatter_log_size: " << scatter_log_size << std::endl;
	std::cout << "uncompressed scatter_log_buffer_size: " << scatter_log_buffer_size << std::endl;

	std::cout << "uncompressed scatter_sel_log_size: " << scatter_sel_log_size << std::endl;
	std::cout << "uncompressed scatter_sel_log_buffer_size: " << scatter_sel_log_buffer_size << std::endl;

	std::cout << "uncompressed gather_log_size: " << gather_log_size << std::endl;
	std::cout << "uncompressed gather_log_buffer_size: " << gather_log_buffer_size << std::endl;

	std::cout << "uncompressed combine_log_size: " << combine_log_size << std::endl;
	std::cout << "uncompressed combine_log_buffer_size: " << combine_log_buffer_size << std::endl;

	std::cout << "uncompressed finalize_states_log_size: " << finalize_states_log_size << std::endl;
	std::cout << "uncompressed finalize_states_log_buffer_size: " << finalize_states_log_buffer_size << std::endl;

	std::cout << "uncompressed join_gather_log_size: " << join_gather_log_size << std::endl;
	std::cout << "uncompressed join_gather_log_buffer_size: " << join_gather_log_buffer_size << std::endl;

	std::cout << "uncompressed reorder_log_size: " << reorder_log_size << std::endl;
	std::cout << "uncompressed reorder_log_buffer_size: " << reorder_log_buffer_size << std::endl;

	std::cout << "uncompressed cross_log_size: " << cross_log_size << std::endl;
	std::cout << "uncompressed cross_log_buffer_size: " << cross_log_buffer_size << std::endl;

	std::cout << "uncompressed nlj_log_size: " << nlj_log_size << std::endl;
	std::cout << "uncompressed nlj_log_buffer_size: " << nlj_log_buffer_size << std::endl;

	std::cout << "uncompressed total_size: " << total_size << std::endl;
	std::cout << "uncompressed total_buffer_size: " << total_buffer_size << std::endl;
	std::cout << "uncompressed total_element_size: " << total_size - total_buffer_size << std::endl;

	return total_size;

}

size_t LineageManager::GetCompressedArtifactSize() {
	size_t total_size = 0;
	size_t total_buffer_size = 0;

	size_t filter_log_size = 0;
	size_t filter_log_buffer_size = 0;

	size_t limit_offset_size = 0;
	size_t limit_offset_buffer_size = 0;

	size_t perfect_full_scan_ht_log_size = 0;
	size_t perfect_full_scan_ht_log_buffer_size = 0;

	size_t perfect_probe_ht_log_size = 0;
	size_t perfect_probe_ht_log_buffer_size = 0;

	size_t row_group_log_size = 0;
	size_t row_group_log_buffer_size = 0;

	size_t scatter_log_size = 0;
	size_t scatter_log_buffer_size = 0;

	size_t scatter_sel_log_size = 0;
	size_t scatter_sel_log_buffer_size = 0;

	size_t gather_log_size = 0;
	size_t gather_log_buffer_size = 0;

	size_t combine_log_size = 0;
	size_t combine_log_buffer_size = 0;

	size_t finalize_states_log_size = 0;
	size_t finalize_states_log_buffer_size = 0;

	size_t join_gather_log_size = 0;
	size_t join_gather_log_buffer_size = 0;

	size_t reorder_log_size = 0;
	size_t reorder_log_buffer_size = 0;

	size_t cross_log_size = 0;
	size_t cross_log_buffer_size = 0;

	size_t nlj_log_size = 0;
	size_t nlj_log_buffer_size = 0;

	for (const auto& pair : lineage_manager->global_logger){
		OperatorLineage* lop = pair.second.get();
		for (const auto& pair_log : lop->log){
			Log* curr_log = pair_log.second.get();

			// compressed filter log
			{
				size_t tmp_filter_log_size = 0;
				size_t tmp_filter_log_buffer_size = 0;
				size_t tmp_filter_log_element_size = 0;

				tmp_filter_log_element_size += curr_log->compressed_filter_log.GetBytesSize();

				if(curr_log->compressed_filter_log.size != 0){
					idx_t total_bitmap_num = curr_log->compressed_filter_log.artifacts->start_bitmap_idx[curr_log->compressed_filter_log.size];

					for(size_t i = 0; i < total_bitmap_num; i++){
						tmp_filter_log_buffer_size += curr_log->compressed_filter_log.artifacts->bitmap_size[i]; // sel_size
					}
				}

				tmp_filter_log_size = tmp_filter_log_element_size + tmp_filter_log_buffer_size;

				filter_log_size += tmp_filter_log_size;
				filter_log_buffer_size += tmp_filter_log_buffer_size;

				total_size += tmp_filter_log_size;
				total_buffer_size += tmp_filter_log_buffer_size;
			}

			// limit offset log
			{
				size_t tmp_limit_offset_size = 0;
				size_t tmp_limit_offset_buffer_size = 0;
				size_t tmp_limit_offset_element_size = 0;

				tmp_limit_offset_element_size += curr_log->compressed_limit_offset.GetBytesSize();
				tmp_limit_offset_buffer_size = 0;

				tmp_limit_offset_size = tmp_limit_offset_element_size + tmp_limit_offset_buffer_size;

				limit_offset_size += tmp_limit_offset_size;
				limit_offset_buffer_size += tmp_limit_offset_buffer_size;

				total_size += tmp_limit_offset_size;
				total_buffer_size += tmp_limit_offset_buffer_size;
			}

			// perfect full scan ht log
			{
				size_t tmp_perfect_full_scan_ht_log_size = 0;
				size_t tmp_perfect_full_scan_ht_log_buffer_size = 0;
				size_t tmp_perfect_full_scan_ht_log_element_size = 0;

				tmp_perfect_full_scan_ht_log_element_size += curr_log->compressed_perfect_full_scan_ht_log.GetBytesSize();

				for(size_t i = 0; i < curr_log->compressed_perfect_full_scan_ht_log.size; i++){
					if(curr_log->compressed_perfect_full_scan_ht_log.artifacts->sel_build[i] != 0){
						tmp_perfect_full_scan_ht_log_buffer_size += GetDeltaBitpackSize(reinterpret_cast<sel_t*>(curr_log->compressed_perfect_full_scan_ht_log.artifacts->sel_build[i]),
						                                                                curr_log->compressed_perfect_full_scan_ht_log.artifacts->key_count[i]); // sel_build
					}

					if(curr_log->compressed_perfect_full_scan_ht_log.artifacts->sel_tuples[i] != 0){
						tmp_perfect_full_scan_ht_log_buffer_size += GetDeltaRLESize(reinterpret_cast<idx_t*>(curr_log->compressed_perfect_full_scan_ht_log.artifacts->sel_tuples[i]),
						                                                            curr_log->compressed_perfect_full_scan_ht_log.artifacts->key_count[i]); // sel_tuples
					}

					if(curr_log->compressed_perfect_full_scan_ht_log.artifacts->compressed_row_locations[i] != 0){
						if(curr_log->compressed_perfect_full_scan_ht_log.artifacts->row_locations_is_compressed[i] == 0){
							tmp_perfect_full_scan_ht_log_buffer_size += sizeof(data_t) * curr_log->compressed_perfect_full_scan_ht_log.artifacts->vector_buffer_size[i]; // row_locations
						} else {
							tmp_perfect_full_scan_ht_log_buffer_size += sizeof(unsigned char) * curr_log->compressed_perfect_full_scan_ht_log.artifacts->compressed_row_locations_size[i]; // row_locations
						}
					}
				}

				tmp_perfect_full_scan_ht_log_size = tmp_perfect_full_scan_ht_log_element_size + tmp_perfect_full_scan_ht_log_buffer_size;

				perfect_full_scan_ht_log_size += tmp_perfect_full_scan_ht_log_size;
				perfect_full_scan_ht_log_buffer_size += tmp_perfect_full_scan_ht_log_buffer_size;

				total_size += tmp_perfect_full_scan_ht_log_size;
				total_buffer_size += tmp_perfect_full_scan_ht_log_buffer_size;
			}

			// perfect probe ht log
			{
				size_t tmp_perfect_probe_ht_log_size = 0;
				size_t tmp_perfect_probe_ht_log_buffer_size = 0;
				size_t tmp_perfect_probe_ht_log_element_size = 0;

				tmp_perfect_probe_ht_log_element_size += curr_log->compressed_perfect_probe_ht_log.GetBytesSize();

				size_t tmp = 0;
				// left bitmap
				if(curr_log->compressed_perfect_probe_ht_log.size != 0){
					idx_t total_bitmap_num = curr_log->compressed_perfect_probe_ht_log.artifacts->start_bitmap_idx[curr_log->compressed_perfect_probe_ht_log.size];

					for(size_t i = 0; i < total_bitmap_num; i++){
						tmp_perfect_probe_ht_log_buffer_size += curr_log->compressed_perfect_probe_ht_log.artifacts->bitmap_size[i]; // sel_size
						tmp += curr_log->compressed_perfect_probe_ht_log.artifacts->bitmap_size[i];
					}
				}

				tmp = 0;
				// right bitpack
				for(size_t i = 0; i < curr_log->compressed_perfect_probe_ht_log.size; i++){
					if(curr_log->compressed_perfect_probe_ht_log.artifacts->right[i] != 0){
						tmp_perfect_probe_ht_log_buffer_size += GetSelBitpackSize(reinterpret_cast<sel_t*>(curr_log->compressed_perfect_probe_ht_log.artifacts->right[i]),
						                                                                curr_log->compressed_perfect_probe_ht_log.artifacts->count[i]); // right
						tmp += GetSelBitpackSize(reinterpret_cast<sel_t*>(curr_log->compressed_perfect_probe_ht_log.artifacts->right[i]),
						                      curr_log->compressed_perfect_probe_ht_log.artifacts->count[i]);
					}
				}

				tmp_perfect_probe_ht_log_size = tmp_perfect_probe_ht_log_element_size + tmp_perfect_probe_ht_log_buffer_size;

				perfect_probe_ht_log_size += tmp_perfect_probe_ht_log_size;
				perfect_probe_ht_log_buffer_size += tmp_perfect_probe_ht_log_buffer_size;

				total_size += tmp_perfect_probe_ht_log_size;
				total_buffer_size += tmp_perfect_probe_ht_log_buffer_size;
			}

			// row group log
			{
				size_t tmp_row_group_log_size = 0;
				size_t tmp_row_group_log_buffer_size = 0;
				size_t tmp_row_group_log_element_size = 0;

				tmp_row_group_log_element_size += curr_log->compressed_row_group_log.GetBytesSize();

				if(curr_log->compressed_row_group_log.size != 0){
					idx_t total_bitmap_num = curr_log->compressed_row_group_log.artifacts->start_bitmap_idx[curr_log->compressed_row_group_log.size];

					for(size_t i = 0; i < total_bitmap_num; i++){
						tmp_row_group_log_buffer_size += curr_log->compressed_row_group_log.artifacts->bitmap_size[i]; // sel_size
					}
				}

				tmp_row_group_log_size = tmp_row_group_log_element_size + tmp_row_group_log_buffer_size;

				row_group_log_size += tmp_row_group_log_size;
				row_group_log_buffer_size += tmp_row_group_log_buffer_size;

				total_size += tmp_row_group_log_size;
				total_buffer_size += tmp_row_group_log_buffer_size;
			}

			// scatter log
			{
				size_t tmp_scatter_log_size = 0;
				size_t tmp_scatter_log_buffer_size = 0;
				size_t tmp_scatter_log_element_size = 0;

				tmp_scatter_log_element_size += curr_log->compressed_scatter_log.GetBytesSize();

				for(size_t i = 0; i < curr_log->compressed_scatter_log.size; i++){
					if(curr_log->compressed_scatter_log.artifacts->addresses[i] != 0){
						tmp_scatter_log_buffer_size += GetAddressBitpackSize(
						    reinterpret_cast<data_ptr_t*>(curr_log->compressed_scatter_log.artifacts->addresses[i]),
						    curr_log->compressed_scatter_log.artifacts->count[i],
						    curr_log->compressed_scatter_log.artifacts->is_ascend[i]);
					}
				}

				tmp_scatter_log_size = tmp_scatter_log_element_size + tmp_scatter_log_buffer_size;

				scatter_log_size += tmp_scatter_log_size;
				scatter_log_buffer_size += tmp_scatter_log_buffer_size;

				total_size += tmp_scatter_log_size;
				total_buffer_size += tmp_scatter_log_buffer_size;
			}

			// scatter sel log
			{
				size_t tmp_scatter_sel_log_size = 0;
				size_t tmp_scatter_sel_log_buffer_size = 0;
				size_t tmp_scatter_sel_log_element_size = 0;

				tmp_scatter_sel_log_element_size += curr_log->compressed_scatter_sel_log.GetBytesSize();

				for(size_t i = 0; i < curr_log->compressed_scatter_sel_log.size; i++){

					if(curr_log->compressed_scatter_sel_log.artifacts->addresses[i] != 0){
						tmp_scatter_sel_log_buffer_size += GetAddressBitpackSize(
						    reinterpret_cast<data_ptr_t*>(curr_log->compressed_scatter_sel_log.artifacts->addresses[i]),
						    curr_log->compressed_scatter_sel_log.artifacts->count[i],
						    curr_log->compressed_scatter_sel_log.artifacts->is_ascend[i]);
					}

					if(curr_log->compressed_scatter_sel_log.artifacts->sel[i] != 0){
						tmp_scatter_sel_log_buffer_size += GetDeltaBitpackSize(reinterpret_cast<sel_t*>(curr_log->compressed_scatter_sel_log.artifacts->sel[i]),
						                                                        curr_log->compressed_scatter_sel_log.artifacts->count[i]); // sel
					}

				}

				tmp_scatter_sel_log_size = tmp_scatter_sel_log_element_size + tmp_scatter_sel_log_buffer_size;

				scatter_sel_log_size += tmp_scatter_sel_log_size;
				scatter_sel_log_buffer_size += tmp_scatter_sel_log_buffer_size;

				total_size += tmp_scatter_sel_log_size;
				total_buffer_size += tmp_scatter_sel_log_buffer_size;
			}

			// gather log
			{
				size_t tmp_gather_log_size = 0;
				size_t tmp_gather_log_buffer_size = 0;
				size_t tmp_gather_log_element_size = 0;

				tmp_gather_log_element_size += curr_log->compressed_gather_log.GetBytesSize();

				for(size_t i = 0; i < curr_log->compressed_gather_log.size; i++){
					if(curr_log->compressed_gather_log.artifacts->addresses[i] != 0){
						tmp_gather_log_buffer_size += GetAddressBitpackSize(
						    reinterpret_cast<data_ptr_t*>(curr_log->compressed_gather_log.artifacts->addresses[i]),
						    curr_log->compressed_gather_log.artifacts->count[i],
						    curr_log->compressed_gather_log.artifacts->is_ascend[i]);
					}
				}

				tmp_gather_log_size = tmp_gather_log_element_size + tmp_gather_log_buffer_size;

				gather_log_size += tmp_gather_log_size;
				gather_log_buffer_size += tmp_gather_log_buffer_size;

				total_size += tmp_gather_log_size;
				total_buffer_size += tmp_gather_log_buffer_size;
			}

			// combine log
			{
				size_t tmp_combine_log_size = 0;
				size_t tmp_combine_log_buffer_size = 0;
				size_t tmp_combine_log_element_size = 0;

				tmp_combine_log_element_size += curr_log->compressed_combine_log.GetBytesSize();

				for(size_t i = 0; i < curr_log->compressed_combine_log.size; i++){
					if(curr_log->compressed_combine_log.artifacts->src[i] != 0){
						tmp_combine_log_buffer_size += GetAddressDeltaRLESize(
						    reinterpret_cast<data_ptr_t*>(curr_log->compressed_combine_log.artifacts->src[i]),
						                                   curr_log->compressed_combine_log.artifacts->count[i]);
					}
					if(curr_log->compressed_combine_log.artifacts->target[i] != 0){
						tmp_combine_log_buffer_size += GetAddressDeltaRLESize(
						    reinterpret_cast<data_ptr_t*>(curr_log->compressed_combine_log.artifacts->target[i]),
						                                   curr_log->compressed_combine_log.artifacts->count[i]);
					}
				}

				tmp_combine_log_size = tmp_combine_log_element_size + tmp_combine_log_buffer_size;

				combine_log_size += tmp_combine_log_size;
				combine_log_buffer_size += tmp_combine_log_buffer_size;

				total_size += tmp_combine_log_size;
				total_buffer_size += tmp_combine_log_buffer_size;
			}

			// finalize states log
			{
				size_t tmp_finalize_states_log_size = 0;
				size_t tmp_finalize_states_log_buffer_size = 0;
				size_t tmp_finalize_states_log_element_size = 0;

				tmp_finalize_states_log_element_size += curr_log->compressed_finalize_states_log.GetBytesSize();

				for(size_t i = 0; i < curr_log->compressed_finalize_states_log.size; i++){
					if(curr_log->compressed_finalize_states_log.artifacts->addresses[i] != 0){
						tmp_finalize_states_log_buffer_size += GetAddressBitpackSize(
						    reinterpret_cast<data_ptr_t*>(curr_log->compressed_finalize_states_log.artifacts->addresses[i]),
						    curr_log->compressed_finalize_states_log.artifacts->count[i],
						    curr_log->compressed_finalize_states_log.artifacts->is_ascend[i]);
					}
				}

				tmp_finalize_states_log_size = tmp_finalize_states_log_element_size + tmp_finalize_states_log_buffer_size;

				finalize_states_log_size += tmp_finalize_states_log_size;
				finalize_states_log_buffer_size += tmp_finalize_states_log_buffer_size;

				total_size += tmp_finalize_states_log_size;
				total_buffer_size += tmp_finalize_states_log_buffer_size;
			}

			// join gather log
			{
				size_t tmp_join_gather_log_size = 0;
				size_t tmp_join_gather_log_buffer_size = 0;
				size_t tmp_join_gather_log_element_size = 0;

				tmp_join_gather_log_element_size += curr_log->compressed_join_gather_log.GetBytesSize();

				for(size_t i = 0; i < curr_log->compressed_join_gather_log.size; i++){
					if(curr_log->compressed_join_gather_log.artifacts->rhs[i] != 0){
						tmp_join_gather_log_buffer_size += GetAddressRLEBitpackSize(reinterpret_cast<data_ptr_t*>(curr_log->compressed_join_gather_log.artifacts->rhs[i]),
						                                                           curr_log->compressed_join_gather_log.artifacts->count[i],
						                                                            curr_log->compressed_join_gather_log.artifacts->use_rle[i]); // rhs
					}
				}

				if(curr_log->compressed_join_gather_log.size != 0){
					idx_t total_bitmap_num = curr_log->compressed_join_gather_log.artifacts->start_bitmap_idx[curr_log->compressed_join_gather_log.size];

					for(size_t i = 0; i < total_bitmap_num; i++){
						tmp_join_gather_log_buffer_size += curr_log->compressed_join_gather_log.artifacts->bitmap_size[i]; // lhs
					}
				}

				tmp_join_gather_log_size = tmp_join_gather_log_element_size + tmp_join_gather_log_buffer_size;

				join_gather_log_size += tmp_join_gather_log_size;
				join_gather_log_buffer_size += tmp_join_gather_log_buffer_size;

				total_size += tmp_join_gather_log_size;
				total_buffer_size += tmp_join_gather_log_buffer_size;
			}

			// reorder log
			{
				size_t tmp_reorder_log_size = 0;
				size_t tmp_reorder_log_buffer_size = 0;
				size_t tmp_reorder_log_element_size = 0;

				tmp_reorder_log_element_size += sizeof(vector<CompressedReorderLogArtifactList>);
				tmp_reorder_log_element_size += curr_log->compressed_reorder_log.capacity() * sizeof(CompressedReorderLogArtifactList);

				for(size_t i = 0; i < curr_log->compressed_reorder_log.size(); i++){
					tmp_reorder_log_element_size += curr_log->compressed_reorder_log[i].GetBytesSize();
				}

				tmp_reorder_log_buffer_size = 0;

				tmp_reorder_log_size = tmp_reorder_log_element_size + tmp_reorder_log_buffer_size;

				reorder_log_size += tmp_reorder_log_size;
				reorder_log_buffer_size += tmp_reorder_log_buffer_size;

				total_size += tmp_reorder_log_size;
				total_buffer_size += tmp_reorder_log_buffer_size;
			}

			// cross log
			{
				size_t tmp_cross_log_size = 0;
				size_t tmp_cross_log_buffer_size = 0;
				size_t tmp_cross_log_element_size = 0;

				tmp_cross_log_element_size += curr_log->compressed_cross_log.GetBytesSize();

				tmp_cross_log_buffer_size = 0;

				tmp_cross_log_size = tmp_cross_log_element_size + tmp_cross_log_buffer_size;

				cross_log_size += tmp_cross_log_size;
				cross_log_buffer_size += tmp_cross_log_buffer_size;

				total_size += tmp_cross_log_size;
				total_buffer_size += tmp_cross_log_buffer_size;
			}

			// nlj log
			{
				size_t tmp_nlj_log_size = 0;
				size_t tmp_nlj_log_buffer_size = 0;
				size_t tmp_nlj_log_element_size = 0;

				tmp_nlj_log_element_size += curr_log->compressed_nlj_log.GetBytesSize();

				for(size_t i = 0; i < curr_log->compressed_nlj_log.size; i++){
					if(curr_log->compressed_nlj_log.artifacts->left[i] != 0){
						tmp_nlj_log_buffer_size += sizeof(sel_t) * curr_log->compressed_nlj_log.artifacts->count[i];
					}
					if(curr_log->compressed_nlj_log.artifacts->right[i] != 0){
						tmp_nlj_log_buffer_size += sizeof(sel_t) * curr_log->compressed_nlj_log.artifacts->count[i];
					}
				}

				tmp_nlj_log_size = tmp_nlj_log_element_size + tmp_nlj_log_buffer_size;

				nlj_log_size += tmp_nlj_log_size;
				nlj_log_buffer_size += tmp_nlj_log_buffer_size;

				total_size += tmp_nlj_log_size;
				total_buffer_size += tmp_nlj_log_buffer_size;
			}
		}
	}

	std::cout << "\ncompressed filter_log_size: " << filter_log_size << std::endl;
	std::cout << "compressed filter_log_buffer_size: " << filter_log_buffer_size << std::endl;

	std::cout << "compressed limit_offset_size: " << limit_offset_size << std::endl;
	std::cout << "compressed limit_offset_buffer_size: " << limit_offset_buffer_size << std::endl;

	std::cout << "compressed perfect_full_scan_ht_log_size: " << perfect_full_scan_ht_log_size << std::endl;
	std::cout << "compressed perfect_full_scan_ht_log_buffer_size: " << perfect_full_scan_ht_log_buffer_size << std::endl;

	std::cout << "compressed perfect_probe_ht_log_size: " << perfect_probe_ht_log_size << std::endl;
	std::cout << "compressed perfect_probe_ht_log_buffer_size: " << perfect_probe_ht_log_buffer_size << std::endl;

	std::cout << "compressed row_group_log_size: " << row_group_log_size << std::endl;
	std::cout << "compressed row_group_log_buffer_size: " << row_group_log_buffer_size << std::endl;

	std::cout << "compressed scatter_log_size: " << scatter_log_size << std::endl;
	std::cout << "compressed scatter_log_buffer_size: " << scatter_log_buffer_size << std::endl;

	std::cout << "compressed scatter_sel_log_size: " << scatter_sel_log_size << std::endl;
	std::cout << "compressed scatter_sel_log_buffer_size: " << scatter_sel_log_buffer_size << std::endl;

	std::cout << "compressed gather_log_size: " << gather_log_size << std::endl;
	std::cout << "compressed gather_log_buffer_size: " << gather_log_buffer_size << std::endl;

	std::cout << "compressed combine_log_size: " << combine_log_size << std::endl;
	std::cout << "compressed combine_log_buffer_size: " << combine_log_buffer_size << std::endl;

	std::cout << "compressed finalize_states_log_size: " << finalize_states_log_size << std::endl;
	std::cout << "compressed finalize_states_log_buffer_size: " << finalize_states_log_buffer_size << std::endl;

	std::cout << "compressed join_gather_log_size: " << join_gather_log_size << std::endl;
	std::cout << "compressed join_gather_log_buffer_size: " << join_gather_log_buffer_size << std::endl;

	std::cout << "compressed reorder_log_size: " << reorder_log_size << std::endl;
	std::cout << "compressed reorder_log_buffer_size: " << reorder_log_buffer_size << std::endl;

	std::cout << "compressed cross_log_size: " << cross_log_size << std::endl;
	std::cout << "compressed cross_log_buffer_size: " << cross_log_buffer_size << std::endl;

	std::cout << "compressed nlj_log_size: " << nlj_log_size << std::endl;
	std::cout << "compressed nlj_log_buffer_size: " << nlj_log_buffer_size << std::endl;

	std::cout << "compressed total_size: " << total_size << std::endl;
	std::cout << "compressed total_buffer_size: " << total_buffer_size << std::endl;
	std::cout << "compressed total_element_size: " << total_size - total_buffer_size << std::endl;

	return total_size;
}

} // namespace duckdb
#endif
