#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBQueriesListData : public GlobalTableFunctionState {
	DuckDBQueriesListData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBQueriesListBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("size_bytes_max");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("size_bytes_min");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("nchunks");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("postprocess_time");
	return_types.emplace_back(LogicalType::FLOAT);

  names.emplace_back("plan");
	return_types.emplace_back(LogicalType::VARCHAR);


	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBQueriesListInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueriesListData>();
	return std::move(result);
}

static string JSONSanitize(const string &text) {
	string result;
	result.reserve(text.size());
	for (idx_t i = 0; i < text.size(); i++) {
		switch (text[i]) {
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		default:
			result += text[i];
			break;
		}
	}
	return result;
}

string PlanToString(shared_ptr<OperatorLineage> lop) {
  if (!lop) return "";
	string child_str;
	for (idx_t i = 0; i < lop->children.size(); i++) {
		child_str += PlanToString(lop->children[i]);
		if (i != lop->children.size() - 1) {
			child_str += ",";
		}
	}
  // std::cout << " ################## " << std::endl;
  // std::cout << lop->name << " " << lop->extra << std::endl;
  // std::cout << " ----------------- " << std::endl;
	return "{\"name\": \"" + lop->name + "\", \"opid\": \"" + std::to_string(lop->operator_id) + "\", \"children\": [" + child_str + "],\"table\": \"" + lop->table_name +  "\",\"extra\": \"" + JSONSanitize(lop->extra)+ "\"}";
}

//! Create table to store executed queries with their IDs
//! Table name: queries_list
//! Schema: (INT query_id, varchar query)
void DuckDBQueriesListFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueriesListData>();
  if (!lineage_manager) return; 
	auto query_to_id = lineage_manager->query_to_id;
	if (data.offset >= query_to_id.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	idx_t total_bytes = 0;

	std::vector<idx_t> stats(3, 0);

	if(lineage_manager->compress) {
		idx_t compressed_total_size = lineage_manager->GetCompressedArtifactSize();
		stats[0] = compressed_total_size;
	} else {
		idx_t uncompressed_total_size = lineage_manager->GetUncompressedArtifactSize();
		stats[0] = uncompressed_total_size;
	}

	while (data.offset < query_to_id.size() && count < STANDARD_VECTOR_SIZE) {
//		std::cout << "Query ID: " << data.offset << " Query To ID Size: " << query_to_id.size() << std::endl;
		string query = query_to_id[data.offset];
		idx_t col = 0;
		// query_id, INT
		output.SetValue(col++, count,Value::INTEGER(data.offset));
		// query, VARCHAR
		output.SetValue(col++, count, query);

    // size_bytes_max
		output.SetValue(col++, count,Value::INTEGER(stats[0]));

    // size_bytes_min
		output.SetValue(col++, count,Value::INTEGER(stats[2]));

    // nchunks
		output.SetValue(col++, count,Value::INTEGER(stats[1]));

    // postprocess_time
    	float postprocess_time = 0.0;//((float) end - start) / CLOCKS_PER_SEC;
		output.SetValue(col++, count,Value::FLOAT(postprocess_time));

    // plan, VARCHAR
		output.SetValue(col++, count, PlanToString(lineage_manager->queryid_to_plan[data.offset]));

		count++;
		data.offset++;
	}
	output.SetCardinality(count);
}

void DuckDBQueriesListFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_queries_list", {}, DuckDBQueriesListFunction, DuckDBQueriesListBind, DuckDBQueriesListInit));
}

} // namespace duckdb
#endif