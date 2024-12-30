//
// Created by hxy on 12/26/24.
//
#ifdef LINEAGE
#pragma once

#include "operator_lineage.hpp"

namespace duckdb {

class OperatorLineage;
class RecyclerNode;
class RecyclerGraph;

class RecyclerNode{
public:
	explicit RecyclerNode(PhysicalOperatorType type, string name, string table_name, string extra)
	    : type(type), name(name), table_name(table_name), extra(extra), lop_is_stored(false) {
	}

	~RecyclerNode(){}

	void AddChild(const shared_ptr<RecyclerNode>& child) {
		children.push_back(child);
	}

	void AddParent(const shared_ptr<RecyclerNode>& parent) {
		parents.push_back(parent);
	}

	void SetRecyclerLop(const shared_ptr<OperatorLineage>& lop){
		lop_is_stored = true;
		this->recycler_lop = lop;
	}

	void ClearLog(){
		lop_is_stored = false;
		recycler_lop = nullptr;
	}

	PhysicalOperatorType GetType(){ return type;}
	string GetName(){ return name;}
	string GetTableName(){ return table_name;}
	string GetExtra(){ return extra;}
	std::vector<shared_ptr<RecyclerNode>> GetChildren(){ return children;}
	std::vector<shared_ptr<RecyclerNode>> GetParents(){ return parents;}

	std::vector<shared_ptr<RecyclerNode>> GetLeafNodes(){ return leaf_nodes;}
	void AddLeafNode(const shared_ptr<RecyclerNode>& leaf_node){ leaf_nodes.push_back(leaf_node);}
	void UpdateChildLeafNode(const shared_ptr<RecyclerNode>& child_node){
		for (auto &leaf_node : child_node->GetLeafNodes()){
			leaf_nodes.push_back(leaf_node);
		}
	}

	shared_ptr<OperatorLineage> GetRecyclerLop(){
		if (lop_is_stored){
			return recycler_lop;
		}
		return nullptr;
	}


private:
	PhysicalOperatorType type;
	string name;
	string table_name;
	string extra;

	std::vector<shared_ptr<RecyclerNode>> children;
	std::vector<shared_ptr<RecyclerNode>> parents;

	bool lop_is_stored;
	shared_ptr<OperatorLineage> recycler_lop;

	std::vector<shared_ptr<RecyclerNode>> leaf_nodes;
};


class RecyclerGraph{
public:
	explicit RecyclerGraph() {
		root = make_shared_ptr<RecyclerNode>(PhysicalOperatorType::RECYCLER_ROOT, "Root", "", "");
	}

	void AddQuery(const shared_ptr<OperatorLineage>&);
	bool MatchTree(const shared_ptr<OperatorLineage>&);

private:
	shared_ptr<RecyclerNode> root;
};

shared_ptr<RecyclerNode> ConvertToRecyclerNode(const shared_ptr<OperatorLineage>&);

}
#endif
