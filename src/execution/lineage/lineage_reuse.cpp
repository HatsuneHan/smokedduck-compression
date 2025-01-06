//
// Created by hxy on 12/26/24.
//
#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_reuse.hpp"

namespace duckdb {

shared_ptr<RecyclerNode> ConvertToRecyclerNode(const shared_ptr<OperatorLineage>& plan){
	if (!plan || plan->type == PhysicalOperatorType::PRAGMA) {
		return nullptr;
	}

	if(plan->mapping_recycler_node){
		return plan->mapping_recycler_node;
	}

	if(plan->type == PhysicalOperatorType::RESULT_COLLECTOR
	    || plan->type == PhysicalOperatorType::PROJECTION){
		for (auto& child : plan->children) {
			auto converted_child = ConvertToRecyclerNode(child);
			if (converted_child) {
				return converted_child;
			}
		}
		return nullptr;
	}

	shared_ptr<RecyclerNode> recycler_node = make_shared_ptr<RecyclerNode>(plan->type, plan->name, plan->table_name, plan->extra);

	for (auto& child : plan->children) {
		auto converted_child = ConvertToRecyclerNode(child);
		if (converted_child) {
			recycler_node->AddChild(converted_child);
			recycler_node->UpdateChildLeafNode(converted_child);

			// if root parent, remove
			if(converted_child->GetParents().size() == 1){
				for(auto& parent: converted_child->GetParents()){
					if(parent->GetType() == PhysicalOperatorType::RECYCLER_ROOT){
						parent->EraseChild(converted_child);
						converted_child->EraseParent(parent);
					}
				}
			}

			converted_child->AddParent(recycler_node);
		}
	}

	if(recycler_node->GetChildren().empty()){
		recycler_node->AddLeafNode(recycler_node);
	}

	recycler_node->SetRecyclerLop(plan);
	return recycler_node;
}

void RecyclerGraph::AddQuery(const shared_ptr<OperatorLineage>& lineage_plan){
	// notice that the parent elements of a recycler_node may be added during the convert
	shared_ptr<RecyclerNode> converted_lineage_plan = ConvertToRecyclerNode(lineage_plan);

	if(!converted_lineage_plan){
		return;
	}

	if(lineage_plan->mapping_recycler_node){
		// the whole plan has already existed in the recycler_graph
		return;
	}

	root->AddChild(converted_lineage_plan);
	root->UpdateChildLeafNode(converted_lineage_plan);
	converted_lineage_plan->AddParent(root);

	// remove some children of root if they are the children of converted_lineage_plan
	// and update their parents
	// this is because the converted_lineage_plan is a totally new recycler_node
	// and we need to update the parent of its children
	// this has been done in the convert function


	// case 1: converted_lineage_plan is a totally new recycler_node
	// directly add it to the root children, and update the leaf nodes of the root
	// if a node in the subtree is originally the child of root, we need to update its parent

	// case 2: converted_lineage_plan is the root of a subtree
	// no need to do anything

	return;
}

bool RecyclerGraph::MatchTree(const shared_ptr<OperatorLineage>& lineage_plan) {
	if (!lineage_plan) {
		return false;
	}

	// unlike other cases, in our lineage capture, we do not care about columns
	if(lineage_plan->type == PhysicalOperatorType::RESULT_COLLECTOR
	    || lineage_plan->type == PhysicalOperatorType::PROJECTION){
		return MatchTree(lineage_plan->children[0]);
	}

	// set the mapping_recycler_node as far as possible for each operator in the lineage plan
	if(lineage_plan->children.empty()){
		for(const shared_ptr<RecyclerNode>& leaf_node: root->GetLeafNodes()){
			if(lineage_plan->Matches(leaf_node)){
				lineage_plan->mapping_recycler_node = leaf_node;
				return true;
			}
		}
		return false;
	}

	// if all children match, then this operator may match
	bool all_children_match = true;
	for(auto& child: lineage_plan->children){
		all_children_match &= MatchTree(child);
	}

	// one of the children does not match, then this operator does not match definitely
	if(!all_children_match){
		return false;
	} else {
		// all children match, then we need to check whether this operator exists in the recycler_graph
		shared_ptr<RecyclerNode> matched_node = nullptr;

		shared_ptr<OperatorLineage> usable_child = lineage_plan->children[0];
		while(usable_child->type == PhysicalOperatorType::RESULT_COLLECTOR
		       || usable_child->type == PhysicalOperatorType::PROJECTION){
			usable_child = usable_child->children[0];
		}

		for(auto& parent: usable_child->mapping_recycler_node->GetParents()){
			if(lineage_plan->Matches(parent)){
				matched_node = parent;
				break;
			}
		}
		if(matched_node){
			lineage_plan->mapping_recycler_node = matched_node;
			return true;
		} else {
			return false;
		}
	}

}


}

#endif
