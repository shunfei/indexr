#include "Condition.h"
#include "TempTable.h"

void Condition::AddDescriptor(CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc)
{
	descriptors.push_back(Descriptor(e1, op, e2, e3, t, no_dims, like_esc));
}

void Condition::AddDescriptor(DescTree* tree, TempTable* t, int no_dims)
{
	descriptors.push_back(Descriptor(tree, t, no_dims));
}

void Condition::AddDescriptor(const Descriptor& desc)
{
	descriptors.push_back(desc);
}

void Condition::Simplify()
{
	int size = int(descriptors.size());
	for(int i = 0; i < size; i++) {
		if(descriptors[i].op == O_OR_TREE) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(descriptors[i].tree);
			Descriptor desc;
			do {
				if((descriptors[i].op != O_OR_TREE))
					break;
				desc = descriptors[i].tree->ExtractDescriptor();
				if(!desc.IsEmpty()) {
					descriptors[i].Simplify(true);  //true required not to simplify parameters
					desc.CalculateJoinType();
					desc.CoerceColumnTypes();
					descriptors.push_back(desc);
				}
			} while(!desc.IsEmpty());
		}
	}
}

void Condition::MakeSingleColsPrivate(std::vector<VirtualColumn*>& virt_cols)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(descriptors.size() == 1);
	descriptors[0].tree->MakeSingleColsPrivate(virt_cols);
	
}

SingleTreeCondition::SingleTreeCondition(CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc)
{	
	tree = new DescTree(e1, op, e2, e3, t, no_dims, like_esc);
}

SingleTreeCondition::~SingleTreeCondition()
{
	delete tree;
}

void SingleTreeCondition::AddDescriptor(LogicalOperator lop, CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc)
{
	if(tree)
		tree->AddDescriptor(lop, e1, op, e2, e3, t, no_dims, like_esc);
	else
		tree = new DescTree(e1, op, e2, e3, t, no_dims, like_esc);
}

void SingleTreeCondition::AddTree(LogicalOperator lop, DescTree* sec_tree, int no_dims)
{
	if(tree)
		tree->AddTree(lop, sec_tree, no_dims);
	else
		tree = new DescTree(*sec_tree);
}
