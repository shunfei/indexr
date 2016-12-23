/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#include <vector>

#ifndef _DESCRIPTOR_H_
#define _DESCRIPTOR_H_

#include "common/CommonDefinitions.h"
#include "CQTerm.h"
#include "core/DimensionGroup.h"

class ConstColumn;
class ParameterizedFilter;
class MIIterator;
class Query;
class TempTable;
class MultiIndex;
class DescTree;

/*! DT_NON_JOIN - 1 dimension condition
 * 	DT_SIMPLE_JOIN - suitable for e.g. hash join
 *  DT_COMPLEX_JOIN - possibly requires nested loop join
 */
enum DescriptorJoinType	{ DT_NON_JOIN, DT_SIMPLE_JOIN, DT_COMPLEX_JOIN, DT_NOT_KNOWN_YET };
enum SubSelectOptimizationType { ROW_BASED, PACK_BASED };

class Descriptor
{
public:
	Operator 			op;
	CQTerm				attr;		// descriptor is usually "attr op val1 [val2]", e.g. "a = 7", "a BETWEEN 8 AND 12"
    CQTerm 				val1;
    CQTerm 				val2;

    LogicalOperator		lop;		// OR is not implemented on some lower levels! Only valid in HAVING tree.
    bool 				sharp;		// used to indicate that BETWEEN contains sharp inequalities
    bool 				encoded;
    bool 				done;
    double 				evaluation;	// approximate weight ("hardness") of the condition; execution order is based on it
    bool				delayed;	// descriptor is delayed (usually because of outer joins) tbd. after joins
    TempTable* 			table;
	DescTree*			tree;
	DimensionVector		left_dims;
	DimensionVector		right_dims;
	RSValue				rv;			// rough evaluation of descriptor (accumulated or used locally)
	char				like_esc;	// nonstandard LIKE escape character

	Descriptor();
    Descriptor(TempTable* t, int no_dims);		// no_dims is a destination number of dimensions (multiindex may be smaller at this point)
    ~Descriptor();
    Descriptor(const Descriptor & desc);
	Descriptor(CQTerm e1, Operator pr, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_escape = '\\');
	Descriptor(DescTree* tree, TempTable* t, int no_dims);
	Descriptor(TempTable* t, VirtualColumn *v1, Operator pr, VirtualColumn *v2 = NULL, VirtualColumn *v3 = NULL);

    void swap(Descriptor& d);

	Descriptor& operator=(const Descriptor & desc);
	int operator==(const Descriptor& sec) const;
	bool operator<(const Descriptor& sec) const;
	bool operator<=(const Descriptor& sec) const;
    bool EqualExceptOuter(const Descriptor& sec);		// descriptors are equal, but one may be inner and other may be outer
    void Simplify(bool in_having = false);
    void SwitchSides();
	bool IsEmpty() const { return op == O_UNKNOWN_FUNC; }
	bool IsTrue() const { return op == O_TRUE; }
    bool IsFalse() const { return op == O_FALSE; }
    bool IsBHItemsEmpty() ;
    bool WithoutAttrs();
	bool WithoutTypeCast();

	void UpdateVCStatistics();					// Apply all the information from constants etc. to involved VC

	bool CheckCondition_UTF(const MIIterator& mit);
	bool CheckCondition(const MIIterator& mit);	// Assumption: LockSourcePacks done externally.
	bool IsNull(const MIIterator& mit);
    void LockSourcePacks(const MIIterator& mit);
    void LockSourcePacks(const MIIterator& mit, int th_no);
    void EvaluatePack(MIUpdatingIterator& mit, int th_no);
	void EvaluatePack(MIUpdatingIterator& mit);	// Assumption: no locking needed, done inside
	RSValue EvaluateRoughlyPack(const MIIterator& mit);

	void UnlockSourcePacks();
    void DimensionUsed(DimensionVector & dims);
    bool IsParameterized() const;
    bool IsDeterministic() const;
	bool IsType_OrTree() const			{ return op == O_OR_TREE; }
    bool IsType_JoinSimple() const;
    bool IsType_AttrAttr() const;
    bool IsType_AttrValOrAttrValVal() const;
    bool IsType_AttrMultiVal() const;
    // Note: CalculateJoinType() must be executed before checking a join type
    void CalculateJoinType();
    DescriptorJoinType GetJoinType() const { return desc_t; }
	bool IsType_Subquery();

	bool IsType_IBExpression() const;		// only columns, constants and IBExpressions
    bool IsType_JoinComplex() const;
    bool IsType_Join() const { return (IsType_JoinSimple() || IsType_JoinComplex()); }

    bool IsDelayed() const { return delayed; }

    bool IsInner() { return right_dims.IsEmpty(); } // join_type == JO_INNER; }

    bool IsOuter() { return !right_dims.IsEmpty(); } // (IsLeftJoin() || IsRightJoin() || IsFullOuterJoin()); }

    char *ToString(char buf[], size_t buf_ct);
    void CoerceColumnTypes();
    bool NullMayBeTrue(); // true, if the descriptor may give nontrivial answer if any of involved dimension is null
    const QueryOperator *CreateQueryOperator(Operator type) const;
    void CoerceColumnType(VirtualColumn *& to_be_casted);
    DTCollation GetCollation() const { return collation; }

    void InitPrefetching(MIIterator& mit, RSValue* r_filter = NULL);
    void StopPrefetching(bool unlock = true);
    void CopyStatefulVCs(std::vector<VirtualColumn*> & local_cols);
    bool IsParallelReady();
    void EvaluatePackImpl(MIUpdatingIterator & mit);
    void SimplifyAfterRoughAccumulate();
    void RoughAccumulate(MIIterator& mit);
	void ClearRoughValues();

private:
    /*! \brief Checks condition for set operator, e.g., <ALL
	* \pre LockSourcePacks done externally.
	* \param mit - iterator on MultiIndex
	* \param op - operator to check
	* \return bool
	*/
	bool CheckSetCondition(const MIIterator& mit, Operator op);
	bool IsNull_Set(const MIIterator& mit, Operator op);
	BHTribool RoughCheckSetSubSelectCondition(const MIIterator& mit, Operator op, SubSelectOptimizationType sot);
	bool CheckSetCondition_UTF(const MIIterator& mit, Operator op);

	void AppendString(char *buffer, size_t bufferSize, const char *string, size_t stringLength, size_t offset) const;
    void AppendConstantToString(char buffer[], size_t size, const QueryOperator *operatorObject) const;
    void AppendUnaryOperatorToString(char buffer[], size_t size, const QueryOperator *operatorObject) const;
    void AppendBinaryOperatorToString(char buffer[], size_t size, const QueryOperator *operatorObject) const;
    void AppendTernaryOperatorToString(char buffer[], size_t size, const QueryOperator *operatorObject) const;

	void CoerceCollation();
	BHTribool RoughCheckSubselectCondition(MIIterator& mit, SubSelectOptimizationType);
	//! make the type of to_be_casted to be comparable to attr.vc by wrapping to_be_casted in a TypeCastColumn
    DescriptorJoinType desc_t;
    DTCollation collation;
public:
	bool null_after_simplify; // true if Simplify set O_FALSE because of NULL
};

class SortDescriptor
{
public:
	VirtualColumn*	vc;
	int			dir;		// ordering direction: 0 - ascending, 1 - descending
	SortDescriptor() : vc(NULL), dir(0) {};
	int operator==(const SortDescriptor& sec);

};

DescriptorType GetDescOperType(const Descriptor& d);
bool IsSetOperator(Operator op);
bool IsSetAllOperator(Operator op);
bool IsSetAnyOperator(Operator op);
bool ISTypeOfEqualOperator(Operator op);
bool ISTypeOfNotEqualOperator(Operator op);
bool ISTypeOfLessOperator(Operator op);
bool ISTypeOfLessEqualOperator(Operator op);
bool ISTypeOfMoreOperator(Operator op);
bool ISTypeOfMoreEqualOperator(Operator op);
bool IsSimpleEqualityOperator(Operator op);

/////////////////////////////////////////////////////////////////////////////////////////////

struct DescTreeNode {
	DescTreeNode(LogicalOperator _lop, TempTable* t, int no_dims) : desc(t, no_dims), locked(0), left(NULL), right(NULL), parent(NULL)
	{ desc.lop = _lop; }

	DescTreeNode(CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc) : desc(t, no_dims), locked(0), left(NULL), right(NULL), parent(NULL)
	{ desc.attr = e1; desc.op = op; desc.val1 = e2; desc.val2 = e3; desc.like_esc = like_esc; desc.CoerceColumnTypes(); desc.CalculateJoinType(); desc.Simplify(true); }

	DescTreeNode(DescTreeNode &n, bool in_subq = false) : desc(n.desc), locked(0), left(NULL), right(NULL), parent(NULL) { }
	~DescTreeNode();
	bool CheckCondition(MIIterator& mit);
	bool IsNull(MIIterator& mit);
	void EvaluatePack(MIUpdatingIterator& mit);
	void PrepareToLock(int locked_by);			// mark all descriptors as "must be locked before use"
	void UnlockSourcePacks();					// must be run after all CheckCondition(), before query destructor
	BHTribool Simplify(DescTreeNode*& root, bool in_having = false);
	bool IsParameterized();
	void DimensionUsed(DimensionVector &dims);
	bool NullMayBeTrue();
	void EncodeIfPossible(bool for_rough_query, bool additional_nulls);
	double EvaluateConditionWeight(ParameterizedFilter *p, bool for_or);
	RSValue EvaluateRoughlyPack(const MIIterator& mit);
	void CollectDescriptor(std::vector<std::pair<int, Descriptor> >& desc_counts);

	void IncreaseDescriptorCount(std::vector<std::pair<int, Descriptor> > &desc_counts);

	/*! \brief Check if descriptor is common to all subtrees
	 * \param desc - descriptor to be evaluated
	 * \return bool - 
	*/
	bool CanBeExtracted(Descriptor& desc);
	
	void ExtractDescriptor(Descriptor& desc, DescTreeNode*& root);
	void InitPrefetching(MIIterator& mit, RSValue* r_filter = NULL);
	void StopPrefetching(bool unlock = true);
	void LockSourcePacks(const MIIterator& mit, const int th_no);
	void CopyStatefulVCs(std::vector<VirtualColumn*> & local_cols);
	bool IsParallelReady();
	BHTribool ReplaceNode(DescTreeNode* src, DescTreeNode* dst, DescTreeNode*& root);
	bool UseRoughAccumulated();				// true if any leaf becomes trivial
	void RoughAccumulate(MIIterator& mit);
	void ClearRoughValues();
	void MakeSingleColsPrivate(std::vector<VirtualColumn*>& virt_cols);
	bool IsBHItemsEmpty();
	bool WithoutAttrs();
	bool WithoutTypeCast();
	Descriptor desc;
	int locked;		// for a leaf: >= 0 => source pack must be locked before use, -1 => already locked
	DescTreeNode *left, *right, *parent;
};

// Tree of descriptors: internal nodes - predicates, leaves - terms
// Used to store conditions filtering output columns of TempTable
// Used, particularly, to store conditions of 'having' clause

class DescTree {
public:
	DescTreeNode *root, *curr;
	DescTreeNode * Copy(DescTreeNode * node);
//	void Release(DescTreeNode * &node); // releases whole subtree starting from node

	DescTree(CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* q, int no_dims, char like_esc);
	DescTree(DescTree &);
	~DescTree() { Release(); }

	void Display();
	void Display(DescTreeNode * node);

	void Release(); // releases whole subtree

	bool Left()		{ if(!curr || !curr->left) return false; curr = curr->left; return true; }
	bool Right()	{ if(!curr || !curr->right) return false; curr = curr->right; return true; }
	bool Up()		{ if(!curr || !curr->parent) return false; curr = curr->parent; return true; }
	void Root()		{ curr = root; }

	// make lop the root, make current tree the left child, make descriptor the right child
	void AddDescriptor(LogicalOperator lop, CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc);
	// make lop the root, make current tree the left child, make tree the right child
	void AddTree(LogicalOperator lop, DescTree *tree, int no_dims);
	bool IsParameterized();
	void DimensionUsed(DimensionVector &dims)		{ root->DimensionUsed(dims); }
	bool NullMayBeTrue()							{ return root->NullMayBeTrue(); }
	BHTribool Simplify(bool in_having) { return root->Simplify(root, in_having); }
	Descriptor ExtractDescriptor();
    void InitPrefetching(MIIterator& mit, RSValue* r_filter = NULL) { root->InitPrefetching(mit, r_filter); }
    void StopPrefetching(bool unlock = true) { root->StopPrefetching(unlock); }
    void CopyStatefulVCs(std::vector<VirtualColumn*>& local_cols) { root->CopyStatefulVCs(local_cols); }
    bool IsParallelReady() { return root->IsParallelReady(); }
    void RoughAccumulate(MIIterator& mit) { root->RoughAccumulate(mit); }
    bool UseRoughAccumulated() { return root->UseRoughAccumulated(); }			// true if anything to simplify
	void MakeSingleColsPrivate(std::vector<VirtualColumn*>& virt_cols);
	bool IsBHItemsEmpty()	{ return root->IsBHItemsEmpty(); }
	bool WithoutAttrs()		{ return root->WithoutAttrs(); }
	bool WithoutTypeCast()	{ return root->WithoutTypeCast(); }
};

#endif
